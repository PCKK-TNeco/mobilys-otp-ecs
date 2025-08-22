# app/ecs_control.py
import time
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError


# -------- Utility: resolve "service.namespace" DNS name --------
def router_dns_name(region: str, cloudmap_namespace_id: str, service_name: str) -> str:
    sd = boto3.client("servicediscovery", region_name=region)
    ns = sd.get_namespace(Id=cloudmap_namespace_id)["Namespace"]
    ns_name = ns["Name"]  # e.g., "mobilys-otp-staging.local"
    return f"{service_name}.{ns_name}"


# -------- One-off builder task: run & wait, return (ok, logs) --------
def submit_builder_and_wait(
    *,
    region: str,
    cluster_arn: str,
    subnets: List[str],
    security_groups: List[str],
    cloudwatch_log_group: str,
    task_family: str,
    task_exec_role_arn: Optional[str] = None,
    task_role_arn: Optional[str] = None,
    image: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    cpu: str = "2048",
    memory: str = "4096",
    stream_prefix: str = "builder",
) -> Tuple[bool, List[str]]:
    """
    Registers a one-off Fargate task definition for the OTP graph build,
    runs it, waits for STOPPED, and returns (success, last_logs).
    """
    ecs = boto3.client("ecs", region_name=region)
    logs = boto3.client("logs", region_name=region)

    if not image:
        raise ValueError("submit_builder_and_wait: image is required")

    # Register a fresh task def revision for the build
    td = ecs.register_task_definition(
        family=task_family,
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu=cpu,
        memory=memory,
        executionRoleArn=task_exec_role_arn or None,
        taskRoleArn=task_role_arn or None,
        containerDefinitions=[
            {
                "name": "builder",
                "image": image,
                "essential": True,
                # The builder image should know what to do from env (S3 paths etc.)
                "environment": [{"name": k, "value": str(v)} for k, v in (env or {}).items()],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": cloudwatch_log_group,
                        "awslogs-region": region,
                        "awslogs-stream-prefix": stream_prefix,
                    },
                },
            }
        ],
    )
    task_def_arn = td["taskDefinition"]["taskDefinitionArn"]

    # Run the task
    run = ecs.run_task(
        cluster=cluster_arn,
        launchType="FARGATE",
        taskDefinition=task_def_arn,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "ENABLED",
            }
        },
        count=1,
    )
    failures = run.get("failures", [])
    if failures:
        return False, [f"run_task failure: {failures}"]

    task_arn = run["tasks"][0]["taskArn"]

    # Wait for STOPPED
    waiter = ecs.get_waiter("tasks_stopped")
    waiter.wait(cluster=cluster_arn, tasks=[task_arn])

    desc = ecs.describe_tasks(cluster=cluster_arn, tasks=[task_arn])["tasks"][0]
    containers = desc.get("containers", [])
    exit_code = None
    log_stream_name = None
    for c in containers:
        if c["name"] == "builder":
            exit_code = c.get("exitCode")
            details = c.get("logConfiguration", {})  # usually empty in describe
            # awslogs stream name format: <prefix>/<container>/<ecs-task-id>
            # we can derive it:
            ecs_task_id = task_arn.split("/")[-1]
            log_stream_name = f"{stream_prefix}/builder/{ecs_task_id}"
            break

    # Try to read logs (best-effort)
    log_lines: List[str] = []
    if log_stream_name:
        try:
            token = None
            while True:
                kw = dict(logGroupName=cloudwatch_log_group, logStreamName=log_stream_name, startFromHead=True)
                if token:
                    kw["nextToken"] = token
                resp = logs.get_log_events(**kw)
                for e in resp.get("events", []):
                    log_lines.append(e.get("message", ""))
                token_next = resp.get("nextForwardToken")
                if token_next == token:
                    break
                token = token_next
        except ClientError as e:
            log_lines.append(f"[logs] unable to fetch: {e}")

    ok = (exit_code == 0)
    if exit_code is None:
        log_lines.append("[builder] missing exit code in ECS describe_tasks")

    return ok, log_lines[-400:]  # tail


# -------- Ensure (or create) a router ECS service for a scenario --------
def ensure_router_service(
    region: str,
    cluster_arn: str,
    subnets: List[str],
    security_groups: List[str],
    cloudmap_namespace_id: str,
    service_prefix: str,
    scenario_id: str,
    task_family: str,
    task_exec_role_arn: Optional[str] = None,
    task_role_arn: Optional[str] = None,
    image: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    desired_count: int = 1,
    container_port: int = 8081,
    cw_log_group: str = "/mobilys-otp/router",
) -> str:
    ecs = boto3.client("ecs", region_name=region)
    sd = boto3.client("servicediscovery", region_name=region)

    service_name = f"{service_prefix}-{scenario_id}"

    # If service exists and active, reuse it
    desc = ecs.describe_services(cluster=cluster_arn, services=[service_name])
    svcs = desc.get("services", [])
    if svcs and svcs[0].get("status") != "INACTIVE":
        return router_dns_name(region, cloudmap_namespace_id, service_name)

    # Create Cloud Map service
    sd_resp = sd.create_service(
        Name=service_name,
        NamespaceId=cloudmap_namespace_id,
        DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
        HealthCheckCustomConfig={"FailureThreshold": 1},
    )
    registry_arn = sd_resp["Service"]["Arn"]

    # Register a router task def revision
    if not image:
        raise ValueError("ensure_router_service: image is required")

    td = ecs.register_task_definition(
        family=task_family,
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu="512",
        memory="1024",
        executionRoleArn=task_exec_role_arn or None,
        taskRoleArn=task_role_arn or None,
        containerDefinitions=[
            {
                "name": "router",
                "image": image,
                "essential": True,
                "portMappings": [{"containerPort": container_port, "protocol": "tcp"}],
                "environment": [{"name": k, "value": str(v)} for k, v in (env or {}).items()],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": cw_log_group,
                        "awslogs-region": region,
                        "awslogs-stream-prefix": "router",
                    },
                },
            }
        ],
    )
    task_def_arn = td["taskDefinition"]["taskDefinitionArn"]

    # Create ECS service wired to Cloud Map
    ecs.create_service(
        cluster=cluster_arn,
        serviceName=service_name,
        taskDefinition=task_def_arn,
        desiredCount=desired_count,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "ENABLED",
            }
        },
        serviceRegistries=[{"registryArn": registry_arn}],
        enableExecuteCommand=True,
    )

    return router_dns_name(region, cloudmap_namespace_id, service_name)


# -------- Delete (scale to 0 then remove) a router service --------
def delete_router_service(
    *,
    region: str,
    cluster_arn: str,
    service_name: str,
) -> None:
    ecs = boto3.client("ecs", region_name=region)
    try:
        ecs.update_service(cluster=cluster_arn, service=service_name, desiredCount=0)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("ClusterNotFoundException", "ServiceNotFoundException"):
            return
        raise

    # Wait briefly for tasks to drain
    for _ in range(30):
        d = ecs.describe_services(cluster=cluster_arn, services=[service_name])
        s = d.get("services", [{}])[0]
        if s.get("runningCount", 0) == 0:
            break
        time.sleep(2)

    try:
        ecs.delete_service(cluster=cluster_arn, service=service_name, force=True)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("ClusterNotFoundException", "ServiceNotFoundException"):
            raise
