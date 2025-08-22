# app/ecs_control.py
import time
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError


def router_dns_name(region: str, cloudmap_namespace_id: str, service_name: str) -> str:
    """Return "service.namespace" DNS name for Cloud Map."""
    sd = boto3.client("servicediscovery", region_name=region)
    ns = sd.get_namespace(Id=cloudmap_namespace_id)["Namespace"]
    ns_name = ns["Name"]  # e.g., "mobilys-otp-staging.local"
    return f"{service_name}.{ns_name}"


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
    Synchronous builder: registers a Fargate task def, runs it once, waits for STOPPED,
    then returns (success, last_log_lines).
    """
    if not image:
        raise ValueError("submit_builder_and_wait: image is required")

    ecs = boto3.client("ecs", region_name=region)
    logs = boto3.client("logs", region_name=region)

    # Register fresh revision for the one-off build
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

    # Run task
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

    # Wait for completion
    waiter = ecs.get_waiter("tasks_stopped")
    waiter.wait(cluster=cluster_arn, tasks=[task_arn])

    # Inspect result
    desc = ecs.describe_tasks(cluster=cluster_arn, tasks=[task_arn])["tasks"][0]
    containers = desc.get("containers", [])
    exit_code = None
    log_stream_name = None
    for c in containers:
        if c["name"] == "builder":
            exit_code = c.get("exitCode")
            ecs_task_id = task_arn.split("/")[-1]
            log_stream_name = f"{stream_prefix}/builder/{ecs_task_id}"
            break

    # Fetch logs (best effort)
    tail: List[str] = []
    if log_stream_name:
        try:
            token = None
            while True:
                kw = dict(
                    logGroupName=cloudwatch_log_group,
                    logStreamName=log_stream_name,
                    startFromHead=True,
                )
                if token:
                    kw["nextToken"] = token
                resp = logs.get_log_events(**kw)
                for e in resp.get("events", []):
                    tail.append(e.get("message", ""))
                nxt = resp.get("nextForwardToken")
                if nxt == token:
                    break
                token = nxt
        except ClientError as e:
            tail.append(f"[logs] fetch error: {e}")

    ok = (exit_code == 0)
    if exit_code is None:
        tail.append("[builder] missing exit code in ECS describe_tasks")
    return ok, tail[-400:]


def ensure_router_service(
    *,
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
    """Ensure (or create) a router ECS service for this scenario, then return its Cloud Map DNS name."""
    if not image:
        raise ValueError("ensure_router_service: image is required")

    ecs = boto3.client("ecs", region_name=region)
    sd = boto3.client("servicediscovery", region_name=region)
    service_name = f"{service_prefix}-{scenario_id}"

    # Reuse if active
    existing = ecs.describe_services(cluster=cluster_arn, services=[service_name]).get("services", [])
    if existing and existing[0].get("status") != "INACTIVE":
        return router_dns_name(region, cloudmap_namespace_id, service_name)

    # Cloud Map service (create if new)
    try:
        sd_resp = sd.create_service(
            Name=service_name,
            NamespaceId=cloudmap_namespace_id,
            DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        registry_arn = sd_resp["Service"]["Arn"]
    except ClientError as e:
        # If already exists, resolve its ARN
        if e.response.get("Error", {}).get("Code") in ("ServiceAlreadyExists", "DuplicateRequest"):
            # minimal lookup: name is unique per namespace
            svc = sd.list_services(Filters=[{"Name": "NAMESPACE_ID", "Values": [cloudmap_namespace_id]}]).get("Services", [])
            registry_arn = next((s["Arn"] for s in svc if s["Name"] == service_name), None)
        else:
            raise

    # New router task def revision
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

    # Create service wired to Cloud Map
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
        serviceRegistries=[{"registryArn": registry_arn}] if registry_arn else [],
        enableExecuteCommand=True,
    )

    return router_dns_name(region, cloudmap_namespace_id, service_name)


def delete_router_service(
    *,
    region: str,
    cluster_arn: str,
    service_name: str,
) -> None:
    """Scale service to 0 then delete (best-effort)."""
    ecs = boto3.client("ecs", region_name=region)
    try:
        ecs.update_service(cluster=cluster_arn, service=service_name, desiredCount=0)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("ClusterNotFoundException", "ServiceNotFoundException"):
            return
        raise

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
