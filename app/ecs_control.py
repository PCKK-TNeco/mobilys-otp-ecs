import time
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError


def router_dns_name(region: str, cloudmap_namespace_id: str, service_name: str) -> str:
    sd = boto3.client("servicediscovery", region_name=region)
    ns = sd.get_namespace(Id=cloudmap_namespace_id)["Namespace"]
    return f"{service_name}.{ns['Name']}"


# -------- One-off builder task: fire-and-return --------
def submit_builder_async(
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
) -> str:
    if not image:
        raise ValueError("submit_builder_async: image is required")

    ecs = boto3.client("ecs", region_name=region)

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
        raise RuntimeError(f"run_task failure: {failures}")

    return run["tasks"][0]["taskArn"]


def get_task_status(*, region: str, cluster_arn: str, task_arn: str) -> Dict[str, Optional[int]]:
    ecs = boto3.client("ecs", region_name=region)
    d = ecs.describe_tasks(cluster=cluster_arn, tasks=[task_arn])
    tasks = d.get("tasks", [])
    if not tasks:
        return {"lastStatus": "UNKNOWN", "exitCode": None}
    t = tasks[0]
    last = t.get("lastStatus")
    code = None
    for c in t.get("containers", []):
        if c.get("name") == "builder" and "exitCode" in c:
            code = c["exitCode"]
            break
    return {"lastStatus": last, "exitCode": code}


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
    if not image:
        raise ValueError("ensure_router_service: image is required")

    ecs = boto3.client("ecs", region_name=region)
    sd = boto3.client("servicediscovery", region_name=region)

    service_name = f"{service_prefix}-{scenario_id}"

    # reuse if exists
    desc = ecs.describe_services(cluster=cluster_arn, services=[service_name])
    svcs = desc.get("services", [])
    if svcs and svcs[0].get("status") != "INACTIVE":
        return router_dns_name(region, cloudmap_namespace_id, service_name)

    # find-or-create cloudmap service
    registry_arn = None
    try:
        created = sd.create_service(
            Name=service_name,
            NamespaceId=cloudmap_namespace_id,
            DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        registry_arn = created["Service"]["Arn"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("ServiceAlreadyExists", "ResourceAlreadyExistsException"):
            ls = sd.list_services(
                Filters=[{"Name": "NAMESPACE_ID", "Values": [cloudmap_namespace_id], "Condition": "EQ"}]
            )
            for s in ls.get("Services", []):
                if s.get("Name") == service_name:
                    registry_arn = s.get("Arn")
                    break
            if not registry_arn:
                raise
        else:
            raise

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


def delete_router_service(*, region: str, cluster_arn: str, service_name: str) -> None:
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
