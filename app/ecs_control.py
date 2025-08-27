# app/ecs_control.py
import time
import random
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ConnectionClosedError,
    ReadTimeoutError,
)

# ---------- Shared boto3 config & clients ----------

_BOTO_CFG = Config(
    retries={"max_attempts": 12, "mode": "adaptive"},
    connect_timeout=5,
    read_timeout=30,
)

def _ecs(region: str):
    return boto3.client("ecs", region_name=region, config=_BOTO_CFG)

def _logs(region: str):
    return boto3.client("logs", region_name=region, config=_BOTO_CFG)

def _sd(region: str):
    return boto3.client("servicediscovery", region_name=region, config=_BOTO_CFG)

def _sleep_backoff(attempt: int, base: float = 0.8, cap: float = 16.0):
    # Exponential backoff with jitter
    delay = min(cap, base * (2 ** attempt)) * (0.5 + random.random())
    time.sleep(delay)

def _is_transient(err: Exception) -> bool:
    if isinstance(err, (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError)):
        return True
    if isinstance(err, ClientError):
        code = err.response.get("Error", {}).get("Code", "")
        return code in {
            "ThrottlingException",
            "TooManyRequestsException",
            "RequestLimitExceeded",
            "ServiceUnavailableException",
            "ServerException",
            "InternalServiceException",
        }
    return "Service Unavailable" in str(err)

# ---------- Utilities ----------

def router_dns_name(region: str, cloudmap_namespace_id: str, service_name: str) -> str:
    """Return 'service.namespace' DNS name for Cloud Map."""
    sd = _sd(region)
    ns = sd.get_namespace(Id=cloudmap_namespace_id)["Namespace"]
    ns_name = ns["Name"]  # e.g., "mobilys-otp-staging.local"
    return f"{service_name}.{ns_name}"

def _is_taskdef_arn(s: str) -> bool:
    return isinstance(s, str) and s.startswith("arn:aws:ecs:")

def _ensure_taskdef_exists(
    *,
    region: str,
    family: str,
    image: Optional[str],
    cpu: str,
    memory: str,
    exec_role: Optional[str],
    task_role: Optional[str],
    container_name: str,
    log_group: str,
    log_prefix: str,
) -> str:
    """
    Make sure an ACTIVE task definition exists for the given family.
    Returns the ARN of some ACTIVE revision (the latest).
    Registers once if none exists (requires 'image').
    """
    ecs = _ecs(region)

    # Try to find the latest ACTIVE revision
    for attempt in range(6):
        try:
            resp = ecs.list_task_definitions(
                familyPrefix=family, status="ACTIVE", sort="DESC", maxResults=1
            )
            arns = resp.get("taskDefinitionArns", [])
            if arns:
                return arns[0]
            break
        except Exception as e:
            if _is_transient(e) and attempt < 5:
                _sleep_backoff(attempt)
                continue
            raise

    # None exist: must register one
    if not image:
        raise ValueError(
            f"No ACTIVE task definitions found for family '{family}', and no image provided to create one."
        )

    for attempt in range(6):
        try:
            td = ecs.register_task_definition(
                family=family,
                networkMode="awsvpc",
                requiresCompatibilities=["FARGATE"],
                cpu=cpu,
                memory=memory,
                executionRoleArn=exec_role or None,
                taskRoleArn=task_role or None,
                containerDefinitions=[
                    {
                        "name": container_name,
                        "image": image,
                        "essential": True,
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": log_group,
                                "awslogs-region": region,
                                "awslogs-stream-prefix": log_prefix,
                            },
                        },
                    }
                ],
            )
            return td["taskDefinition"]["taskDefinitionArn"]
        except Exception as e:
            if _is_transient(e) and attempt < 5:
                _sleep_backoff(attempt)
                continue
            raise

# ---------- Builder one-off ----------

def submit_builder_and_wait(
    *,
    region: str,
    cluster_arn: str,
    subnets: List[str],
    security_groups: List[str],
    cloudwatch_log_group: str,
    task_family: str,                 # family name OR full task definition ARN
    task_exec_role_arn: Optional[str] = None,
    task_role_arn: Optional[str] = None,
    image: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    cpu: str = "2048",
    memory: str = "12288",           
    stream_prefix: str = "builder",
) -> Tuple[bool, List[str]]:
    """
    Run a one-off builder task and wait for STOPPED. Returns (ok, last_logs_tail).
    If 'task_family' is a family name, we reuse the latest ACTIVE revision, creating one
    if the family doesn't exist yet (needs 'image'). If 'task_family' is a full ARN,
    it's used directly.
    """
    ecs = _ecs(region)
    logs = _logs(region)

    # Resolve a usable task definition
    if _is_taskdef_arn(task_family):
        # Caller provided an explicit ARN
        task_def_to_run = task_family
        family_for_run = None
    else:
        # Ensure an ACTIVE revision exists; then we'll RUN by family name (latest ACTIVE)
        _ensure_taskdef_exists(
            region=region,
            family=task_family,
            image=image,
            cpu=cpu,
            memory=memory,
            exec_role=task_exec_role_arn,
            task_role=task_role_arn,
            container_name="builder",
            log_group=cloudwatch_log_group,
            log_prefix=stream_prefix,
        )
        task_def_to_run = task_family   # run by family name uses latest ACTIVE
        family_for_run = task_family

    # Container overrides for per-run environment
    overrides = {
        "containerOverrides": [
            {
                "name": "builder",
                "environment": [{"name": k, "value": str(v)} for k, v in (env or {}).items()],
            }
        ]
    }

    # Run task (with retries on transient failures)
    run_resp = None
    for attempt in range(6):
        try:
            run_resp = ecs.run_task(
                cluster=cluster_arn,
                launchType="FARGATE",
                taskDefinition=task_def_to_run,
                overrides=overrides,
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": subnets,
                        "securityGroups": security_groups,
                        "assignPublicIp": "ENABLED",
                    }
                },
                count=1,
            )
            break
        except Exception as e:
            if _is_transient(e) and attempt < 5:
                _sleep_backoff(attempt)
                continue
            raise

    failures = (run_resp or {}).get("failures", [])
    if failures:
        return False, [f"run_task failure: {failures}"]

    task_arn = run_resp["tasks"][0]["taskArn"]

    # Wait until STOPPED
    waiter = ecs.get_waiter("tasks_stopped")
    waiter.wait(cluster=cluster_arn, tasks=[task_arn])

    # Describe to get exit code
    desc = ecs.describe_tasks(cluster=cluster_arn, tasks=[task_arn])["tasks"][0]
    exit_code = None
    for c in desc.get("containers", []):
        if c.get("name") == "builder":
            exit_code = c.get("exitCode")
            break

    # Fetch CloudWatch Logs tail (best-effort)
    lines: List[str] = []
    ecs_task_id = task_arn.split("/")[-1]
    log_stream_name = f"{stream_prefix}/builder/{ecs_task_id}"
    try:
        token = None
        for _ in range(50):
            kw = dict(
                logGroupName=cloudwatch_log_group,
                logStreamName=log_stream_name,
                startFromHead=True,
            )
            if token:
                kw["nextToken"] = token
            resp = logs.get_log_events(**kw)
            for e in resp.get("events", []):
                lines.append(e.get("message", ""))
            nxt = resp.get("nextForwardToken")
            if nxt == token:
                break
            token = nxt
    except ClientError as e:
        lines.append(f"[logs] unable to fetch: {e}")

    if exit_code is None:
        lines.append("[builder] missing exit code in ECS describe_tasks")

    return (exit_code == 0), lines[-400:]

# ---------- Router service (ensure/create) ----------

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
    """
    Ensure (or create) an ECS service running the router for this scenario,
    then return its Cloud Map DNS name.
    """
    if not image:
        raise ValueError("ensure_router_service: image is required")

    ecs = _ecs(region)
    sd = _sd(region)
    service_name = f"{service_prefix}-{scenario_id}"

    # Reuse existing ACTIVE service
    try:
        existing = ecs.describe_services(cluster=cluster_arn, services=[service_name]).get("services", [])
        if existing and existing[0].get("status") != "INACTIVE":
            return router_dns_name(region, cloudmap_namespace_id, service_name)
    except ClientError:
        # If describe fails, we'll try to create below
        pass

    # Create or resolve Cloud Map service
    registry_arn = None
    try:
        resp = sd.create_service(
            Name=service_name,
            NamespaceId=cloudmap_namespace_id,
            DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        registry_arn = resp["Service"]["Arn"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("ServiceAlreadyExists", "DuplicateRequest"):
            # Find the existing service ARN by name within the namespace
            svc_list = sd.list_services(
                Filters=[{"Name": "NAMESPACE_ID", "Values": [cloudmap_namespace_id]}]
            ).get("Services", [])
            for svc in svc_list:
                if svc.get("Name") == service_name:
                    registry_arn = svc.get("Arn")
                    break
        else:
            raise

    # Register a router task definition revision (retry on control-plane hiccups)
    td = None
    for attempt in range(6):
        try:
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
            break
        except Exception as e:
            if _is_transient(e) and attempt < 5:
                _sleep_backoff(attempt)
                continue
            raise

    task_def_arn = td["taskDefinition"]["taskDefinitionArn"]

    # Create ECS service (retry on transient errors)
    for attempt in range(6):
        try:
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
            break
        except Exception as e:
            if _is_transient(e) and attempt < 5:
                _sleep_backoff(attempt)
                continue
            raise

    return router_dns_name(region, cloudmap_namespace_id, service_name)

# ---------- Delete router service ----------

def delete_router_service(
    *,
    region: str,
    cluster_arn: str,
    service_name: str,
) -> None:
    """Scale service to 0 then delete (best-effort)."""
    ecs = _ecs(region)
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
