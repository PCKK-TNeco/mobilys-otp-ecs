# app/ecs_control.py
import time
import random
from typing import Dict, List, Optional, Tuple
import copy

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ConnectionClosedError,
    ReadTimeoutError,
    WaiterError,
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
    """Exponential backoff with jitter."""
    delay = min(cap, base * (2 ** attempt)) * (0.5 + random.random())
    time.sleep(delay)

def _is_transient(err: Exception) -> bool:
    """Best-effort classification of transient/server-side errors worth retrying."""
    if isinstance(err, (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError, WaiterError)):
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
        task_def_to_run = task_family
    else:
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
    try:
        # Delay: 15s, MaxAttempts: 720 ≈ 3 hours (tune as you like)
        waiter.wait(
            cluster=cluster_arn,
            tasks=[task_arn],
            WaiterConfig={"Delay": 15, "MaxAttempts": 720},
        )
    except WaiterError:
        # Still proceed to DescribeTasks
        pass

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
    env: Optional[Dict[str, str]] = None,  # optional extras/overrides
    graphs_bucket: Optional[str] = None,   # optional override; usually None
    graph_prefix: str = "graphs",          # optional override; matches base TD
    desired_count: int = 1,
    container_port: int = 8081,
    cw_log_group: str = "/mobilys-otp/router",
    # --- failure-loop guard controls (new) ---
    auto_stop_on_fail: bool = True,
    fail_window_seconds: int = 300,   # watch window (seconds)
    fail_threshold: int = 3           # stop after N failures within window
) -> str:
    """
    Register a per-scenario TD revision (injects GRAPH_SCENARIO_ID; optionally overrides
    GRAPHS_BUCKET/GRAPH_PREFIX), then create/update the Service to that revision.

    Extras:
      - deployment circuit breaker + minHealthy=0 to avoid 8081 bind clashes
      - fail-loop guard to scale to 0 if tasks keep STOPPING quickly
    """
    if not image:
        raise ValueError("ensure_router_service: image is required")

    ecs = _ecs(region)
    sd  = _sd(region)
    service_name = f"{service_prefix}-{scenario_id}"
    print(f"[router/ensure] >>> start service_name={service_name}")

    # --- Cloud Map ensure (idempotent) ---
    registry_arn = None
    try:
        resp = sd.create_service(
            Name=service_name,
            NamespaceId=cloudmap_namespace_id,
            DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
            HealthCheckCustomConfig={"FailureThreshold": 1},
        )
        registry_arn = resp["Service"]["Arn"]
        print(f"[router/ensure] Cloud Map create_service OK arn={registry_arn}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("ServiceAlreadyExists", "DuplicateRequest", "ResourceAlreadyExistsException"):
            svc_list = sd.list_services(
                Filters=[{"Name": "NAMESPACE_ID", "Values": [cloudmap_namespace_id]}]
            ).get("Services", [])
            for svc in svc_list:
                if svc.get("Name") == service_name:
                    registry_arn = svc.get("Arn")
                    print(f"[router/ensure] found existing Cloud Map service arn={registry_arn}")
                    break
        else:
            print("[router/ensure] create_service ClientError:", e.response)
            raise

    # --- Get latest ACTIVE base TD ---
    resp = ecs.list_task_definitions(familyPrefix=task_family, status="ACTIVE", sort="DESC", maxResults=1)
    arns = resp.get("taskDefinitionArns", [])
    if not arns:
        raise RuntimeError(f"No ACTIVE task definition found for family '{task_family}'.")
    base_td = ecs.describe_task_definition(taskDefinition=arns[0])["taskDefinition"]

    # --- Clone and inject env ---
    cds = copy.deepcopy(base_td["containerDefinitions"])
    if not cds:
        raise RuntimeError("Base task definition has no containerDefinitions")
    idx = next((i for i, c in enumerate(cds) if c.get("name") == "router"), 0)
    c0 = cds[idx]

    # merge env from base + caller
    base_env = {e["name"]: e["value"] for e in c0.get("environment", [])}
    add_env  = dict(env or {})
    add_env["GRAPH_SCENARIO_ID"] = scenario_id
    if graphs_bucket:
        add_env["GRAPHS_BUCKET"] = graphs_bucket
    if graph_prefix:
        add_env["GRAPH_PREFIX"] = graph_prefix
    add_env.setdefault("AWS_REGION", region)
    merged = {**base_env, **add_env}
    c0["environment"] = [{"name": k, "value": v} for k, v in sorted(merged.items())]

    # image / logs / port
    if image:
        c0["image"] = image
    if c0.get("logConfiguration", {}).get("logDriver") == "awslogs":
        opts = c0["logConfiguration"].setdefault("options", {})
        if cw_log_group:
            opts["awslogs-group"] = cw_log_group
        opts.setdefault("awslogs-region", region)
        opts.setdefault("awslogs-create-group", "true")
    if "portMappings" in c0 and c0["portMappings"]:
        c0["portMappings"][0]["containerPort"] = container_port
    else:
        c0["portMappings"] = [{"containerPort": container_port, "protocol": "tcp"}]

    # --- Register new TD revision ---
    reg_kwargs = {
        "family": base_td["family"],
        "taskRoleArn": task_role_arn or base_td.get("taskRoleArn"),
        "executionRoleArn": task_exec_role_arn or base_td.get("executionRoleArn"),
        "networkMode": base_td["networkMode"],
        "containerDefinitions": cds,
        "requiresCompatibilities": base_td.get("requiresCompatibilities") or [],
        "cpu": base_td.get("cpu"),
        "memory": base_td.get("memory"),
        "runtimePlatform": base_td.get("runtimePlatform"),
        "ephemeralStorage": base_td.get("ephemeralStorage"),
        "volumes": base_td.get("volumes") or [],
        "tags": [{"key": "scenario_id", "value": scenario_id}],
    }
    reg_kwargs = {k: v for k, v in reg_kwargs.items() if v not in (None, [], {})}
    new_td_arn = ecs.register_task_definition(**reg_kwargs)["taskDefinition"]["taskDefinitionArn"]
    desc = ecs.describe_task_definition(taskDefinition=new_td_arn)["taskDefinition"]
    print("[router/ensure] new TD env:", [
        {"name": e["name"], "value": e["value"]}
        for e in desc["containerDefinitions"][idx].get("environment", [])
    ])

    # --- Create/Update Service (idempotent + fallback) ---
    svc_resp = ecs.describe_services(cluster=cluster_arn, services=[service_name])
    svcs = svc_resp.get("services", [])
    exists = svcs and svcs[0].get("status") != "INACTIVE"

    deploy_cfg = {
        "deploymentCircuitBreaker": {"enable": True, "rollback": True},
        "maximumPercent": 200,
        "minimumHealthyPercent": 0,   # allow replace-before-add to avoid 8081 clashes
    }

    if exists:
        # UPDATE path (retry only for transient/server-side errors)
        for attempt in range(6):
            try:
                resp = ecs.update_service(
                    cluster=cluster_arn,
                    service=service_name,
                    taskDefinition=new_td_arn,
                    desiredCount=desired_count,
                    forceNewDeployment=True,
                    deploymentConfiguration=deploy_cfg,  # NEW
                )
                print(f"[router/ensure] update_service OK arn={resp['service']['serviceArn']}")
                break
            except Exception as e:
                if _is_transient(e) and attempt < 5:
                    print(f"[router/ensure] update_service transient err; attempt={attempt}; err={repr(e)}")
                    _sleep_backoff(attempt)
                    continue
                raise
    else:
        # CREATE path with backoff; fall back to no-SD then attach SD
        created = False
        for attempt in range(6):
            try:
                print(f"[router/ensure] create_service with SD attempt={attempt}")
                ecs.create_service(
                    cluster=cluster_arn,
                    serviceName=service_name,
                    taskDefinition=new_td_arn,
                    desiredCount=desired_count,
                    launchType="FARGATE",
                    deploymentConfiguration=deploy_cfg,  # NEW
                    networkConfiguration={
                        "awsvpcConfiguration": {
                            "subnets": subnets,
                            "securityGroups": security_groups,
                            "assignPublicIp": "ENABLED",
                        }
                    },
                    serviceRegistries=[{"registryArn": registry_arn}] if registry_arn else [],
                    enableExecuteCommand=True,
                    propagateTags="SERVICE",
                    tags=[{"key": "scenario_id", "value": scenario_id}],
                )
                created = True
                print("[router/ensure] create_service OK (with SD)")
                break
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                msg  = e.response.get("Error", {}).get("Message", "")
                if code in {"ServerException", "ServiceUnavailableException"}:
                    print(f"[router/ensure] create_service server-side err; attempt={attempt}; msg={msg}")
                    _sleep_backoff(attempt)
                    continue
                raise

        if not created:
            print("[router/ensure] fallback: create without SD, then attach SD")
            ecs.create_service(
                cluster=cluster_arn,
                serviceName=service_name,
                taskDefinition=new_td_arn,
                desiredCount=desired_count,
                launchType="FARGATE",
                deploymentConfiguration=deploy_cfg,  # NEW
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": subnets,
                        "securityGroups": security_groups,
                        "assignPublicIp": "ENABLED",
                    }
                },
                enableExecuteCommand=True,
                propagateTags="SERVICE",
                tags=[{"key": "scenario_id", "value": scenario_id}],
            )
            time.sleep(3)
            ecs.update_service(
                cluster=cluster_arn,
                service=service_name,
                serviceRegistries=[{"registryArn": registry_arn}],
                forceNewDeployment=True,
            )
            print("[router/ensure] attached SD via update_service")

        # cosmetic wait until service reflects new TD
        for _ in range(6):
            s = ecs.describe_services(cluster=cluster_arn, services=[service_name])["services"][0]
            if s.get("taskDefinition") == new_td_arn:
                break
            time.sleep(1.0)

    # --- Fail-loop guard: stop if tasks keep failing quickly ---
    if auto_stop_on_fail and desired_count > 0:
        start = time.time()
        failures = 0
        td_target = new_td_arn
        while time.time() - start < fail_window_seconds:
            svc = ecs.describe_services(cluster=cluster_arn, services=[service_name])["services"][0]
            if svc.get("runningCount", 0) >= desired_count:
                break  # reached healthy state

            task_arns = ecs.list_tasks(
                cluster=cluster_arn,
                serviceName=service_name,
                desiredStatus="STOPPED",
                maxResults=10,
            ).get("taskArns", [])

            if task_arns:
                tasks = ecs.describe_tasks(cluster=cluster_arn, tasks=task_arns)["tasks"]
                for t in tasks:
                    if t.get("taskDefinitionArn") == td_target and t.get("lastStatus") == "STOPPED":
                        failures += 1

            if failures >= fail_threshold:
                print(f"[router/ensure] auto-stop: {failures} failures within {fail_window_seconds}s; scaling to 0")
                try:
                    ecs.update_service(cluster=cluster_arn, service=service_name, desiredCount=0)
                except Exception as e:
                    print("[router/ensure] auto-stop update_service error:", repr(e))
                break

            time.sleep(5)

    # --- DNS (Cloud Map) ---
    dns = router_dns_name(region, cloudmap_namespace_id, service_name)
    print(f"[router/ensure] <<< done dns={dns}")
    return dns

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
        if code in ("ClusterNotFoundException", "ServiceNotFoundException"):
            return
        raise  # Re-raise the original exception
