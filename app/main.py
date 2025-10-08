# app/main.py
import os
import boto3
from fastapi import FastAPI, UploadFile, Form, File, HTTPException

from app.ecs_control import (
    submit_builder_and_wait,
    ensure_router_service,
    delete_router_service,
)
from botocore.exceptions import ClientError
from typing import Literal
import time
import asyncio
import socket
from fastapi import Response

app = FastAPI()

# --- Config from env ---
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
GRAPHS_BUCKET = os.getenv("GRAPHS_BUCKET")

ECS_CLUSTER_ARN = os.getenv("ECS_CLUSTER_ARN")
ECS_SUBNETS = [s.strip() for s in os.getenv("ECS_SUBNETS", "").split(",") if s.strip()]
ECS_SGS = [s.strip() for s in os.getenv("ECS_SECURITY_GROUPS", "").split(",") if s.strip()]
CLOUDMAP_NAMESPACE_ID = os.getenv("CLOUDMAP_NAMESPACE_ID")

BUILDER_TASK_FAMILY = os.getenv("BUILDER_TASKDEF", "otp-builder")
ROUTER_TASK_FAMILY  = os.getenv("ROUTER_TASKDEF_BASE", "otp-router")
TASK_EXEC_ROLE_ARN  = os.getenv("TASK_EXEC_ROLE_ARN") or None
TASK_ROLE_ARN       = os.getenv("TASK_ROLE_ARN") or None
ROUTER_IMAGE        = os.getenv("ROUTER_IMAGE") or None
BUILDER_IMAGE       = os.getenv("BUILDER_IMAGE") or None

SNIPPETS_DIR = os.getenv("NGINX_SNIPPETS_DIR", "/shared/nginx/routers").rstrip("/")

# Logs groups (must exist)
LOG_GROUP_BUILDER = os.getenv("LOG_GROUP_BUILDER", "/mobilys-otp/builder")
LOG_GROUP_ROUTER  = os.getenv("LOG_GROUP_ROUTER", "/mobilys-otp/router")

ecs = boto3.client("ecs", region_name=AWS_REGION)

# Idle after which we scale to 0 (seconds). Override with env ROUTER_IDLE_SECONDS if you want.
IDLE_SECS = int(os.getenv("ROUTER_IDLE_SECONDS", "1800"))  # 15 minutes
_last_hit = {}  # scenario_id -> last epoch seconds

def _tcp_check(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def _router_host(sid: str) -> str:
    # must match the host used in site.conf
    return f"router-{sid}.mobilys-staging.mobilys-otp.local"


def _require(cond, msg):
    if not cond:
        raise HTTPException(status_code=500, detail=msg)


def _write_nginx_snippet(scenario_id: str, host: str, port: int = 8081):
    os.makedirs(SNIPPETS_DIR, exist_ok=True)
    path = f"{SNIPPETS_DIR}/{scenario_id}.conf"
    conf = f"""
# generated for {scenario_id}
location /router/{scenario_id}/ {{
  proxy_set_header Host $host;
  proxy_set_header X-Real-IP $remote_addr;
  proxy_http_version 1.1;
  proxy_pass http://{host}:{port}/;
}}
""".lstrip()
    with open(path, "w") as f:
        f.write(conf)
    return path


def _remove_nginx_snippet(scenario_id: str):
    try:
        os.remove(f"{SNIPPETS_DIR}/{scenario_id}.conf")
    except FileNotFoundError:
        pass

def _bucket_is_versioned(bucket: str) -> bool:
    try:
        v = s3.get_bucket_versioning(Bucket=bucket)
        return v.get("Status") in ("Enabled", "Suspended")
    except Exception:
        return False


def _delete_prefix_unversioned(bucket: str, prefix: str) -> dict:
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
            deleted += len(objs)
    # remove the “folder” marker if it exists
    try:
        s3.delete_object(Bucket=bucket, Key=prefix)
    except Exception:
        pass
    return {"objects": deleted}


def _delete_prefix_versioned(bucket: str, prefix: str) -> dict:
    versions = 0
    markers = 0
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        batch = []
        for v in page.get("Versions", []):
            batch.append({"Key": v["Key"], "VersionId": v["VersionId"]})
        versions += len(page.get("Versions", []))
        for m in page.get("DeleteMarkers", []):
            batch.append({"Key": m["Key"], "VersionId": m["VersionId"]})
        markers += len(page.get("DeleteMarkers", []))
        for i in range(0, len(batch), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch[i:i+1000], "Quiet": True})
    try:
        s3.delete_object(Bucket=bucket, Key=prefix)
    except Exception:
        pass
    return {"versions": versions, "delete_markers": markers}


def _delete_prefix(bucket: str, prefix: str) -> dict:
    return (
        _delete_prefix_versioned(bucket, prefix)
        if _bucket_is_versioned(bucket)
        else _delete_prefix_unversioned(bucket, prefix)
    )

@app.get("/api/router_warmup")
def router_warmup(rid: str, resp: Response):
    """
    - Record 'last used' for rid
    - Ensure ECS service router-<rid> is running (desiredCount=1) or create it
    - Wait until TCP :8081 on router host is reachable
    - Return 204 (no body) so Nginx auth_request can proceed
    """
    now = time.time()
    _last_hit[rid] = now

    service_name = f"router-{rid}"

    try:
        # Try to scale an existing service up
        ecs.update_service(
            cluster=ECS_CLUSTER_ARN,
            service=service_name,
            desiredCount=1,
        )
        waiter = ecs.get_waiter("services_stable")
        waiter.wait(
            cluster=ECS_CLUSTER_ARN,
            services=[service_name],
            WaiterConfig={"Delay": 5, "MaxAttempts": 60},
        )
    except ecs.exceptions.ServiceNotFoundException:
        # Create on demand
        ensure_router_service(
            region=AWS_REGION,
            cluster_arn=ECS_CLUSTER_ARN,
            subnets=ECS_SUBNETS,
            security_groups=ECS_SGS,
            cloudmap_namespace_id=CLOUDMAP_NAMESPACE_ID,
            service_prefix="router",
            scenario_id=rid,
            task_family=ROUTER_TASK_FAMILY,
            task_exec_role_arn=TASK_EXEC_ROLE_ARN,
            task_role_arn=TASK_ROLE_ARN,
            image=ROUTER_IMAGE,
            env={
                "AWS_REGION": AWS_REGION,
                "GRAPHS_BUCKET": GRAPHS_BUCKET,
                "GRAPH_SCENARIO_ID": rid,
            },
            desired_count=1,
            container_port=8081,
            cw_log_group=LOG_GROUP_ROUTER,
        )
    except Exception as e:
        print("[warmup] update/create failed:", repr(e))
        resp.status_code = 503
        return {"error": "router_warmup_failed", "detail": str(e)}

    # DNS may need a moment; wait for socket to open
    host = _router_host(rid)
    start = time.time()
    while time.time() - start < 300:
        if _tcp_check(host, 8081):
            resp.status_code = 204  # required by auth_request
            return
        time.sleep(2.0)

    resp.status_code = 504
    return {"error": "router_start_timeout"}


@app.get("/s3/pbf_files")
def list_pbf_files(bucket: str):

    _require(bucket, "Bucket name is required")
    try:
        resp = s3.list_objects_v2(Bucket=bucket)
    except s3.exceptions.NoSuchBucket:
        raise HTTPException(status_code=404, detail=f"Bucket '{bucket}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    items = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.lower().endswith((".pbf", ".osm")):
            name = key.rsplit("/", 1)[-1]  # take only the filename
            base = os.path.splitext(name)[0]
            items.append(base)

    return {"file_names": sorted(items)}


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/build_graph")
async def build_graph(
    scenario_id: str = Form(...),
    prefecture: str = Form(...),
    gtfs_file: UploadFile = File(...),
    graph_type: Literal["osm", "drm"] = Form("osm", alias="type"),  # <-- NEW
):
    """Synchronous: build Graph.obj in a one-off task, then bring up router service and route traffic."""
    _require(GRAPHS_BUCKET, "GRAPHS_BUCKET not set")
    _require(ECS_CLUSTER_ARN and ECS_SUBNETS and ECS_SGS, "ECS cluster/subnets/SGs not set")
    _require(CLOUDMAP_NAMESPACE_ID, "Cloud Map namespace not set")
    _require(BUILDER_IMAGE, "BUILDER_IMAGE not set")
    _require(ROUTER_IMAGE, "ROUTER_IMAGE not set")

    # Pick prefix + extension from the requested type
    if graph_type == "osm":
        osm_prefix = "preloaded_osm_files"
        osm_ext = ".osm.pbf"
    else:  # "drm"
        osm_prefix = "preloaded_drm_files"
        osm_ext = ".osm"

    # Upload GTFS to s3://bucket/gtfs/<scenario>/<filename>
    gtfs_key = f"gtfs/{scenario_id}/{gtfs_file.filename}"
    s3.upload_fileobj(gtfs_file.file, GRAPHS_BUCKET, gtfs_key)

    # Run builder task and wait
    print("[builder] submit_builder_and_wait: ENTER")
    ok, tail = submit_builder_and_wait(
        region=AWS_REGION,
        cluster_arn=ECS_CLUSTER_ARN,
        subnets=ECS_SUBNETS,
        security_groups=ECS_SGS,
        cloudwatch_log_group=LOG_GROUP_BUILDER,
        task_family=BUILDER_TASK_FAMILY,
        task_exec_role_arn=TASK_EXEC_ROLE_ARN,
        task_role_arn=TASK_ROLE_ARN,
        image=BUILDER_IMAGE,
        env={
            "AWS_REGION": AWS_REGION,
            "GRAPHS_BUCKET": GRAPHS_BUCKET,
            "OSM_PREFIX": osm_prefix, 
            "OSM_EXT": osm_ext, 
            "SCENARIO_ID": scenario_id,
            "PREFECTURE": prefecture,
            "S3_GTFS_URI": f"s3://{GRAPHS_BUCKET}/{gtfs_key}",
            "JAVA_TOOL_OPTIONS": "-Xmx8g -XX:+UseG1GC",
        },
    )
    print(f"[builder] submit_builder_and_wait: EXIT ok={ok}")
    if not ok:
        # include last lines for debugging
        raise HTTPException(status_code=500, detail="Graph build failed")

    # Ensure router service is running (DEBUG logs)
    print(f"[router] calling ensure_router_service for scenario_id={scenario_id}")
    try:
        dns = ensure_router_service(
            region=AWS_REGION,
            cluster_arn=ECS_CLUSTER_ARN,
            subnets=ECS_SUBNETS,
            security_groups=ECS_SGS,
            cloudmap_namespace_id=CLOUDMAP_NAMESPACE_ID,
            service_prefix="router",
            scenario_id=scenario_id,
            task_family=ROUTER_TASK_FAMILY,
            task_exec_role_arn=TASK_EXEC_ROLE_ARN,
            task_role_arn=TASK_ROLE_ARN,
            image=ROUTER_IMAGE,
            env={
                "AWS_REGION": AWS_REGION,
                "GRAPHS_BUCKET": GRAPHS_BUCKET,
                "GRAPH_SCENARIO_ID": scenario_id,
            },
            desired_count=1,
            container_port=8081,
            cw_log_group=LOG_GROUP_ROUTER,
        )
    except ClientError as e:
        # print full AWS error so we see *exactly* why CreateService/RegisterTaskDefinition failed
        print("[router] ensure_router_service ClientError:", e.response)
        raise HTTPException(status_code=500, detail=f"router-create failed: {e.response.get('Error', {})}")
    except Exception as e:
        print("[router] ensure_router_service Exception:", repr(e))
        raise

    print(f"[router] ensure_router_service OK; dns={dns}")

    # Add nginx route (hot-reload sidecar handles reload)
    _write_nginx_snippet(scenario_id, dns, 8081)

    return {"status": "success", "router_path": f"/router/{scenario_id}/"}

@app.post("/delete_graph")
async def delete_graph(scenario_id: str = Form(...)):
    # 1) Kill the router service (ignore if it's already gone)
    try:
        delete_router_service(
            region=AWS_REGION,
            cluster_arn=ECS_CLUSTER_ARN,
            service_name=f"router-{scenario_id}",
        )
    except Exception as e:
        print(f"[delete_graph] delete_router_service warning: {e}")

    # 2) Remove nginx snippet
    _remove_nginx_snippet(scenario_id)

    # 3) Purge S3 artifacts
    _require(GRAPHS_BUCKET, "GRAPHS_BUCKET not set")
    graphs_prefix = f"graphs/{scenario_id}/"
    gtfs_prefix = f"gtfs/{scenario_id}/"

    try:
        graphs_res = _delete_prefix(GRAPHS_BUCKET, graphs_prefix)
        gtfs_res = _delete_prefix(GRAPHS_BUCKET, gtfs_prefix)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 delete failed: {e}")

    return {
        "status": "success",
        "deleted": {
            "graphs_prefix": graphs_prefix,
            "graphs_result": graphs_res,
            "gtfs_prefix": gtfs_prefix,
            "gtfs_result": gtfs_res,
        },
    }

async def _idle_reaper_loop():
    while True:
        try:
            now = time.time()
            stale = [rid for rid, ts in _last_hit.items() if now - ts > IDLE_SECS]
            for rid in stale:
                svc = f"router-{rid}"
                print(f"[idle-reaper] scaling down {svc}")
                try:
                    ecs.update_service(
                        cluster=ECS_CLUSTER_ARN,
                        service=svc,
                        desiredCount=0,
                    )
                except Exception as e:
                    print(f"[idle-reaper] update_service({svc}) failed: {e}")
                _last_hit.pop(rid, None)
        except Exception as e:
            print("[idle-reaper] loop error:", e)

        await asyncio.sleep(60)  # check every minute

@app.on_event("startup")
async def _start_idle_reaper():
    asyncio.create_task(_idle_reaper_loop())
