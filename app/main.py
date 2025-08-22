import os
from fastapi import FastAPI, UploadFile, Form, File, HTTPException
import boto3
from botocore.exceptions import ClientError
from app.ecs_control import (
    submit_builder_and_wait,
    ensure_router_service,
    delete_router_service,
)

app = FastAPI()

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
GRAPHS_BUCKET = os.getenv("GRAPHS_BUCKET")
OSM_PREFIX = os.getenv("OSM_PREFIX", "preloaded_osm_files")

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

s3 = boto3.client("s3", region_name=AWS_REGION)

def _require(cond, msg):
    if not cond:
        raise HTTPException(status_code=500, detail=msg)

def _write_nginx_snippet(scenario_id: str, host: str, port: int = 8081):
    os.makedirs(SNIPPETS_DIR, exist_ok=True)
    path = f"{SNIPPETS_DIR}/{scenario_id}.conf"
    with open(path, "w") as f:
        f.write(f"""
# generated for {scenario_id}
location /router/{scenario_id}/ {{
  proxy_set_header Host $host;
  proxy_set_header X-Real-IP $remote_addr;
  proxy_http_version 1.1;
  proxy_pass http://{host}:{port}/;
}}
""".lstrip())
    return path

def _remove_nginx_snippet(scenario_id: str):
    try:
        os.remove(f"{SNIPPETS_DIR}/{scenario_id}.conf")
    except FileNotFoundError:
        pass

@app.post("/build_graph")
async def build_graph(
    scenario_id: str = Form(...),
    prefecture: str = Form(...),
    gtfs_file: UploadFile = File(...)
):
    _require(GRAPHS_BUCKET, "GRAPHS_BUCKET not set")
    _require(ECS_CLUSTER_ARN and ECS_SUBNETS and ECS_SGS, "ECS cluster/subnets/SGs not set")
    _require(CLOUDMAP_NAMESPACE_ID, "Cloud Map namespace not set")

    # upload GTFS to s3://bucket/gtfs/<scenario>/<filename>
    gtfs_key = f"gtfs/{scenario_id}/{gtfs_file.filename}"
    s3.upload_fileobj(gtfs_file.file, GRAPHS_BUCKET, gtfs_key)

    ok, tail = submit_builder_and_wait(
        region=AWS_REGION,
        cluster_arn=ECS_CLUSTER_ARN,
        subnets=ECS_SUBNETS,
        security_groups=ECS_SGS,
        cloudwatch_log_group="/mobilys-otp/builder",
        task_family=BUILDER_TASK_FAMILY,
        task_exec_role_arn=TASK_EXEC_ROLE_ARN,
        task_role_arn=TASK_ROLE_ARN,
        image=BUILDER_IMAGE,
        env={
            "AWS_REGION": AWS_REGION,
            "GRAPHS_BUCKET": GRAPHS_BUCKET,
            "OSM_PREFIX": OSM_PREFIX,
            "SCENARIO_ID": scenario_id,
            "PREFECTURE": prefecture,
            "S3_GTFS_URI": f"s3://{GRAPHS_BUCKET}/{gtfs_key}",
        },
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Graph build failed")

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
        cw_log_group="/mobilys-otp/router",
    )

    _write_nginx_snippet(scenario_id, dns, 8081)
    return {"status": "success", "router": f"/router/{scenario_id}/"}

@app.post("/edit_graph")
async def edit_graph(
    scenario_id: str = Form(...),
    prefecture: str = Form(...),
    gtfs_file: UploadFile = File(...)
):
    return await build_graph(scenario_id, prefecture, gtfs_file)

@app.post("/delete_graph")
async def delete_graph(scenario_id: str = Form(...)):
    delete_router_service(
        region=AWS_REGION,
        cluster_arn=ECS_CLUSTER_ARN,
        service_name=f"router-{scenario_id}",
    )
    _remove_nginx_snippet(scenario_id)
    return {"status": "success"}
