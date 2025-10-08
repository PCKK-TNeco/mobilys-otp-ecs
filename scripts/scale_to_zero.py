import boto3, time

REGION  = "ap-northeast-1"
CLUSTER = "mobilys-otp-staging-cluster"
PREFIX  = "otp-router-"
IDLE_AFTER_SEC = 1800  # 30 minutes

ecs = boto3.client("ecs", region_name=REGION)

def last_used(service_name: str) -> float:
    # TODO: integrate with your access logs/metrics to return last-use epoch per scenario
    return 0.0

def handler(event=None, context=None):
    paginator = ecs.get_paginator("list_services")
    for page in paginator.paginate(cluster=CLUSTER):
        arns = page.get("serviceArns", [])
        if not arns: 
            continue
        desc = ecs.describe_services(cluster=CLUSTER, services=arns)["services"]
        for s in desc:
            name = s["serviceName"]
            if not name.startswith(PREFIX): 
                continue
            if s.get("desiredCount", 0) == 0:
                continue
            if time.time() - last_used(name) > IDLE_AFTER_SEC:
                ecs.update_service(cluster=CLUSTER, service=name, desiredCount=0)
                print(f"Scaled {name} → 0")
