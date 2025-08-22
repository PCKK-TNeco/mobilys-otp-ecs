param(
  [string]$Region = "ap-northeast-1",
  [string]$AccountId,
  [string]$RepoPrefix = "mobilys-otp"
)

if (-not $AccountId) {
  $AccountId = (aws sts get-caller-identity --query Account --output text)
}

$ErrorActionPreference = "Stop"

$routerRepo = "$AccountId.dkr.ecr.$Region.amazonaws.com/$RepoPrefix/otp-router"
$builderRepo = "$AccountId.dkr.ecr.$Region.amazonaws.com/$RepoPrefix/otp-builder"

# Ensure repos exist
function Ensure-EcrRepo($name) {
  $exists = aws ecr describe-repositories --repository-names $name --region $Region 2>$null
  if (-not $?) {
    aws ecr create-repository --repository-name $name --region $Region | Out-Null
  }
}

Ensure-EcrRepo "$RepoPrefix/otp-router"
Ensure-EcrRepo "$RepoPrefix/otp-builder"

aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin "$AccountId.dkr.ecr.$Region.amazonaws.com"

# Build & push router
docker build -t $routerRepo:latest -f otp/Dockerfile.router otp
docker push $routerRepo:latest

# Build & push builder
docker build -t $builderRepo:latest -f otp/Dockerfile.builder otp
docker push $builderRepo:latest

Write-Host "Done."
Write-Host "ROUTER_IMAGE=$routerRepo:latest"
Write-Host "BUILDER_IMAGE=$builderRepo:latest"
