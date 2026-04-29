$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
Set-Location $repoRoot

Write-Host "[INFO] Removing volumes (FULL RESET)..."
docker compose down -v

Write-Host "[INFO] Starting database containers only..."
docker compose up -d user-db-primary user-db-replica order-db-primary order-db-replica inventory-db-primary inventory-db-replica mongodb-primary mongodb-secondary

Write-Host "[INFO] Waiting 15 seconds before collecting startup logs..."
Start-Sleep -Seconds 15

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logDir = Join-Path $repoRoot "logs\db-init"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logPath = Join-Path $logDir "db-startup-$timestamp.log"

Write-Host "[INFO] Writing database logs to $logPath"
docker compose logs user-db-primary user-db-replica order-db-primary order-db-replica inventory-db-primary inventory-db-replica mongodb-primary mongodb-secondary | Tee-Object -FilePath $logPath

Write-Host "[INFO] Container status:"
docker compose ps

Write-Host "[INFO] Done. Use this to follow logs live:"
Write-Host "docker compose logs -f user-db-primary order-db-primary inventory-db-primary mongodb-primary"
