$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Stop native bridge supaya tidak bentrok port/session
Get-Process whatsapp-bridge -ErrorAction SilentlyContinue | Stop-Process -Force

# Pastikan Docker siap
$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
  docker info 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) { break }
  Start-Sleep -Seconds 3
}
if ($LASTEXITCODE -ne 0) {
  throw "Docker belum siap. Buka Docker Desktop dulu."
}

docker compose up -d --build
Write-Host "Bridge Docker running. Logs: docker logs -f whatsapp-bridge"
Write-Host "Stop: docker compose -f `"$PSScriptRoot\docker-compose.yml`" down"
