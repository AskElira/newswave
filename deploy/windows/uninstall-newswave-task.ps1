<# Removes the NewsWave scheduled task (stops it first). Elevated PowerShell. Data, .env and .armed are untouched. #>
param([string]$TaskName = "NewsWave")
$ErrorActionPreference = "Stop"
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
  Write-Host "Removed task '$TaskName'."
} else {
  Write-Host "No task named '$TaskName'."
}
