<#
  Installs the "NewsWave" scheduled task: starts AT BOOT, runs `python -m newswave run` (OBSERVE unless .env says
  otherwise; --execute is a separate deliberate step after `arm`), whether or not anyone is logged on.

  Runs as the SAME Windows user you ran `claude` as: the classifier uses that user's Claude login (SYSTEM has none).
  Run from an ELEVATED PowerShell:   powershell -ExecutionPolicy Bypass -File deploy\windows\install-newswave-task.ps1
  You are asked for that user's Windows password (Task Scheduler needs it to run while logged off).
#>
param(
  [string]$TaskName = "NewsWave",
  [string]$RepoDir  = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)
$ErrorActionPreference = "Stop"

$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
         ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) { throw "Run this from an elevated (Administrator) PowerShell." }

$py = Join-Path $RepoDir ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "$py not found: create the venv first (see deploy\windows\SETUP.md)." }
if (-not (Test-Path (Join-Path $RepoDir ".env"))) { throw ".env not found in $RepoDir (see SETUP.md)." }

$me   = "$env:USERDOMAIN\$env:USERNAME"
$cred = Get-Credential -UserName $me -Message "Windows password for $me (the user logged in to claude)"

# -X utf8 == PYTHONUTF8=1 for the daemon itself; the user env var covers anything it spawns.
[Environment]::SetEnvironmentVariable("PYTHONUTF8", "1", "User")
$action   = New-ScheduledTaskAction -Execute $py -Argument "-X utf8 -m newswave run" -WorkingDirectory $RepoDir
$trigger  = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero) -StartWhenAvailable -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
  -User $cred.UserName -Password $cred.GetNetworkCredential().Password -RunLevel Limited -Force | Out-Null

Write-Host "Installed task '$TaskName' (runs as $($cred.UserName), at startup, restarts every 1 min on failure)."
Write-Host "Start now:  Start-ScheduledTask -TaskName $TaskName"
Write-Host "Stop:       New-Item '$RepoDir\data\STOP' -ItemType File    (graceful, within ~1 s)"
