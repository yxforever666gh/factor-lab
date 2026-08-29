[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$RuntimePython,
    [string]$ReleaseTag = "5.7",
    [string]$TaskName = "Factor Lab Prospective Watchdog 5.7"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $IsWindows) {
    throw "the prospective watchdog task can only be registered on Windows"
}
if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "PowerShell 7 or newer is required"
}
if ($ReleaseTag -notmatch "^[0-9]+\.[0-9]+$") {
    throw "ReleaseTag must be a major.minor tag"
}

$rootItem = Get-Item -LiteralPath $ProjectRoot -ErrorAction Stop
if (-not $rootItem.PSIsContainer) { throw "ProjectRoot is not a directory" }
$ProjectRoot = $rootItem.FullName
if ([string]::IsNullOrWhiteSpace($RuntimePython)) {
    $RuntimePython = Join-Path $ProjectRoot "runtime/environments/5.7/Scripts/python.exe"
}
$pythonItem = Get-Item -LiteralPath $RuntimePython -ErrorAction Stop
if ($pythonItem.PSIsContainer) { throw "RuntimePython is not a file" }
$RuntimePython = $pythonItem.FullName

function Invoke-GitText {
    param([string[]]$Arguments)
    $text = & git -C $ProjectRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "git command failed while resolving the published release tag"
    }
    return (($text | Out-String).Trim())
}

$tagType = Invoke-GitText -Arguments @("cat-file", "-t", "refs/tags/$ReleaseTag")
if ($tagType -ne "tag") {
    throw "refs/tags/$ReleaseTag is not an annotated tag"
}
$releaseCommit = Invoke-GitText -Arguments @("rev-parse", "$ReleaseTag^{commit}")
if ($releaseCommit -notmatch "^[0-9a-f]{40}$") {
    throw "the release tag did not peel to one commit"
}
$capsuleScript = Join-Path $ProjectRoot "runtime/prospective/5.0/release-runners/$releaseCommit/scripts/invoke-prospective-watchdog.ps1"
if (-not (Test-Path -LiteralPath $capsuleScript -PathType Leaf)) {
    throw "the formal $ReleaseTag release capsule does not contain invoke-prospective-watchdog.ps1"
}
$capsuleScript = (Get-Item -LiteralPath $capsuleScript).FullName

if (Get-ScheduledTask -TaskName $TaskName -TaskPath "\" -ErrorAction SilentlyContinue) {
    throw "scheduled task already exists; refusing to replace it"
}

$pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
function Quote-TaskArgument {
    param([string]$Value)
    if ($Value.Contains('"')) { throw "task argument contains an unsupported quote" }
    return '"' + $Value + '"'
}
$taskArguments = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", (Quote-TaskArgument $capsuleScript),
    "-ProjectRoot", (Quote-TaskArgument $ProjectRoot),
    "-RuntimePython", (Quote-TaskArgument $RuntimePython),
    "-Origin", "task"
) -join " "
$action = New-ScheduledTaskAction -Execute $pwsh -Argument $taskArguments -WorkingDirectory $ProjectRoot

$notBefore = [DateTimeOffset]::Parse("2026-08-31T07:00:00Z")
$notAfter = [DateTimeOffset]::Parse("2026-09-01T01:15:00Z")
$fastStart = [DateTimeOffset]::Parse("2026-08-31T23:55:00Z")
$slowDuration = ($notAfter - $notBefore) + [TimeSpan]::FromMinutes(30)
$fastDuration = ($notAfter - $fastStart) + [TimeSpan]::FromMinutes(5)
$slowTrigger = New-ScheduledTaskTrigger -Once -At $notBefore.LocalDateTime -RepetitionInterval ([TimeSpan]::FromMinutes(30)) -RepetitionDuration $slowDuration
$fastTrigger = New-ScheduledTaskTrigger -Once -At $fastStart.LocalDateTime -RepetitionInterval ([TimeSpan]::FromMinutes(5)) -RepetitionDuration $fastDuration
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $identity
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -WakeToRun `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::FromMinutes(45))
$task = New-ScheduledTask -Action $action -Trigger @($slowTrigger, $fastTrigger, $logonTrigger) -Principal $principal -Settings $settings
Register-ScheduledTask -TaskName $TaskName -TaskPath "\" -InputObject $task -ErrorAction Stop | Out-Null

[pscustomobject]@{
    task_name = $TaskName
    task_path = "\"
    release_tag = $ReleaseTag
    release_commit = $releaseCommit
    watchdog_script = $capsuleScript
    runtime_python = $RuntimePython
    principal = $identity
    logon_type = "Interactive"
    run_level = "Limited"
} | ConvertTo-Json -Depth 4
