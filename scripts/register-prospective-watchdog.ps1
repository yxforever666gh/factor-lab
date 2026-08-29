[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$RuntimePython,
    [string]$ReleaseTag = "5.8",
    [string]$TaskName,
    [ValidateSet("first_cycle", "continuous")]
    [string]$ControllerMode = "first_cycle",
    [switch]$PlanOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $IsWindows) {
    throw "the prospective watchdog task can only be registered on Windows"
}
if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "PowerShell 7 or newer is required"
}
$localTimeZoneId = [TimeZoneInfo]::Local.Id
if ($localTimeZoneId -ne "China Standard Time") {
    throw "the prospective watchdog requires Windows time zone China Standard Time"
}
if ($ReleaseTag -notmatch "^[0-9]+\.[0-9]+$") {
    throw "ReleaseTag must be a major.minor tag"
}
$releaseVersion = @($ReleaseTag.Split(".") | ForEach-Object { [uint64]::Parse($_) })
if (
    $ControllerMode -eq "continuous" -and
    ($releaseVersion[0] -lt 5 -or ($releaseVersion[0] -eq 5 -and $releaseVersion[1] -lt 8))
) {
    throw "continuous controller mode requires release tag 5.8 or newer"
}
if ([string]::IsNullOrWhiteSpace($TaskName)) {
    $TaskName = if ($ControllerMode -eq "continuous") {
        "Factor Lab Prospective Continuous Watchdog $ReleaseTag"
    }
    else {
        "Factor Lab Prospective Watchdog $ReleaseTag"
    }
}

$rootItem = Get-Item -LiteralPath $ProjectRoot -ErrorAction Stop
if (-not $rootItem.PSIsContainer) { throw "ProjectRoot is not a directory" }
$ProjectRoot = $rootItem.FullName
if ([string]::IsNullOrWhiteSpace($RuntimePython)) {
    $RuntimePython = Join-Path $ProjectRoot "runtime/environments/$ReleaseTag/Scripts/python.exe"
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
if ($ControllerMode -eq "continuous") {
    $tokens = $null
    $parseErrors = $null
    $scriptAst = [Management.Automation.Language.Parser]::ParseFile($capsuleScript, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count -ne 0) {
        throw "the formal $ReleaseTag watchdog runner does not parse"
    }
    $parameterNames = @($scriptAst.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
    if ($parameterNames -notcontains "ControllerMode") {
        throw "the formal $ReleaseTag watchdog runner does not support continuous controller mode"
    }
}

if (-not $PlanOnly -and (Get-ScheduledTask -TaskName $TaskName -TaskPath "\" -ErrorAction SilentlyContinue)) {
    throw "scheduled task already exists; refusing to replace it"
}

$pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
function Quote-TaskArgument {
    param([string]$Value)
    if ($Value.Contains('"')) { throw "task argument contains an unsupported quote" }
    return '"' + $Value + '"'
}
$taskArgumentItems = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", (Quote-TaskArgument $capsuleScript),
    "-ProjectRoot", (Quote-TaskArgument $ProjectRoot),
    "-RuntimePython", (Quote-TaskArgument $RuntimePython),
    "-Origin", "task"
)
if ($ControllerMode -eq "continuous") {
    $taskArgumentItems += @("-ControllerMode", "continuous")
}
$taskArguments = $taskArgumentItems -join " "
$action = New-ScheduledTaskAction -Execute $pwsh -Argument $taskArguments -WorkingDirectory $ProjectRoot

$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
$scheduledTimesLocal = @()
$scheduledDays = @()
$weekendScheduledTimesLocal = @()
$weekendScheduledDays = @()
$logonTriggerEnabled = $true
if ($ControllerMode -eq "first_cycle") {
    $notBefore = [DateTimeOffset]::Parse("2026-08-31T07:00:00Z")
    $notAfter = [DateTimeOffset]::Parse("2026-09-01T01:15:00Z")
    $fastStart = [DateTimeOffset]::Parse("2026-08-31T23:55:00Z")
    $slowDuration = ($notAfter - $notBefore) + [TimeSpan]::FromMinutes(30)
    $fastDuration = ($notAfter - $fastStart) + [TimeSpan]::FromMinutes(5)
    $slowTrigger = New-ScheduledTaskTrigger -Once -At $notBefore.LocalDateTime -RepetitionInterval ([TimeSpan]::FromMinutes(30)) -RepetitionDuration $slowDuration
    $fastTrigger = New-ScheduledTaskTrigger -Once -At $fastStart.LocalDateTime -RepetitionInterval ([TimeSpan]::FromMinutes(5)) -RepetitionDuration $fastDuration
    $logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $identity
    $triggers = @($slowTrigger, $fastTrigger, $logonTrigger)
    $multipleInstances = "IgnoreNew"
    $scheduleKind = "first_cycle_window"
}
else {
    # Dense weekday checkpoints cover post-close progress and the next-open
    # deadline. Coarse weekend checkpoints and logon recovery prevent a Friday
    # provider/network failure from remaining invisible until Monday morning.
    $scheduledMinutes = [Collections.Generic.List[int]]::new()
    foreach ($minute in @(30, 900, 990, 1050, 1080, 1110, 1140, 1170, 1200, 1230, 1410)) {
        $scheduledMinutes.Add($minute)
    }
    for ($minute = 475; $minute -le 555; $minute += 5) {
        $scheduledMinutes.Add($minute)
    }
    $scheduledMinutes.Sort()
    $weekdays = @(
        [DayOfWeek]::Monday,
        [DayOfWeek]::Tuesday,
        [DayOfWeek]::Wednesday,
        [DayOfWeek]::Thursday,
        [DayOfWeek]::Friday
    )
    $weekends = @(
        [DayOfWeek]::Saturday,
        [DayOfWeek]::Sunday
    )
    $weekendMinutes = @(30, 510, 990, 1080, 1230, 1410)
    $scheduledDays = @($weekdays | ForEach-Object { $_.ToString() })
    $weekendScheduledDays = @($weekends | ForEach-Object { $_.ToString() })
    $weekdayTriggers = @(
        foreach ($minute in $scheduledMinutes) {
            New-ScheduledTaskTrigger `
                -Weekly `
                -WeeksInterval 1 `
                -DaysOfWeek $weekdays `
                -At ([DateTime]::Today.AddMinutes($minute))
        }
    )
    $weekendTriggers = @(
        foreach ($minute in $weekendMinutes) {
            New-ScheduledTaskTrigger `
                -Weekly `
                -WeeksInterval 1 `
                -DaysOfWeek $weekends `
                -At ([DateTime]::Today.AddMinutes($minute))
        }
    )
    $logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $identity
    $triggers = @($weekdayTriggers) + @($weekendTriggers) + @($logonTrigger)
    $scheduledTimesLocal = @(
        foreach ($minute in $scheduledMinutes) {
            "{0:00}:{1:00}" -f [Math]::Floor($minute / 60), ($minute % 60)
        }
    )
    $weekendScheduledTimesLocal = @(
        foreach ($minute in $weekendMinutes) {
            "{0:00}:{1:00}" -f [Math]::Floor($minute / 60), ($minute % 60)
        }
    )
    # Each invocation still acquires the runner's file-handle lock. Parallel
    # prevents Task Scheduler's IgnoreNew policy from silently dropping every
    # later checkpoint behind one slow or hung instance.
    $multipleInstances = "Parallel"
    $scheduleKind = "continuous_with_weekend_recovery"
}
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances $multipleInstances `
    -StartWhenAvailable `
    -WakeToRun `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::FromMinutes(45))
$task = New-ScheduledTask -Action $action -Trigger $triggers -Principal $principal -Settings $settings
if (-not $PlanOnly) {
    Register-ScheduledTask -TaskName $TaskName -TaskPath "\" -InputObject $task -ErrorAction Stop | Out-Null
}

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
    local_time_zone_id = $localTimeZoneId
    controller_mode = $ControllerMode
    schedule_kind = $scheduleKind
    trigger_count = $triggers.Count
    scheduled_days = $scheduledDays
    scheduled_times_local = $scheduledTimesLocal
    weekend_scheduled_days = $weekendScheduledDays
    weekend_scheduled_times_local = $weekendScheduledTimesLocal
    logon_trigger_enabled = $logonTriggerEnabled
    multiple_instances = $multipleInstances
    action_arguments = $taskArguments
    registered = (-not $PlanOnly)
} | ConvertTo-Json -Depth 4
