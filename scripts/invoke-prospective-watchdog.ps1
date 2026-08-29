[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$RuntimePython,
    [ValidateSet("task", "heartbeat")]
    [string]$Origin = "task",
    [DateTimeOffset]$NotBeforeUtc = [DateTimeOffset]::Parse("2026-08-31T07:00:00Z"),
    [DateTimeOffset]$NotAfterUtc = [DateTimeOffset]::Parse("2026-09-01T01:15:00Z"),
    [DateTimeOffset]$SoftDeadlineUtc = [DateTimeOffset]::Parse("2026-09-01T00:55:00Z"),
    [ValidateRange(1, 12)]
    [int]$MaxActions = 12,
    [ValidateRange(1, 86400)]
    [int]$CommandTimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ContractId = "factor-lab/prospective-readiness/5.7"
$FirstSignalDate = "2026-08-31"
$script:RunWriter = $null
$script:RunStream = $null
$script:RunId = $null
$script:RunLogPath = $null
$script:AlertsDirectory = $null

function Get-Sha256Text {
    param([AllowEmptyString()][string]$Text)
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
    try {
        return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
    }
    finally {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
}

function Get-ArgvDigest {
    param([string[]]$Argv)
    $builder = [Text.StringBuilder]::new()
    foreach ($item in $Argv) {
        [void]$builder.Append($item.Length)
        [void]$builder.Append(":")
        [void]$builder.Append($item)
        [void]$builder.Append(";")
    }
    return Get-Sha256Text -Text $builder.ToString()
}

function Write-RunRecord {
    param([hashtable]$Record)
    if ($null -eq $script:RunWriter) {
        return
    }
    $base = [ordered]@{
        schema_version = 1
        run_id = $script:RunId
        recorded_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        origin = $Origin
    }
    foreach ($entry in $Record.GetEnumerator()) {
        $base[$entry.Key] = $entry.Value
    }
    $script:RunWriter.WriteLine(($base | ConvertTo-Json -Compress -Depth 12))
    $script:RunWriter.Flush()
    $script:RunStream.Flush($true)
}

function Write-CreateOnlyAlert {
    param(
        [int]$ExitCode,
        [string]$Reason,
        [AllowNull()][object]$Readiness
    )
    if ($ExitCode -notin @(3, 4) -or $null -eq $script:AlertsDirectory) {
        return
    }
    $alertId = "{0}-{1}" -f [DateTimeOffset]::UtcNow.ToString("yyyyMMddTHHmmssfffffffZ"), [Guid]::NewGuid().ToString("N")
    $path = Join-Path $script:AlertsDirectory "$alertId.json"
    $fingerprintInput = [ordered]@{
        exit_code = $ExitCode
        reason = $Reason
        readiness_status = if ($null -ne $Readiness) { [string]$Readiness.status } else { $null }
        readiness_reason = if ($null -ne $Readiness) { [string]$Readiness.reason } else { $null }
    }
    $payload = [ordered]@{
        schema_version = 1
        kind = "prospective_watchdog_alert"
        contract_id = $ContractId
        alert_id = $alertId
        run_id = $script:RunId
        origin = $Origin
        created_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        exit_code = $ExitCode
        reason = $Reason
        fingerprint_sha256 = Get-Sha256Text -Text ($fingerprintInput | ConvertTo-Json -Compress)
        readiness_status = if ($null -ne $Readiness) { [string]$Readiness.status } else { $null }
        readiness_reason = if ($null -ne $Readiness) { [string]$Readiness.reason } else { $null }
        run_log = if ($null -ne $script:RunLogPath) { $script:RunLogPath } else { $null }
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes(($payload | ConvertTo-Json -Compress -Depth 8) + "`n")
    $stream = $null
    try {
        $stream = [IO.FileStream]::new($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally {
        if ($null -ne $stream) { $stream.Dispose() }
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
}

function Invoke-PythonProcess {
    param(
        [string]$Role,
        [string[]]$Argv
    )
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $RuntimePython
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.WorkingDirectory = $ProjectRoot
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($item in $Argv) {
        [void]$start.ArgumentList.Add($item)
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $stdout = ""
    $stderr = ""
    $timedOut = $false
    $startedAt = [DateTimeOffset]::UtcNow
    try {
        if (-not $process.Start()) {
            throw "process did not start"
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($CommandTimeoutSeconds * 1000)) {
            $timedOut = $true
            try { $process.Kill($true) } catch { }
        }
        $process.WaitForExit()
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        $exitCode = if ($timedOut) { -1 } else { $process.ExitCode }
    }
    catch {
        $stderr = $_.Exception.Message
        $exitCode = -1
    }
    finally {
        $process.Dispose()
    }

    $stdoutBytes = [Text.Encoding]::UTF8.GetBytes($stdout)
    $stderrBytes = [Text.Encoding]::UTF8.GetBytes($stderr)
    try {
        Write-RunRecord -Record @{
            event = "process"
            role = $Role
            argv_count = $Argv.Count
            argv_sha256 = Get-ArgvDigest -Argv $Argv
            started_at_utc = $startedAt.ToString("o")
            duration_milliseconds = [int64]([DateTimeOffset]::UtcNow - $startedAt).TotalMilliseconds
            exit_code = $exitCode
            timed_out = $timedOut
            stdout_length_bytes = $stdoutBytes.Length
            stdout_sha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($stdoutBytes)).ToLowerInvariant()
            stderr_length_bytes = $stderrBytes.Length
            stderr_sha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($stderrBytes)).ToLowerInvariant()
        }
    }
    finally {
        [Array]::Clear($stdoutBytes, 0, $stdoutBytes.Length)
        [Array]::Clear($stderrBytes, 0, $stderrBytes.Length)
    }
    return [pscustomobject]@{
        ExitCode = $exitCode
        Stdout = $stdout
        Stderr = $stderr
        TimedOut = $timedOut
    }
}

function Convert-Readiness {
    param([string]$Text)
    try {
        $report = $Text | ConvertFrom-Json -Depth 100
    }
    catch {
        throw "readiness stdout is not one JSON document"
    }
    if ($null -eq $report -or [string]$report.contract_id -ne $ContractId) {
        throw "readiness contract_id is not $ContractId"
    }
    if ([string]$report.kind -ne "prospective_readiness") {
        throw "readiness kind is invalid"
    }
    return $report
}

function Test-FirstDecisionComplete {
    param([AllowNull()][object]$Readiness)
    if ($null -eq $Readiness -or $null -eq $Readiness.ledger) {
        return $false
    }
    try { $count = [int64]$Readiness.ledger.decision_count } catch { return $false }
    return (
        $count -ge 1 -and
        [string]$Readiness.ledger.last_decision_signal_date -eq $FirstSignalDate -and
        [string]$Readiness.ledger.phase -ne "awaiting_receipt"
    )
}

function Complete-Watchdog {
    param(
        [int]$ProposedExitCode,
        [string]$Reason,
        [AllowNull()][object]$LastReadiness
    )
    $exitCode = $ProposedExitCode
    $finalReason = $Reason
    if (
        $exitCode -ne 4 -and
        [DateTimeOffset]::UtcNow -ge $SoftDeadlineUtc.ToUniversalTime() -and
        -not (Test-FirstDecisionComplete -Readiness $LastReadiness)
    ) {
        $exitCode = 3
        $finalReason = "first_decision_soft_deadline_unmet"
    }
    Write-RunRecord -Record @{
        event = "finish"
        exit_code = $exitCode
        reason = $finalReason
    }
    if ($exitCode -in @(3, 4)) {
        Write-CreateOnlyAlert -ExitCode $exitCode -Reason $finalReason -Readiness $LastReadiness
    }
    return $exitCode
}

$lockStream = $null
try {
    if ($PSVersionTable.PSVersion.Major -lt 7) {
        throw "PowerShell 7 or newer is required"
    }
    if ($NotBeforeUtc -gt $SoftDeadlineUtc -or $SoftDeadlineUtc -gt $NotAfterUtc) {
        throw "NotBeforeUtc, SoftDeadlineUtc, and NotAfterUtc are not ordered"
    }

    $now = [DateTimeOffset]::UtcNow
    if ($now -lt $NotBeforeUtc.ToUniversalTime() -or $now -gt $NotAfterUtc.ToUniversalTime()) {
        exit 0
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

    $operationsDirectory = Join-Path $ProjectRoot "runtime/operations/prospective-watchdog-5.7"
    $runsDirectory = Join-Path $operationsDirectory "runs"
    $script:AlertsDirectory = Join-Path $operationsDirectory "alerts"
    [void][IO.Directory]::CreateDirectory($runsDirectory)
    [void][IO.Directory]::CreateDirectory($script:AlertsDirectory)
    $lockPath = Join-Path $operationsDirectory "controller.lock"
    try {
        $lockStream = [IO.FileStream]::new($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    }
    catch [IO.IOException] {
        exit 2
    }

    $script:RunId = "{0}-{1}" -f $now.ToString("yyyyMMddTHHmmssfffffffZ"), [Guid]::NewGuid().ToString("N")
    $script:RunLogPath = Join-Path $runsDirectory "$($script:RunId).jsonl"
    $script:RunStream = [IO.FileStream]::new($script:RunLogPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    $script:RunWriter = [IO.StreamWriter]::new($script:RunStream, [Text.UTF8Encoding]::new($false))
    Write-RunRecord -Record @{
        event = "start"
        project_root_sha256 = Get-Sha256Text -Text $ProjectRoot
        runtime_python_sha256 = Get-Sha256Text -Text $RuntimePython
        max_actions = $MaxActions
        command_timeout_seconds = $CommandTimeoutSeconds
        not_before_utc = $NotBeforeUtc.ToUniversalTime().ToString("o")
        not_after_utc = $NotAfterUtc.ToUniversalTime().ToString("o")
        soft_deadline_utc = $SoftDeadlineUtc.ToUniversalTime().ToString("o")
    }

    $prefix = @("-I", "-m", "factor_lab.cli", "--root", $ProjectRoot)
    $actionsExecuted = 0
    $lastReadiness = $null
    while ($true) {
        $observed = Invoke-PythonProcess -Role "readiness" -Argv @($prefix + @("prospective", "readiness"))
        if ($observed.TimedOut) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "readiness_timeout" -LastReadiness $lastReadiness)
        }
        try {
            $lastReadiness = Convert-Readiness -Text $observed.Stdout
        }
        catch {
            Write-RunRecord -Record @{ event = "invalid_readiness"; reason = $_.Exception.Message }
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "invalid_readiness_json_or_contract" -LastReadiness $null)
        }

        $expectedStatus = @{ 0 = "ready"; 2 = "waiting"; 3 = "blocked"; 4 = "terminal" }
        if (-not $expectedStatus.ContainsKey([int]$observed.ExitCode) -or [string]$lastReadiness.status -ne $expectedStatus[[int]$observed.ExitCode]) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "readiness_exit_status_mismatch" -LastReadiness $lastReadiness)
        }
        Write-RunRecord -Record @{
            event = "observation"
            observation_number = $actionsExecuted + 1
            readiness_exit_code = $observed.ExitCode
            status = [string]$lastReadiness.status
            reason = [string]$lastReadiness.reason
            action_command = if ($null -ne $lastReadiness.action) { [string]$lastReadiness.action.command } else { $null }
        }

        if ($observed.ExitCode -ne 0) {
            exit (Complete-Watchdog -ProposedExitCode $observed.ExitCode -Reason "readiness_$($lastReadiness.status)" -LastReadiness $lastReadiness)
        }
        if ($actionsExecuted -ge $MaxActions) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "max_actions_exhausted_while_ready" -LastReadiness $lastReadiness)
        }
        if ($null -eq $lastReadiness.action -or $null -eq $lastReadiness.action.argv) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "ready_without_action_argv" -LastReadiness $lastReadiness)
        }
        $actionArgv = @($lastReadiness.action.argv)
        if ($actionArgv.Count -eq 0 -or @($actionArgv | Where-Object { $_ -isnot [string] }).Count -ne 0) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "invalid_action_argv" -LastReadiness $lastReadiness)
        }

        Write-RunRecord -Record @{
            event = "action_dispatch"
            action_number = $actionsExecuted + 1
            action_argv = $actionArgv
        }
        $action = Invoke-PythonProcess -Role "action" -Argv @($prefix + $actionArgv)
        $actionsExecuted += 1
        if ($action.TimedOut) {
            exit (Complete-Watchdog -ProposedExitCode 3 -Reason "action_timeout" -LastReadiness $lastReadiness)
        }
        if ($action.ExitCode -eq 0) {
            continue
        }
        if ($action.ExitCode -in @(2, 3, 4)) {
            exit (Complete-Watchdog -ProposedExitCode $action.ExitCode -Reason "action_exit_$($action.ExitCode)" -LastReadiness $lastReadiness)
        }
        exit (Complete-Watchdog -ProposedExitCode 3 -Reason "unexpected_action_exit_code" -LastReadiness $lastReadiness)
    }
}
catch {
    $messageHash = Get-Sha256Text -Text $_.Exception.Message
    Write-RunRecord -Record @{ event = "controller_error"; message_sha256 = $messageHash }
    if ($null -ne $script:RunWriter) {
        Write-CreateOnlyAlert -ExitCode 3 -Reason "controller_error" -Readiness $null
    }
    [Console]::Error.WriteLine("prospective watchdog failed closed (error sha256: $messageHash)")
    exit 3
}
finally {
    if ($null -ne $script:RunWriter) { $script:RunWriter.Dispose() }
    if ($null -ne $script:RunStream) { $script:RunStream.Dispose() }
    if ($null -ne $lockStream) { $lockStream.Dispose() }
}
