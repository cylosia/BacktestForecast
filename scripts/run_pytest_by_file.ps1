param(
    [string]$Root = "tests",
    [string]$PytestArgs = "-q -p no:cacheprovider",
    [string]$LogDir = ".pytest-file-logs",
    [switch]$StopOnFailure,
    [switch]$ContinueOnError,
    [switch]$CollectAll,
    [string]$PathFilter = "",
    [string]$SummaryFile = ""
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

function Convert-ToLogName {
    param([string]$Path)
    $name = $Path -replace '[:\\\/]', '__'
    $name = $name -replace '[^A-Za-z0-9._-]', '_'
    return "$name.log"
}

function Parse-TestOutcome {
    param([string[]]$Lines, [int]$ExitCode)

    $summary = ($Lines | Select-String -Pattern '([0-9]+)\s+failed|([0-9]+)\s+passed|([0-9]+)\s+skipped|([0-9]+)\s+error' | Select-Object -Last 1).Line
    $failed = 0
    $passed = 0
    $skipped = 0
    $errors = 0

    if ($summary) {
        if ($summary -match '(\d+)\s+failed') { $failed = [int]$Matches[1] }
        if ($summary -match '(\d+)\s+passed') { $passed = [int]$Matches[1] }
        if ($summary -match '(\d+)\s+skipped') { $skipped = [int]$Matches[1] }
        if ($summary -match '(\d+)\s+error') { $errors = [int]$Matches[1] }
    }

    $status = if ($ExitCode -eq 0) { "passed" } elseif ($failed -gt 0 -or $errors -gt 0) { "failed" } else { "error" }

    return [pscustomobject]@{
        Status  = $status
        Passed  = $passed
        Failed  = $failed
        Skipped = $skipped
        Errors  = $errors
        Summary = $summary
    }
}

if (-not (Test-Path $Root)) {
    throw "Root path not found: $Root"
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$files = Get-ChildItem -Path $Root -Recurse -File -Filter "test_*.py" |
    Where-Object { -not $PathFilter -or $_.FullName -like "*$PathFilter*" } |
    Sort-Object FullName

if (-not $files) {
    throw "No test files found under '$Root'."
}

$results = New-Object System.Collections.Generic.List[object]

foreach ($file in $files) {
    $relativePath = Resolve-Path -Relative $file.FullName
    $logPath = Join-Path $LogDir (Convert-ToLogName $relativePath)
    $argList = @()
    if ($PytestArgs.Trim()) {
        $argList += ($PytestArgs -split '\s+' | Where-Object { $_ -ne "" })
    }
    $argList += $relativePath

    Write-Host "==> $relativePath"
    $stdoutPath = Join-Path $LogDir ((Convert-ToLogName "$relativePath.stdout") )
    $stderrPath = Join-Path $LogDir ((Convert-ToLogName "$relativePath.stderr") )
    $start = Get-Date
    $proc = Start-Process -FilePath "pytest" -ArgumentList $argList -NoNewWindow -Wait -PassThru `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $exitCode = $proc.ExitCode
    $duration = (Get-Date) - $start

    $stdoutLines = if (Test-Path $stdoutPath) { Get-Content $stdoutPath } else { @() }
    $stderrLines = if (Test-Path $stderrPath) { Get-Content $stderrPath } else { @() }
    $allOutput = @($stdoutLines + $stderrLines)
    $allOutput | Out-File -FilePath $logPath -Encoding utf8
    $outcome = Parse-TestOutcome -Lines $allOutput -ExitCode $exitCode

    $result = [pscustomobject]@{
        File       = $relativePath
        Status     = $outcome.Status
        ExitCode   = $exitCode
        Passed     = $outcome.Passed
        Failed     = $outcome.Failed
        Skipped    = $outcome.Skipped
        Errors     = $outcome.Errors
        DurationMs = [int][Math]::Round($duration.TotalMilliseconds)
        Log        = $logPath
        Summary    = $outcome.Summary
    }
    $results.Add($result) | Out-Null

    $label = "{0} exit={1} passed={2} failed={3} skipped={4} errors={5} log={6}" -f `
        $result.Status.ToUpperInvariant(), $result.ExitCode, $result.Passed, $result.Failed, $result.Skipped, $result.Errors, $result.Log
    Write-Host "    $label"

    $shouldStop = $result.ExitCode -ne 0 -and ($StopOnFailure -or (-not $CollectAll -and -not $ContinueOnError))
    if ($shouldStop) {
        break
    }
}

$summaryPath = if ($SummaryFile) { $SummaryFile } else { Join-Path $LogDir "summary.csv" }
$results | Export-Csv -NoTypeInformation -Path $summaryPath

$failedResults = $results | Where-Object { $_.ExitCode -ne 0 }
$passedCount = ($results | Where-Object { $_.ExitCode -eq 0 }).Count

Write-Host ""
Write-Host ("Completed {0} file(s): {1} passed, {2} failed. Summary: {3}" -f $results.Count, $passedCount, $failedResults.Count, $summaryPath)

if ($failedResults.Count -gt 0) {
    Write-Host "Failing files:"
    foreach ($row in $failedResults) {
        Write-Host (" - {0} ({1})" -f $row.File, $row.Log)
    }
    exit 1
}
