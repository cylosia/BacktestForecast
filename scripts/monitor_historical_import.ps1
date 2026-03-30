[CmdletBinding()]
param(
    [string]$StatusPath = "",
    [string]$LogsDir = "",
    [string]$StatusPattern = "historical_import_*.status.json",
    [int]$PollSeconds = 30,
    [int]$TailLines = 5,
    [string]$StartDate = "",
    [string]$EndDate = "",
    [switch]$Once,
    [switch]$StopWhenExited,
    [switch]$NoClear,
    [switch]$SkipDatabaseSummary
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not $RepoRoot) {
    throw "Unable to determine repository root from PSScriptRoot."
}

function Get-LatestStatusPath {
    param(
        [string]$DirectoryPath,
        [string]$Pattern
    )

    if (-not (Test-Path -LiteralPath $DirectoryPath)) {
        throw "Import logs directory not found: $DirectoryPath"
    }

    $latest = Get-ChildItem -LiteralPath $DirectoryPath -File -Filter $Pattern |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $latest) {
        throw "No import status files matching '$Pattern' were found under '$DirectoryPath'."
    }

    return $latest.FullName
}

function Get-CommandOptionValue {
    param(
        [string]$Command,
        [string]$OptionName
    )

    if (-not $Command) {
        return $null
    }

    $pattern = "(?:^|\s)$([regex]::Escape($OptionName))\s+([^\s]+)"
    $match = [regex]::Match($Command, $pattern)
    if ($match.Success) {
        return $match.Groups[1].Value
    }

    return $null
}

function Resolve-DateWindow {
    param(
        [string]$ConfiguredStartDate,
        [string]$ConfiguredEndDate,
        [pscustomobject]$Status
    )

    $resolvedStartDate = $ConfiguredStartDate
    $resolvedEndDate = $ConfiguredEndDate

    if (-not $resolvedStartDate) {
        $resolvedStartDate = Get-CommandOptionValue -Command $Status.command -OptionName "--start-date"
    }
    if (-not $resolvedEndDate) {
        $resolvedEndDate = Get-CommandOptionValue -Command $Status.command -OptionName "--end-date"
    }

    [pscustomobject]@{
        StartDate = $resolvedStartDate
        EndDate   = $resolvedEndDate
    }
}

function Invoke-DatabaseSummary {
    param(
        [string]$WindowStart,
        [string]$WindowEnd
    )

    if (-not $WindowStart -or -not $WindowEnd) {
        return [pscustomobject]@{
            Success = $false
            Message = "Skipped database summary because no start/end date window was configured."
            Data    = $null
        }
    }

    $pythonCode = @"
import json

from scripts._bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_session
from sqlalchemy import text

window_start = "$WindowStart"
window_end = "$WindowEnd"
queries = {
    "stock": """
        select min(trade_date) as min_date,
               max(trade_date) as max_date,
               count(distinct trade_date) as dates,
               count(*) as rows
        from historical_underlying_day_bars
        where trade_date between :window_start and :window_end
    """,
    "option": """
        select min(trade_date) as min_date,
               max(trade_date) as max_date,
               count(distinct trade_date) as dates,
               count(*) as rows
        from historical_option_day_bars
        where trade_date between :window_start and :window_end
    """,
}
summary = {}
with create_session() as session:
    for name, sql in queries.items():
        row = session.execute(
            text(sql),
            {"window_start": window_start, "window_end": window_end},
        ).mappings().one()
        summary[name] = {
            key: (value.isoformat() if hasattr(value, "isoformat") and value is not None else value)
            for key, value in row.items()
        }
print(json.dumps(summary))
"@

    $rawOutput = $pythonCode | python - 2>&1
    if ($LASTEXITCODE -ne 0) {
        return [pscustomobject]@{
            Success = $false
            Message = ($rawOutput | Out-String).Trim()
            Data    = $null
        }
    }

    try {
        $payload = ($rawOutput -join [Environment]::NewLine) | ConvertFrom-Json
    } catch {
        return [pscustomobject]@{
            Success = $false
            Message = "Database summary returned non-JSON output: $($rawOutput | Out-String)".Trim()
            Data    = $null
        }
    }

    return [pscustomobject]@{
        Success = $true
        Message = ""
        Data    = $payload
    }
}

function Write-SectionHeader {
    param([string]$Label)
    Write-Host ""
    Write-Host $Label
}

function Write-LogTail {
    param(
        [string]$Label,
        [string]$Path,
        [int]$Lines
    )

    Write-SectionHeader -Label $Label

    if (-not $Path) {
        Write-Host "No log path configured."
        return
    }
    if (-not (Test-Path -LiteralPath $Path)) {
        Write-Host "Missing log file: $Path"
        return
    }

    $item = Get-Item -LiteralPath $Path
    Write-Host ("Path: {0}" -f $item.FullName)
    Write-Host ("Bytes: {0}  Updated: {1}" -f $item.Length, $item.LastWriteTime.ToString("o"))

    $content = Get-Content -LiteralPath $Path -Tail $Lines -ErrorAction SilentlyContinue
    if (-not $content) {
        Write-Host "(empty)"
        return
    }

    $content | ForEach-Object { Write-Host $_ }
}

function Write-ImportStatusSummary {
    param(
        [pscustomobject]$Status,
        [pscustomobject]$Window,
        [string]$StatusPath
    )

    Write-Host ("Status file: {0}" -f $StatusPath)
    Write-Host ("Started: {0}" -f $Status.started_at)
    Write-Host ("Command: {0}" -f $Status.command)
    if ($Window.StartDate -and $Window.EndDate) {
        Write-Host ("Window: {0} -> {1}" -f $Window.StartDate, $Window.EndDate)
    } else {
        Write-Host "Window: unavailable"
    }
    if ($Status.status) {
        Write-Host ("Import status: {0}" -f $Status.status)
    }
    if ($null -ne $Status.completed_trade_dates -or $null -ne $Status.total_trade_dates -or $null -ne $Status.completed_pct) {
        $completedTradeDates = if ($null -ne $Status.completed_trade_dates) { $Status.completed_trade_dates } else { "?" }
        $totalTradeDates = if ($null -ne $Status.total_trade_dates) { $Status.total_trade_dates } else { "?" }
        $completedPct = if ($null -ne $Status.completed_pct) { $Status.completed_pct } else { "?" }
        Write-Host ("Progress: {0}/{1} trade dates ({2}%)" -f $completedTradeDates, $totalTradeDates, $completedPct)
    }
    if ($Status.last_completed_trade_date) {
        Write-Host ("Last completed trade date: {0}" -f $Status.last_completed_trade_date)
    }
    if ($null -ne $Status.completed_stock_rows -or $null -ne $Status.completed_option_rows) {
        $completedStockRows = if ($null -ne $Status.completed_stock_rows) { $Status.completed_stock_rows } else { "?" }
        $completedOptionRows = if ($null -ne $Status.completed_option_rows) { $Status.completed_option_rows } else { "?" }
        Write-Host ("Completed rows: stock={0} option={1}" -f $completedStockRows, $completedOptionRows)
    }
    if ($Status.updated_at) {
        Write-Host ("Last status update: {0}" -f $Status.updated_at)
    }
    if ($Status.error) {
        Write-Host ("Last error: {0}" -f $Status.error)
    }
}

function Read-ImportStatusFile {
    param([string]$Path)

    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    } catch {
        throw "Status file contains invalid JSON: $Path`n$($_.Exception.Message)"
    }
}

if (-not $LogsDir) {
    $LogsDir = Join-Path $RepoRoot "logs\imports"
}

if (-not $StatusPath) {
    $StatusPath = Get-LatestStatusPath -DirectoryPath $LogsDir -Pattern $StatusPattern
}

if (-not (Test-Path -LiteralPath $StatusPath)) {
    throw "Status file not found: $StatusPath"
}

Push-Location $RepoRoot
try {
    while ($true) {
        $status = Read-ImportStatusFile -Path $StatusPath
        $window = Resolve-DateWindow -ConfiguredStartDate $StartDate -ConfiguredEndDate $EndDate -Status $status
        $processId = if ($status.python_pid) { [int]$status.python_pid } else { $null }

        if (-not $NoClear) {
            Clear-Host
        }

        Write-Host ("Time: {0}" -f (Get-Date -Format o))
        Write-ImportStatusSummary -Status $status -Window $window -StatusPath $StatusPath

        Write-SectionHeader -Label "Process"
        if ($processId) {
            $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
        } else {
            $proc = $null
        }

        if ($proc) {
            Write-Host ("RUNNING  PID={0}  CPU={1}s  WS={2}MB  Started={3}" -f `
                $proc.Id,
                [math]::Round($proc.CPU, 2),
                [math]::Round($proc.WorkingSet64 / 1MB, 1),
                $proc.StartTime.ToString("o"))
        } else {
            if ($processId) {
                Write-Host ("NOT RUNNING  PID={0}" -f $processId)
            } else {
                Write-Host "NOT RUNNING  PID unavailable"
            }
        }

        Write-SectionHeader -Label "Database Summary"
        if ($SkipDatabaseSummary) {
            Write-Host "Skipped."
        } else {
            $summary = Invoke-DatabaseSummary -WindowStart $window.StartDate -WindowEnd $window.EndDate
            if (-not $summary.Success) {
                Write-Host $summary.Message
            } else {
                Write-Host ("Stock:  min={0}  max={1}  dates={2}  rows={3}" -f `
                    $summary.Data.stock.min_date,
                    $summary.Data.stock.max_date,
                    $summary.Data.stock.dates,
                    $summary.Data.stock.rows)
                Write-Host ("Option: min={0}  max={1}  dates={2}  rows={3}" -f `
                    $summary.Data.option.min_date,
                    $summary.Data.option.max_date,
                    $summary.Data.option.dates,
                    $summary.Data.option.rows)
            }
        }

        Write-LogTail -Label "Last stdout lines" -Path $status.stdout_log_path -Lines $TailLines
        Write-LogTail -Label "Last stderr lines" -Path $status.stderr_log_path -Lines $TailLines

        if ($Once) {
            break
        }
        if ($StopWhenExited -and -not $proc) {
            break
        }

        Start-Sleep -Seconds $PollSeconds
    }
} finally {
    Pop-Location
}
