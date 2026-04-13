$ErrorActionPreference = "Stop"

Set-Location "C:\Users\Administrator\BacktestForecast"

$env:PYTHONUNBUFFERED = "1"

$jobs = @(
    @{
        Label = "2018"
        Start = "2018-01-01"
        End = "2018-12-31"
        Status = "logs/imports/historical_import_201801_201812.status.json"
    },
    @{
        Label = "2019"
        Start = "2019-01-01"
        End = "2019-12-31"
        Status = "logs/imports/historical_import_201901_201912.status.json"
    },
    @{
        Label = "2020"
        Start = "2020-01-01"
        End = "2020-12-31"
        Status = "logs/imports/historical_import_202001_202012.status.json"
    },
    @{
        Label = "2021"
        Start = "2021-01-01"
        End = "2021-12-31"
        Status = "logs/imports/historical_import_202101_202112.status.json"
    },
    @{
        Label = "2023h2"
        Start = "2023-06-01"
        End = "2023-12-31"
        Status = "logs/imports/historical_import_202306_202312.status.json"
    },
    @{
        Label = "2024"
        Start = "2024-01-01"
        End = "2024-12-31"
        Status = "logs/imports/historical_import_202401_202412.status.json"
    },
    @{
        Label = "2025"
        Start = "2025-01-01"
        End = "2025-12-31"
        Status = "logs/imports/historical_import_202501_202512.status.json"
    },
    @{
        Label = "2026ytd"
        Start = "2026-01-01"
        End = "2026-04-01"
        Status = "logs/imports/historical_import_202601_20260401.status.json"
    }
)

foreach ($job in $jobs) {
    Write-Output ("START {0} {1}..{2}" -f $job.Label, $job.Start, $job.End)
    & .\.venv\Scripts\python.exe scripts\sync_historical_market_data.py `
        --start-date $job.Start `
        --end-date $job.End `
        --batch-size 3000 `
        --workers 1 `
        --skip-rest-enrichment `
        --status-path $job.Status `
        --resume
    if ($LASTEXITCODE -ne 0) {
        throw ("FAILED {0} exit={1}" -f $job.Label, $LASTEXITCODE)
    }
    Write-Output ("DONE {0}" -f $job.Label)
}
