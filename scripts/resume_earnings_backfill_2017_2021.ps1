$ErrorActionPreference = "Stop"

Set-Location "C:\Users\Administrator\BacktestForecast"

$jobs = @(
    @{
        Label = "2017"
        Start = "2017-01-01"
        End = "2017-12-31"
        Symbols = "logs/earnings-symbols-2017.txt"
        Status = "logs/earnings-backfill-2017-status.json"
        StatusWriteEvery = "50"
    },
    @{
        Label = "2018_2021"
        Start = "2018-01-01"
        End = "2021-12-31"
        Symbols = "logs/earnings-symbols-2023.txt"
        Status = "logs/earnings-backfill-2018_2021-status.json"
        StatusWriteEvery = "100"
    }
)

foreach ($job in $jobs) {
    Write-Output ("START {0} {1}..{2}" -f $job.Label, $job.Start, $job.End)
    & .\.venv\Scripts\python.exe scripts\backfill_earnings_events.py `
        --start-date $job.Start `
        --end-date $job.End `
        --symbols-file $job.Symbols `
        --status-write-every $job.StatusWriteEvery `
        --status-path $job.Status `
        --resume
    if ($LASTEXITCODE -ne 0) {
        throw ("FAILED {0} exit={1}" -f $job.Label, $LASTEXITCODE)
    }
    Write-Output ("DONE {0}" -f $job.Label)
}
