param(
    [switch]$Release = $true
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot "rust\\backtest_kernels_pyo3\\Cargo.toml"
$packageDir = Join-Path $repoRoot "src\\backtestforecast"

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo was not found on PATH. Install the Rust toolchain first."
}

& python -m maturin --version | Out-Null

$args = @("-m", "maturin", "develop", "--manifest-path", $manifestPath, "--skip-install")
if ($Release) {
    $args += "--release"
}

& python @args
if ($LASTEXITCODE -ne 0) {
    throw "maturin develop failed with exit code $LASTEXITCODE"
}

$builtModule = Get-ChildItem -LiteralPath $packageDir -File |
    Where-Object { $_.Name -like "_backtest_kernels*.pyd" -or $_.Name -like "_backtest_kernels*.so" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if ($null -eq $builtModule) {
    throw "Expected PyO3 extension module was not found under $packageDir"
}

Write-Host "Built PyO3 backtest kernel extension at $($builtModule.FullName)"
