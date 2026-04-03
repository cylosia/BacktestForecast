param(
    [switch]$Release = $true
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $repoRoot "rust\\backtest_kernels\\Cargo.toml"
$packageDir = Join-Path $repoRoot "src\\backtestforecast"

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo was not found on PATH. Install the Rust toolchain first."
}

$profile = if ($Release) { "release" } else { "debug" }
if ($Release) {
    cargo build --manifest-path $manifestPath --release
} else {
    cargo build --manifest-path $manifestPath
}

$isWindows = $env:OS -eq "Windows_NT"
$isMacOS = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::OSX
)

if ($isWindows) {
    $builtLibrary = Join-Path $repoRoot "rust\\backtest_kernels\\target\\$profile\\backtest_kernels_native.dll"
    $targetLibrary = Join-Path $packageDir "_backtest_kernels_native.dll"
} elseif ($isMacOS) {
    $builtLibrary = Join-Path $repoRoot "rust\\backtest_kernels\\target\\$profile\\libbacktest_kernels_native.dylib"
    $targetLibrary = Join-Path $packageDir "libbacktest_kernels_native.dylib"
} else {
    $builtLibrary = Join-Path $repoRoot "rust\\backtest_kernels\\target\\$profile\\libbacktest_kernels_native.so"
    $targetLibrary = Join-Path $packageDir "libbacktest_kernels_native.so"
}

if (-not (Test-Path $builtLibrary)) {
    throw "Expected built library was not found at $builtLibrary"
}

Copy-Item -LiteralPath $builtLibrary -Destination $targetLibrary -Force
Write-Host "Copied native kernel library to $targetLibrary"
