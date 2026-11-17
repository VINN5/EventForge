# Runs the src/core tests and prints a summary you can trust.
#   .\scripts\test-core.ps1                          the whole suite
#   .\scripts\test-core.ps1 -Run 'TestPublisher' -Count 5
# Exit codes: 0 all passed, 1 a test failed, 2 database not reachable,
# 3 no tests ran (build failure or bad -Run pattern).
param(
    [string]$Run = '.',
    [int]$Count = 1
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\env.ps1"

$reachable = Test-NetConnection -ComputerName localhost -Port 5433 -InformationLevel Quiet -WarningAction SilentlyContinue
if (-not $reachable) {
    Write-Host 'The database is not reachable on port 5433. Start it with:  docker compose up -d --wait' -ForegroundColor Red
    exit 2
}

$goArgs = @('test', './src/core', '-run', $Run, "-count=$Count", '-v')
$lines = @(& go @goArgs 2>&1 | ForEach-Object { "$_" })
$goExit = $LASTEXITCODE
Write-Host ("captured {0} lines of go test output (go exit code {1})" -f $lines.Count, $goExit)

$passed  = @($lines | Where-Object { $_ -cmatch '^\s*--- PASS' }).Count
$failed  = @($lines | Where-Object { $_ -cmatch '^\s*--- FAIL' }).Count
$skipped = @($lines | Where-Object { $_ -cmatch '^\s*--- SKIP' }).Count

# Every failing, skipped or summary line, with the two lines after it.
for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -cmatch '^\s*--- (FAIL|SKIP)|^panic|^FAIL|^ok') {
        $last = [Math]::Min($i + 2, $lines.Count - 1)
        $lines[$i..$last] | ForEach-Object { Write-Host $_ }
    }
}
Write-Host ("passed={0} failed={1} skipped={2}" -f $passed, $failed, $skipped)

if (($passed + $failed + $skipped) -eq 0) {
    Write-Host 'No tests ran. The package probably failed to build, or -Run matched nothing. Compiler output:' -ForegroundColor Red
    $lines | Where-Object { $_ -cmatch '\.go:\d+|build failed|^#' } | ForEach-Object { Write-Host $_ }
    exit 3
}
if ($failed -gt 0 -or $goExit -ne 0) { exit 1 }
exit 0