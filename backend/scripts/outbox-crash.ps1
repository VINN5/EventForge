# outbox_crash with real processes.
#
# Publisher processes deliver events to a "broker" (the broker_log table), and
# on each event's first -CrashAttempts attempts they kill themselves right
# after delivering, before they can mark the event published. After the lease
# expires another attempt delivers the event again (a duplicate delivery) and
# marks it. When the outbox has drained, a consumer pass applies every
# delivery, then CheckOutbox judges the whole run.
#
# Default: the consumer deduplicates on event_id through RunOnce. Must PASS.
# -Plain (negative control): the consumer applies every delivery. The run
# PASSES only if strict verification catches the repeated applications, and a
# lenient verification shows that is the only damage.
#
# Publishers run with batch 1, so a crash only ever strands its own event.
#
# WARNING: wipes the outbox, broker_log, consumer_effects and job_effects
# tables of the DEV database.
param(
    [int]$Events = 40,
    [int]$Publishers = 4,
    [int]$CrashAttempts = 1,
    [int]$LeaseSeconds = 3,
    [int]$ReapSeconds = 1,
    [int]$TimeoutMinutes = 5,
    [switch]$Plain
)
$ErrorActionPreference = 'Stop'

if ($CrashAttempts -lt 1 -or $CrashAttempts -gt 8) { throw 'CrashAttempts must be 1 to 8 (events have max_attempts 10)' }
if ($LeaseSeconds -lt 2) { throw 'LeaseSeconds must be at least 2 (the delivery timeout is 1s and must be shorter)' }

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\env.ps1"

$mode = if ($Plain) { 'plain' } else { 'idempotent' }
$outDir = Join-Path $root "results\outbox_crash_$mode"
New-Item -ItemType Directory -Force $outDir | Out-Null
Get-ChildItem $outDir -File | Remove-Item -Force
$runLog = Join-Path $outDir 'run.log'
function Log([string]$msg) { Write-Host $msg; Add-Content -Path $runLog -Value $msg }

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\loadtest.exe .\cmd\loadtest
if ($LASTEXITCODE -ne 0) { throw 'build failed' }

$expectedCrashes = $Events * $CrashAttempts
Log ("outbox_crash ($mode): events=$Events publishers=$Publishers crash_attempts=$CrashAttempts lease=${LeaseSeconds}s reap=${ReapSeconds}s expected crashes=$expectedCrashes")

& .\bin\loadtest.exe outbox-seed -n $Events -reset 2>&1 | ForEach-Object { Log "$_" }
if ($LASTEXITCODE -ne 0) { throw 'seed failed' }

$incarnation = @{}
function Start-Publisher([int]$slot) {
    $incarnation[$slot] = 1 + [int]$incarnation[$slot]
    $id = "p$slot-$($incarnation[$slot])"
    $p = Start-Process -FilePath .\bin\loadtest.exe -NoNewWindow -PassThru `
        -ArgumentList @('outbox-work', '-id', $id, '-batch', 1,
                        '-lease', "${LeaseSeconds}s", '-deliver-timeout', '1s',
                        '-reap-interval', "${ReapSeconds}s",
                        '-crash-attempts', $CrashAttempts) `
        -RedirectStandardOutput (Join-Path $outDir "$id.out.log") `
        -RedirectStandardError (Join-Path $outDir "$id.err.log")
    return [pscustomobject]@{ Slot = $slot; Id = $id; Process = $p }
}

$bySlot = @{}
for ($s = 1; $s -le $Publishers; $s++) { $bySlot[$s] = Start-Publisher $s }

$sw = [Diagnostics.Stopwatch]::StartNew()
$crashExits = 0
$unexpectedExits = 0
$drained = $false
$lastStatus = -10.0

while ($sw.Elapsed.TotalMinutes -lt $TimeoutMinutes) {
    $now = $sw.Elapsed.TotalSeconds

    # Exit code 4 is the sink killing the process on purpose. Anything else
    # that ends a publisher is a bug.
    for ($s = 1; $s -le $Publishers; $s++) {
        $w = $bySlot[$s]
        if ($w.Process.HasExited) {
            $code = $w.Process.ExitCode
            if ($code -eq 4) {
                $crashExits++
            } else {
                $unexpectedExits++
                Log ("t={0:n1}s UNEXPECTED EXIT: {1} exited with code {2}" -f $now, $w.Id, $code)
            }
            $bySlot[$s] = Start-Publisher $s
        }
    }

    if ($now - $lastStatus -ge 2) {
        $lastStatus = $now
        $line = (& .\bin\loadtest.exe outbox-status 2>$null) | Select-Object -First 1
        if ($LASTEXITCODE -eq 0 -and $line -match 'unfinished=(\d+) published=(\d+) dead=(\d+)') {
            Log ("t={0:n1}s {1}; crash exits so far: {2}" -f $now, $line, $crashExits)
            if ([int]$Matches[1] -eq 0) { $drained = $true; break }
        }
    }
    Start-Sleep -Milliseconds 100
}

# Drained: nothing is in flight. Count any crash since the last poll, then stop.
for ($s = 1; $s -le $Publishers; $s++) {
    $w = $bySlot[$s]
    if ($w.Process.HasExited) {
        if ($w.Process.ExitCode -eq 4) { $crashExits++ } else { $unexpectedExits++ }
    } else {
        Stop-Process -Id $w.Process.Id -Force
    }
    $w.Process.WaitForExit()
}

& .\bin\loadtest.exe outbox-consume -mode $mode 2>&1 | ForEach-Object { Log "$_" }
if ($LASTEXITCODE -ne 0) { throw 'consume failed' }

function Run-Verify([string[]]$extraArgs) {
    $out = & .\bin\loadtest.exe outbox-verify -n $Events -min-extra $expectedCrashes @extraArgs 2>&1
    $code = $LASTEXITCODE
    $text = ($out | ForEach-Object { "$_" }) -join "`n"
    $text -split "`n" | ForEach-Object { Log $_ }
    return [pscustomobject]@{ Exit = $code; Text = $text }
}

Log ''
Log 'strict verification:'
$strict = Run-Verify @()
$lenient = $null
if ($Plain) {
    Log ''
    Log 'lenient verification (consumer repeats allowed):'
    $lenient = Run-Verify @('-allow-consumer-repeats')
}

$extra = if ($strict.Text -match 'deliveries:\s+\d+ total, (\d+) beyond the first') { [int]$Matches[1] } else { -1 }
$repeated = if ($strict.Text -match 'consumer:\s+\d+ rows; (\d+) events applied more than once') { [int]$Matches[1] } else { -1 }

$reasons = @()
if (-not $drained) { $reasons += "timed out after $TimeoutMinutes minutes with events still unfinished" }
if ($unexpectedExits -gt 0) { $reasons += "$unexpectedExits publisher(s) exited for a reason other than the planned crash" }
if ($crashExits -ne $expectedCrashes) { $reasons += "saw $crashExits crash exits, expected exactly $expectedCrashes" }
if ($extra -ne $expectedCrashes) { $reasons += "the database shows $extra duplicate deliveries, expected exactly $expectedCrashes" }

if ($Plain) {
    # Negative control: the consumer's repeats must be caught, and be the only damage.
    if ($strict.Exit -eq 0) { $reasons += 'strict verification PASSED, but a consumer without dedupe must show repeats; the harness cannot see them' }
    if ($strict.Text -notmatch 'applied by the consumer more than once') { $reasons += 'strict verification did not report repeated applications' }
    if ($repeated -ne $Events) { $reasons += "$repeated events were applied more than once, expected all $Events" }
    if ($lenient.Exit -ne 0) { $reasons += 'lenient verification failed: something other than the consumer repeats is wrong' }
} else {
    if ($strict.Exit -ne 0) { $reasons += 'verification failed' }
    if ($repeated -ne 0) { $reasons += "$repeated events were applied more than once, expected none" }
}

Log ''
Log ("crash exits observed: $crashExits (expected $expectedCrashes); duplicate deliveries: $extra (expected $expectedCrashes)")
if ($reasons.Count -eq 0) {
    if ($Plain) { Log "RESULT: PASS (negative control: all $Events events were applied more than once, and verification caught it)" }
    else { Log "RESULT: PASS ($Events events, $expectedCrashes crashes between send and mark, every event delivered, published once and applied once)" }
    exit 0
}
Log "RESULT: FAIL ($mode)"
$reasons | ForEach-Object { Log "  - $_" }
exit 1