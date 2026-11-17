# crash_after_effect with real processes.
#
# Every job's handler records its effect and then kills its own process (exit
# code 4) on the job's first -CrashAttempts attempts, so the effect is
# committed but the ack never arrives. The lease expires, the reaper requeues
# the job, and a later attempt finishes it. The strict verifier then checks that
# every job succeeded with exactly one effect.
#
# Default (idempotent): the effect goes through RunOnce. The run must PASS.
# -Plain (negative control): the effect is a plain insert. The run PASSES only
# if verification catches the duplicates, which shows the harness can see them.
#
# Workers run with concurrency 1, so a crash only ever strands its own job.
#
# WARNING: this wipes the jobs, job_attempts, effects and job_effects tables of
# the DEV database.
param(
    [int]$Jobs = 40,
    [int]$Workers = 4,
    [int]$CrashAttempts = 2,
    [int]$LeaseSeconds = 3,
    [int]$ReapSeconds = 1,
    [int]$TimeoutMinutes = 5,
    [switch]$Plain
)
$ErrorActionPreference = 'Stop'

if ($CrashAttempts -lt 1 -or $CrashAttempts -ge 5) { throw 'CrashAttempts must be 1 to 4 (crash jobs have max_attempts 5)' }

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\env.ps1"

$mode = if ($Plain) { 'plain' } else { 'idempotent' }
$crashType = if ($Plain) { 'crash_plain' } else { 'crash_after_effect' }
$outDir = Join-Path $root "results\crash_after_effect_$mode"
New-Item -ItemType Directory -Force $outDir | Out-Null
Get-ChildItem $outDir -File | Remove-Item -Force
$runLog = Join-Path $outDir 'run.log'
function Log([string]$msg) { Write-Host $msg; Add-Content -Path $runLog -Value $msg }

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\loadtest.exe .\cmd\loadtest
if ($LASTEXITCODE -ne 0) { throw 'build failed' }

$expectedCrashes = $Jobs * $CrashAttempts
Log ("crash_after_effect ($mode): jobs=$Jobs type=$crashType workers=$Workers crash_attempts=$CrashAttempts lease=${LeaseSeconds}s reap=${ReapSeconds}s expected crashes=$expectedCrashes")

& .\bin\loadtest.exe seed -n 0 -crash $Jobs -crash-type $crashType -reset 2>&1 | ForEach-Object { Log "$_" }
if ($LASTEXITCODE -ne 0) { throw 'seed failed' }

$incarnation = @{}
function Start-Worker([int]$slot) {
    $incarnation[$slot] = 1 + [int]$incarnation[$slot]
    $id = "w$slot-$($incarnation[$slot])"
    $p = Start-Process -FilePath .\bin\loadtest.exe -NoNewWindow -PassThru `
        -ArgumentList @('work', '-id', $id, '-concurrency', 1,
                        '-lease', "${LeaseSeconds}s", '-reap-interval', "${ReapSeconds}s",
                        '-crash-attempts', $CrashAttempts) `
        -RedirectStandardOutput (Join-Path $outDir "$id.out.log") `
        -RedirectStandardError (Join-Path $outDir "$id.err.log")
    return [pscustomobject]@{ Slot = $slot; Id = $id; Process = $p }
}

$bySlot = @{}
for ($s = 1; $s -le $Workers; $s++) { $bySlot[$s] = Start-Worker $s }

$sw = [Diagnostics.Stopwatch]::StartNew()
$crashExits = 0
$unexpectedExits = 0
$drained = $false
$lastStatus = -10.0

while ($sw.Elapsed.TotalMinutes -lt $TimeoutMinutes) {
    $now = $sw.Elapsed.TotalSeconds

    # Exit code 4 is the handler killing the process on purpose. Anything else
    # that ends a worker is a bug.
    for ($s = 1; $s -le $Workers; $s++) {
        $w = $bySlot[$s]
        if ($w.Process.HasExited) {
            $code = $w.Process.ExitCode
            if ($code -eq 4) {
                $crashExits++
            } else {
                $unexpectedExits++
                Log ("t={0:n1}s UNEXPECTED EXIT: {1} exited with code {2}" -f $now, $w.Id, $code)
            }
            $bySlot[$s] = Start-Worker $s
        }
    }

    if ($now - $lastStatus -ge 2) {
        $lastStatus = $now
        $line = (& .\bin\loadtest.exe status 2>$null) | Select-Object -First 1
        if ($LASTEXITCODE -eq 0 -and $line -match 'queued=(\d+) running=(\d+) succeeded=(\d+) dead=(\d+)') {
            Log ("t={0:n1}s {1}; crash exits so far: {2}" -f $now, $line, $crashExits)
            if ([int]$Matches[1] + [int]$Matches[2] -eq 0) { $drained = $true; break }
        }
    }
    Start-Sleep -Milliseconds 100
}

# The queue is drained, so nothing is in flight. Count any crash that happened
# between the last poll and now, then stop everything.
for ($s = 1; $s -le $Workers; $s++) {
    $w = $bySlot[$s]
    if ($w.Process.HasExited) {
        if ($w.Process.ExitCode -eq 4) { $crashExits++ } else { $unexpectedExits++ }
    } else {
        Stop-Process -Id $w.Process.Id -Force
    }
    $w.Process.WaitForExit()
}

# Strict verification: repeats are NOT allowed (no -allow-reruns).
$verifyOutput = & .\bin\loadtest.exe verify -n $Jobs 2>&1
$verifyExit = $LASTEXITCODE
$verifyText = ($verifyOutput | ForEach-Object { "$_" }) -join "`n"
$verifyText -split "`n" | ForEach-Object { Log $_ }

$leaseExpired = if ($verifyText -match 'attempts:\s+(\d+) ended by an expired lease') { [int]$Matches[1] } else { -1 }
$rerunJobs = if ($verifyText -match 'effects:\s+\d+ rows; (\d+) jobs ran their effect more than once') { [int]$Matches[1] } else { -1 }

$reasons = @()
if (-not $drained) { $reasons += "timed out after $TimeoutMinutes minutes with jobs still unfinished" }
if ($unexpectedExits -gt 0) { $reasons += "$unexpectedExits worker(s) exited for a reason other than the planned crash" }
if ($crashExits -ne $expectedCrashes) { $reasons += "saw $crashExits crash exits, expected exactly $expectedCrashes" }
if ($leaseExpired -ne $expectedCrashes) { $reasons += "the database shows $leaseExpired attempts ended by an expired lease, expected exactly $expectedCrashes" }

if ($Plain) {
    # Negative control: the duplicates must be caught.
    if ($verifyExit -eq 0) { $reasons += 'verification PASSED, but plain effects must produce duplicates; the harness cannot see them' }
    if ($verifyText -notmatch 'had their effect run more than once') { $reasons += 'verification did not report repeated effects' }
    if ($rerunJobs -ne $Jobs) { $reasons += "$rerunJobs jobs repeated their effect, expected all $Jobs" }
} else {
    if ($verifyExit -ne 0) { $reasons += 'verification failed' }
    if ($rerunJobs -ne 0) { $reasons += "$rerunJobs jobs repeated their effect, expected none" }
}

Log ''
Log ("crash exits observed: $crashExits (expected $expectedCrashes); attempts ended by an expired lease: $leaseExpired")
if ($reasons.Count -eq 0) {
    if ($Plain) { Log "RESULT: PASS (negative control: all $Jobs jobs repeated their effect, and verification caught it)" }
    else { Log "RESULT: PASS ($Jobs jobs, $expectedCrashes crashes after the effect, one effect each)" }
    exit 0
}
Log "RESULT: FAIL ($mode)"
$reasons | ForEach-Object { Log "  - $_" }
exit 1