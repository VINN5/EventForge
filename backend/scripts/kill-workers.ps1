# kill_workers at small scale: hard-kill worker processes at random moments while
# jobs run, then check that nothing was lost, stuck or duplicated.
#
# With -Poison N it also seeds N jobs whose handler crashes the worker process
# every time. Each must end up dead after exactly 3 crashes, while every healthy
# job still succeeds.
#
# WARNING: this wipes the jobs, job_attempts and effects tables of the DEV database.
#
# The kill schedule (the gaps and which worker dies) is generated from -Seed and
# written to the results folder before anything starts, so a run can be repeated
# with the same injected faults. OS scheduling is not controlled, so a repeat is
# repeatable, not bit-for-bit identical.
param(
    [int]$Jobs = 30000,
    [int]$Workers = 4,
    [int]$Kills = 15,
    [int]$Poison = 0,
    [int]$Seed = 1,
    [double]$MinGapSeconds = 1.0,
    [double]$MaxGapSeconds = 4.0,
    [int]$LeaseSeconds = 3,
    [int]$ReapSeconds = 1,
    [int]$DelayMs = 5,
    [int]$Concurrency = 8,
    [int]$TimeoutMinutes = 10
)
$ErrorActionPreference = 'Stop'

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\env.ps1"

$outDir = Join-Path $root "results\kill_workers_seed$Seed"
New-Item -ItemType Directory -Force $outDir | Out-Null
Get-ChildItem $outDir -File | Remove-Item -Force
$runLog = Join-Path $outDir 'run.log'
function Log([string]$msg) { Write-Host $msg; Add-Content -Path $runLog -Value $msg }

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\loadtest.exe .\cmd\loadtest
if ($LASTEXITCODE -ne 0) { throw 'build failed' }

Log ("kill_workers: jobs=$Jobs poison=$Poison workers=$Workers kills=$Kills seed=$Seed lease=${LeaseSeconds}s reap=${ReapSeconds}s delay=${DelayMs}ms concurrency=$Concurrency")

# The whole schedule is decided up front, from the seed alone.
$rng = [System.Random]::new($Seed)
$plan = @()
for ($i = 1; $i -le $Kills; $i++) {
    $gap = [math]::Round($MinGapSeconds + $rng.NextDouble() * ($MaxGapSeconds - $MinGapSeconds), 2)
    $plan += [pscustomobject]@{ Kill = $i; GapSeconds = $gap; Slot = 1 + $rng.Next($Workers) }
}
if ($plan.Count -gt 0) {
    $plan | Format-Table -AutoSize | Out-String | Set-Content (Join-Path $outDir 'planned_schedule.txt')
}

& .\bin\loadtest.exe seed -n $Jobs -poison $Poison -reset 2>&1 | ForEach-Object { Log "$_" }
if ($LASTEXITCODE -ne 0) { throw 'seed failed' }
$totalJobs = $Jobs + $Poison

$incarnation = @{}
function Start-Worker([int]$slot) {
    $incarnation[$slot] = 1 + [int]$incarnation[$slot]
    $id = "w$slot-$($incarnation[$slot])"
    $p = Start-Process -FilePath .\bin\loadtest.exe -NoNewWindow -PassThru `
        -ArgumentList @('work', '-id', $id, '-concurrency', $Concurrency,
                        '-lease', "${LeaseSeconds}s", '-reap-interval', "${ReapSeconds}s",
                        '-delay', "${DelayMs}ms") `
        -RedirectStandardOutput (Join-Path $outDir "$id.out.log") `
        -RedirectStandardError (Join-Path $outDir "$id.err.log")
    return [pscustomobject]@{ Slot = $slot; Id = $id; Process = $p }
}

$bySlot = @{}
for ($s = 1; $s -le $Workers; $s++) { $bySlot[$s] = Start-Worker $s }

$sw = [Diagnostics.Stopwatch]::StartNew()
$killsDone = 0
$unexpectedExits = 0
$poisonCrashes = 0
$drained = $false
$lastStatus = -10.0
$nextKillAt = if ($plan.Count -gt 0) { $plan[0].GapSeconds } else { [double]::MaxValue }

while ($sw.Elapsed.TotalMinutes -lt $TimeoutMinutes) {
    $now = $sw.Elapsed.TotalSeconds

    # A worker that exits by itself is either a poison crash (exit code 3, only
    # expected when poison jobs exist) or a bug.
    for ($s = 1; $s -le $Workers; $s++) {
        $w = $bySlot[$s]
        if ($w.Process.HasExited) {
            $code = $w.Process.ExitCode
            if ($Poison -gt 0 -and $code -eq 3) {
                $poisonCrashes++
                Log ("t={0:n1}s poison crash {1}: {2} exited with code 3; restarting" -f $now, $poisonCrashes, $w.Id)
            } else {
                $unexpectedExits++
                Log ("t={0:n1}s UNEXPECTED EXIT: {1} exited with code {2}; restarting" -f $now, $w.Id, $code)
            }
            $bySlot[$s] = Start-Worker $s
        }
    }

    if ($killsDone -lt $plan.Count -and $now -ge $nextKillAt) {
        $step = $plan[$killsDone]
        $victim = $bySlot[$step.Slot]
        Stop-Process -Id $victim.Process.Id -Force
        $victim.Process.WaitForExit()
        $bySlot[$step.Slot] = Start-Worker $step.Slot
        $killsDone++
        Log ("t={0:n1}s kill {1}/{2}: {3} (pid {4}) -> started {5}" -f `
            $now, $killsDone, $plan.Count, $victim.Id, $victim.Process.Id, $bySlot[$step.Slot].Id)
        if ($killsDone -lt $plan.Count) { $nextKillAt = $now + $plan[$killsDone].GapSeconds }
    }

    if ($now - $lastStatus -ge 2) {
        $lastStatus = $now
        $line = (& .\bin\loadtest.exe status 2>$null) | Select-Object -First 1
        if ($LASTEXITCODE -eq 0 -and $line -match 'queued=(\d+) running=(\d+) succeeded=(\d+) dead=(\d+)') {
            Log ("t={0:n1}s {1}" -f $now, $line)
            if ([int]$Matches[1] + [int]$Matches[2] -eq 0) {
                if ($killsDone -lt $plan.Count) {
                    Log 'the queue drained before every planned kill happened'
                }
                $drained = $true
                break
            }
        }
    }
    Start-Sleep -Milliseconds 100
}

# Stop everything. The queue is drained, so nothing is in flight.
for ($s = 1; $s -le $Workers; $s++) {
    $w = $bySlot[$s]
    if (-not $w.Process.HasExited) { Stop-Process -Id $w.Process.Id -Force }
    $w.Process.WaitForExit()
}

$verifyArgs = @('verify', '-n', $totalJobs, '-allow-reruns')
if ($Poison -gt 0) { $verifyArgs += @('-expected-dead', $Poison, '-dead-by-crash-only') }
$verifyOutput = & .\bin\loadtest.exe @verifyArgs 2>&1
$verifyExit = $LASTEXITCODE
$verifyOutput | ForEach-Object { Log "$_" }

$reasons = @()
if (-not $drained) { $reasons += "timed out after $TimeoutMinutes minutes with jobs still unfinished" }
if ($killsDone -lt $Kills) { $reasons += "only $killsDone of $Kills kills happened (raise -Jobs or lower -Kills)" }
if ($unexpectedExits -gt 0) { $reasons += "$unexpectedExits worker(s) exited by themselves" }
if ($Poison -gt 0 -and $poisonCrashes -lt 3) {
    $reasons += "only $poisonCrashes worker crash(es) came from poison jobs; every poison job needs 3 attempts, so at least 3 were expected"
}
if ($verifyExit -ne 0) { $reasons += 'verification failed' }

Log ''
Log ("poison crashes observed: $poisonCrashes; scheduled kills done: $killsDone")
if ($reasons.Count -eq 0) {
    Log "RESULT: PASS ($killsDone kills, $Poison poison jobs, seed $Seed)"
    exit 0
}
Log "RESULT: FAIL ($killsDone kills, $Poison poison jobs, seed $Seed)"
$reasons | ForEach-Object { Log "  - $_" }
exit 1