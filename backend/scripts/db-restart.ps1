# db_restart, layer 2: restart the real Postgres container several times while
# real worker processes run, then check the outcome.
#
# The verdict. All of these must hold:
#   - every job succeeded, none dead, none lost (loadtest verify, expired-lease
#     reruns allowed and required to explain every repeated effect)
#   - while the database is healthy, no job stays running past its lease for
#     longer than one lease + one reaper interval (+ -StuckSlackSeconds)
#   - work resumes within -ResumeLimitSeconds of the database coming back
#   - no worker process exited by itself
#   - every planned restart actually happened
#
# -Mode stop : graceful. docker compose stop db, wait, docker compose up -d --wait db
# -Mode kill : crash. docker compose kill db, wait, docker compose up -d --wait db
#              (Postgres has to recover from an unclean stop)
#
# -NoReaper (negative control): the workers start with the reaper effectively
# off. The run PASSES only if the abandoned jobs are caught as stuck.
#
# WARNING: this wipes the jobs, job_attempts and effects tables of the DEV
# database and restarts its container (service "db" in docker-compose.yml).
#
# The restart schedule (the uptime before each restart and how long the
# database is kept down) is generated from -Seed and written to the results
# folder before anything starts. OS scheduling and Docker timing are not
# controlled, so a repeat is repeatable, not bit-for-bit identical.
param(
    [int]$Jobs = 60000,
    [int]$Workers = 4,
    [ValidateRange(1, 50)][int]$Restarts = 5,
    [ValidateSet('stop', 'kill')][string]$Mode = 'stop',
    [int]$Seed = 1,
    [double]$MinGapSeconds = 3.0,
    [double]$MaxGapSeconds = 8.0,
    [double]$MinDownSeconds = 0.0,
    [double]$MaxDownSeconds = 5.0,
    [int]$LeaseSeconds = 3,
    [int]$ReapSeconds = 1,
    [int]$StuckSlackSeconds = 2,
    [int]$ResumeLimitSeconds = 15,
    [int]$DelayMs = 5,
    [int]$Concurrency = 8,
    [int]$TimeoutMinutes = 10,
    [switch]$NoReaper
)
$ErrorActionPreference = 'Stop'

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\env.ps1"

$suffix = if ($NoReaper) { '_noreaper' } else { '' }
$outDir = Join-Path $root "results\db_restart_${Mode}_seed${Seed}${suffix}"
New-Item -ItemType Directory -Force $outDir | Out-Null
Get-ChildItem $outDir -File | Remove-Item -Force
$runLog = Join-Path $outDir 'run.log'
function Log([string]$msg) { Write-Host $msg; Add-Content -Path $runLog -Value $msg }

# docker writes progress to stderr; that must not abort the script.
function Invoke-Docker([string[]]$dockerArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & docker @dockerArgs 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $prev
    }
    $out | ForEach-Object { Log "    docker: $_" }
    if ($code -ne 0) { throw "docker $($dockerArgs -join ' ') failed with exit code $code" }
}

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\loadtest.exe .\cmd\loadtest
if ($LASTEXITCODE -ne 0) { throw 'build failed' }

$stuckBound = $LeaseSeconds + $ReapSeconds + $StuckSlackSeconds
$reapArg = if ($NoReaper) { 3600 } else { $ReapSeconds }
Log ("db_restart: mode=$Mode restarts=$Restarts jobs=$Jobs workers=$Workers seed=$Seed lease=${LeaseSeconds}s reap=${reapArg}s delay=${DelayMs}ms concurrency=$Concurrency stuck-bound=${stuckBound}s noreaper=$($NoReaper.IsPresent)")

# The whole schedule is decided up front, from the seed alone.
$rng = [System.Random]::new($Seed)
$plan = @()
for ($i = 1; $i -le $Restarts; $i++) {
    $gap = [math]::Round($MinGapSeconds + $rng.NextDouble() * ($MaxGapSeconds - $MinGapSeconds), 2)
    $down = [math]::Round($MinDownSeconds + $rng.NextDouble() * ($MaxDownSeconds - $MinDownSeconds), 2)
    $plan += [pscustomobject]@{ Restart = $i; UptimeBeforeSeconds = $gap; KeptDownSeconds = $down }
}
$plan | Format-Table -AutoSize | Out-String | Set-Content (Join-Path $outDir 'planned_schedule.txt')

# The database must be up and healthy before anything starts.
Invoke-Docker @('compose', 'up', '-d', '--wait', 'db')

& .\bin\loadtest.exe seed -n $Jobs -reset 2>&1 | ForEach-Object { Log "$_" }
if ($LASTEXITCODE -ne 0) { throw 'seed failed' }

$incarnation = @{}
function Start-Worker([int]$slot) {
    $incarnation[$slot] = 1 + [int]$incarnation[$slot]
    $id = "w$slot-$($incarnation[$slot])"
    $p = Start-Process -FilePath .\bin\loadtest.exe -NoNewWindow -PassThru `
        -ArgumentList @('work', '-id', $id, '-concurrency', $Concurrency,
                        '-lease', "${LeaseSeconds}s", '-reap-interval', "${reapArg}s",
                        '-delay', "${DelayMs}ms") `
        -RedirectStandardOutput (Join-Path $outDir "$id.out.log") `
        -RedirectStandardError (Join-Path $outDir "$id.err.log")
    return [pscustomobject]@{ Slot = $slot; Id = $id; Process = $p }
}

function Restart-Database($step) {
    $startAt = $sw.Elapsed.TotalSeconds
    if ($Mode -eq 'kill') { Invoke-Docker @('compose', 'kill', 'db') }
    else { Invoke-Docker @('compose', 'stop', 'db') }
    Start-Sleep -Milliseconds ([int]($step.KeptDownSeconds * 1000))
    Invoke-Docker @('compose', 'up', '-d', '--wait', 'db')
    return [pscustomobject]@{ StartAt = $startAt; UpAt = $sw.Elapsed.TotalSeconds }
}

$bySlot = @{}
for ($s = 1; $s -le $Workers; $s++) { $bySlot[$s] = Start-Worker $s }

$sw = [Diagnostics.Stopwatch]::StartNew()
$restartsDone = 0
$unexpectedExits = 0
$drained = $false
$stuck = $false
$lastStatus = -10.0
$nextRestartAt = $plan[0].UptimeBeforeSeconds
$healthySince = 0.0
$overdueSince = $null
$pendingResume = $false
$upAt = 0.0
$succAtUp = -1
$resumeTimes = @()
$resumeFailures = 0
$outages = @()

try {
    while ($sw.Elapsed.TotalMinutes -lt $TimeoutMinutes) {
        $now = $sw.Elapsed.TotalSeconds

        # In this run no worker should ever exit by itself: there are no poison jobs.
        for ($s = 1; $s -le $Workers; $s++) {
            $w = $bySlot[$s]
            if ($w.Process.HasExited) {
                $unexpectedExits++
                Log ("t={0:n1}s UNEXPECTED EXIT: {1} exited with code {2}; restarting it" -f $now, $w.Id, $w.Process.ExitCode)
                $bySlot[$s] = Start-Worker $s
            }
        }

        if ($restartsDone -lt $plan.Count -and $now -ge $nextRestartAt) {
            $step = $plan[$restartsDone]
            if ($pendingResume) {
                $resumeFailures++
                $pendingResume = $false
                Log '  the previous restart never showed progress before the next one began'
            }
            Log ("t={0:n1}s restart {1}/{2} ({3}): kept down for about {4}s" -f $now, ($restartsDone + 1), $plan.Count, $Mode, $step.KeptDownSeconds)
            $r = Restart-Database $step
            $restartsDone++
            $outage = $r.UpAt - $r.StartAt
            $outages += $outage
            $upAt = $r.UpAt
            $healthySince = $r.UpAt
            $overdueSince = $null
            $pendingResume = $true
            $succAtUp = -1
            Log ("t={0:n1}s database healthy again after {1:n1}s" -f $r.UpAt, $outage)
            if ($restartsDone -lt $plan.Count) { $nextRestartAt = $r.UpAt + $plan[$restartsDone].UptimeBeforeSeconds }
            continue
        }

        if ($now - $lastStatus -ge 1) {
            $lastStatus = $now
            $line = (& .\bin\loadtest.exe status 2>$null) | Select-Object -First 1
            if ($LASTEXITCODE -eq 0 -and $line -match 'queued=(\d+) running=(\d+) succeeded=(\d+) dead=(\d+) overdue=(\d+)') {
                $queued = [int]$Matches[1]; $running = [int]$Matches[2]
                $succeeded = [int]$Matches[3]; $overdue = [int]$Matches[5]
                Log ("t={0:n1}s {1}" -f $now, $line)

                if ($queued + $running -eq 0) {
                    if ($restartsDone -lt $plan.Count) { Log 'the queue drained before every planned restart happened' }
                    $drained = $true
                    break
                }

                # Did work resume after the last restart?
                if ($pendingResume) {
                    if ($succAtUp -lt 0) {
                        $succAtUp = $succeeded
                    } elseif ($succeeded -gt $succAtUp) {
                        $took = $now - $upAt
                        $resumeTimes += $took
                        $pendingResume = $false
                        Log ("  work resumed {0:n1}s after the database came back" -f $took)
                    } elseif ($now - $upAt -gt $ResumeLimitSeconds) {
                        $resumeFailures++
                        $pendingResume = $false
                        Log ("  NO PROGRESS for {0}s after the database came back" -f $ResumeLimitSeconds)
                    }
                }

                # Stuck: while the database has been healthy for longer than the bound,
                # running jobs must not stay past their lease for longer than the bound.
                if ($overdue -gt 0 -and ($now - $healthySince) -ge $stuckBound) {
                    if ($null -eq $overdueSince) {
                        $overdueSince = $now
                    } elseif (($now - $overdueSince) -ge $stuckBound) {
                        $stuck = $true
                        Log ("t={0:n1}s STUCK: {1} running job(s) have been past their lease for {2:n1}s with a healthy database (bound {3}s)" -f $now, $overdue, ($now - $overdueSince), $stuckBound)
                        break
                    }
                } else {
                    $overdueSince = $null
                }
            }
        }
        Start-Sleep -Milliseconds 100
    }
} finally {
    for ($s = 1; $s -le $Workers; $s++) {
        $w = $bySlot[$s]
        if ($null -ne $w -and -not $w.Process.HasExited) { Stop-Process -Id $w.Process.Id -Force }
        if ($null -ne $w) { $w.Process.WaitForExit() }
    }
}

# Make sure the database is up for verification, whatever happened above.
Invoke-Docker @('compose', 'up', '-d', '--wait', 'db')

$verifyOutput = & .\bin\loadtest.exe verify -n $Jobs -allow-reruns 2>&1
$verifyExit = $LASTEXITCODE
$verifyOutput | ForEach-Object { Log "$_" }

# What the workers said about the outages (for reading, not part of the verdict).
$errText = @(Get-ChildItem $outDir -Filter '*.err.log' | ForEach-Object { Get-Content $_.FullName })
function Count-Lines([string]$text) { @($errText | Select-String -SimpleMatch -CaseSensitive $text).Count }

Log ''
Log ("restarts done: $restartsDone of $($plan.Count)")
if ($outages.Count -gt 0) {
    Log ("outage lengths (s, from the stop or kill command until healthy): " + (($outages | ForEach-Object { '{0:n1}' -f $_ }) -join ', '))
}
if ($resumeTimes.Count -gt 0) {
    Log ("work resumed after (s): " + (($resumeTimes | ForEach-Object { '{0:n1}' -f $_ }) -join ', '))
}
Log ("worker log lines: 'outcome unknown' {0}, 'ack failed' {1}, 'recording failure failed' {2}, 'lease lost' {3}" -f `
    (Count-Lines 'outcome unknown'), (Count-Lines 'ack failed'), (Count-Lines 'recording failure failed'), (Count-Lines 'lease lost'))

if ($NoReaper) {
    # Negative control: the abandoned jobs must be caught.
    if ($stuck -and $verifyExit -ne 0) {
        Log "RESULT: PASS (negative control: with the reaper off, jobs stayed past their lease and both the stuck check and verification caught it; $restartsDone restarts, mode $Mode, seed $Seed)"
        exit 0
    }
    Log "RESULT: FAIL (negative control: the missing reaper was NOT caught; stuck=$stuck, verify exit=$verifyExit)"
    exit 1
}

$reasons = @()
if (-not $drained -and -not $stuck) { $reasons += "timed out after $TimeoutMinutes minutes with jobs still unfinished" }
if ($stuck) { $reasons += "jobs stayed past their lease for more than ${stuckBound}s while the database was healthy" }
if ($restartsDone -lt $plan.Count) { $reasons += "only $restartsDone of $($plan.Count) restarts happened (raise -Jobs or lower -Restarts)" }
if ($unexpectedExits -gt 0) { $reasons += "$unexpectedExits worker(s) exited by themselves" }
if ($resumeFailures -gt 0) { $reasons += "work did not resume within ${ResumeLimitSeconds}s after $resumeFailures restart(s)" }
if ($verifyExit -ne 0) { $reasons += 'verification failed' }

if ($reasons.Count -eq 0) {
    Log "RESULT: PASS ($restartsDone restarts, mode $Mode, seed $Seed)"
    exit 0
}
Log "RESULT: FAIL ($restartsDone restarts, mode $Mode, seed $Seed)"
$reasons | ForEach-Object { Log "  - $_" }
exit 1