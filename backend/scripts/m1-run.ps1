. "$PSScriptRoot\env.ps1"

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\loadtest.exe .\cmd\loadtest
if ($LASTEXITCODE -ne 0) { throw "build failed" }

.\bin\loadtest.exe seed -n 100000 -reset
if ($LASTEXITCODE -ne 0) { throw "seed failed" }

$sw = [Diagnostics.Stopwatch]::StartNew()
$procs = 1..4 | ForEach-Object {
    Start-Process -FilePath .\bin\loadtest.exe `
        -ArgumentList "work", "-id", "w$_", "-concurrency", "8", "-until-drained" `
        -NoNewWindow -PassThru
}
$procs | Wait-Process
$sw.Stop()
"drained in {0:n1}s" -f $sw.Elapsed.TotalSeconds

.\bin\loadtest.exe verify -n 100000