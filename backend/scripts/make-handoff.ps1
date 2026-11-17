# Bundles the project's source into one text file for a new chat.
# Run from anywhere:  .\scripts\make-handoff.ps1
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

$files = @()
$files += Get-ChildItem migrations -Filter *.sql -File | Sort-Object Name
$files += Get-ChildItem src -Recurse -Filter *.go -File | Sort-Object FullName
$files += Get-ChildItem cmd -Recurse -Filter *.go -File | Sort-Object FullName
$files += Get-ChildItem scripts -Filter *.ps1 -File | Sort-Object Name
$files += Get-Item docker-compose.yml, go.mod

$sb = [System.Text.StringBuilder]::new()
foreach ($f in $files) {
    $rel = '.' + $f.FullName.Substring($root.Length)
    [void]$sb.AppendLine("=== FILE: $rel ===")
    [void]$sb.AppendLine((Get-Content $f.FullName -Raw))
}

$out = Join-Path $root 'handoff-code.txt'
[System.IO.File]::WriteAllText($out, $sb.ToString(), [System.Text.UTF8Encoding]::new($false))
"{0} files, {1} lines -> {2}" -f $files.Count, ($sb.ToString() -split "`n").Count, $out
$files | ForEach-Object { '.' + $_.FullName.Substring($root.Length) }