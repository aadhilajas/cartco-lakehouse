$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path (Split-Path -Parent $repoRoot) 'work\phase1-locked-venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { $python = (Get-Command python -ErrorAction Stop).Source }
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$docker = if ($dockerCommand) { $dockerCommand.Source } else { Join-Path $env:USERPROFILE 'AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe' }
$result = Join-Path $repoRoot ('test-results\assessed-recovery\' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
& $python -X utf8 (Join-Path $PSScriptRoot 'prove_assessed_recovery.py') --docker $docker --output $result
if ($LASTEXITCODE -ne 0) { throw "Assessed recovery proof failed; evidence retained at $result" }
