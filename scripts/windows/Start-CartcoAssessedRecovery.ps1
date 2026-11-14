param([string]$PythonPath = '', [string]$DockerPath = '')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'Tools.ps1')
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Resolve-CartcoPython -ExplicitPath $PythonPath -RepoRoot $repoRoot
$docker = Resolve-CartcoDocker -ExplicitPath $DockerPath
$result = Join-Path $repoRoot ('test-results/assessed-recovery/' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
& $python -X utf8 (Join-Path $repoRoot 'scripts/recovery/prove_assessed_recovery.py') --docker $docker --output $result
if ($LASTEXITCODE -ne 0) { throw "Assessed recovery proof failed; evidence retained at $result" }
