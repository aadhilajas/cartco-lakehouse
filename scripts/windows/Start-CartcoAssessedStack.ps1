param([string]$PythonPath = '', [switch]$SkipBuild, [string]$TerraformPath = '', [string]$ResumeResultRoot = '', [string]$DockerPath = '')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'Tools.ps1')
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$python = Resolve-CartcoPython -ExplicitPath $PythonPath -RepoRoot $repoRoot
$docker = Resolve-CartcoDocker -ExplicitPath $DockerPath
$arguments = @('-X','utf8',(Join-Path $repoRoot 'scripts/acceptance/run_assessed_stack.py'),'--docker',$docker)
if ($SkipBuild) { $arguments += '--skip-build' }
if ($TerraformPath) { $arguments += @('--terraform',$TerraformPath) }
if ($ResumeResultRoot) { $arguments += @('--result-root',$ResumeResultRoot,'--resume-after-recreation') }
& $python @arguments
if ($LASTEXITCODE -ne 0) { throw 'Assessed stack acceptance failed; full logs are retained in test-results/assessed.' }
