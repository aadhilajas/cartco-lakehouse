param([string]$PythonPath = '', [switch]$SkipBuild, [string]$TerraformPath = '')
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $PythonPath) {
    $workspacePython = Join-Path (Split-Path -Parent $repoRoot) 'work\phase1-locked-venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $workspacePython) { $PythonPath = $workspacePython }
    else { $PythonPath = (Get-Command python -ErrorAction Stop).Source }
}
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$docker = if ($dockerCommand) { $dockerCommand.Source } else { Join-Path $env:USERPROFILE 'AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe' }
if (-not (Test-Path -LiteralPath $docker)) { throw 'Docker CLI not found. Start Docker Desktop with the Linux engine.' }
$arguments = @('-X','utf8',(Join-Path $PSScriptRoot 'run_assessed_stack.py'),'--docker',$docker)
if ($SkipBuild) { $arguments += '--skip-build' }
if ($TerraformPath) { $arguments += @('--terraform',$TerraformPath) }
& $PythonPath @arguments
if ($LASTEXITCODE -ne 0) { throw 'Assessed stack acceptance failed; full logs are retained in test-results/assessed.' }
