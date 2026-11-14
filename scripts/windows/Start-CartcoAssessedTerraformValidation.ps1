param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'Tools.ps1')
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$terraform = Resolve-CartcoTerraform -ExplicitPath $TerraformPath -RepoRoot $repoRoot
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedStack.ps1') -TerraformPath $terraform -PythonPath $PythonPath -DockerPath $DockerPath
