param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
# Run after assessed Compose acceptance. Stop on any failure.
$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedRecovery.ps1') -PythonPath $PythonPath -DockerPath $DockerPath
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedTerraformValidation.ps1') -TerraformPath $TerraformPath -PythonPath $PythonPath -DockerPath $DockerPath
Write-Host 'Remaining local recovery and Terraform acceptance passed.'
