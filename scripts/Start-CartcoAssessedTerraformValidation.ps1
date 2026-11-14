param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
# Compatibility launcher. Prefer scripts/windows/Start-CartcoAssessedTerraformValidation.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoAssessedTerraformValidation.ps1') @PSBoundParameters
