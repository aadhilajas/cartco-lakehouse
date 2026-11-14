param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
# Compatibility launcher. Prefer scripts/windows/Resume-CartcoAssessedTerraformValidation.ps1.
& (Join-Path $PSScriptRoot 'windows/Resume-CartcoAssessedTerraformValidation.ps1') @PSBoundParameters
