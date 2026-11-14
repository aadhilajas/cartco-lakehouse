param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
# Compatibility launcher. Prefer scripts/windows/Start-CartcoTerraformValidation.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoTerraformValidation.ps1') @PSBoundParameters
