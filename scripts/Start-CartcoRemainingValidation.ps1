param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
# Run after assessed Compose acceptance. Stop on any failure.
# Compatibility launcher. Prefer scripts/windows/Start-CartcoRemainingValidation.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoRemainingValidation.ps1') @PSBoundParameters
