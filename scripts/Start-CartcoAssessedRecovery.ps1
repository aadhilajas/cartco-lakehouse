param([string]$PythonPath = '', [string]$DockerPath = '')
# Compatibility launcher. Prefer scripts/windows/Start-CartcoAssessedRecovery.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoAssessedRecovery.ps1') @PSBoundParameters
