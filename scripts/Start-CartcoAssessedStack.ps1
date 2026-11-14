param([string]$PythonPath = '', [switch]$SkipBuild, [string]$TerraformPath = '', [string]$ResumeResultRoot = '', [string]$DockerPath = '')
# Compatibility launcher. Prefer scripts/windows/Start-CartcoAssessedStack.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoAssessedStack.ps1') @PSBoundParameters
