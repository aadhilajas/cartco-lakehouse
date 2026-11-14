param(
    [string]$ResultRoot = '',
    [switch]$SkipBuild,
    [switch]$RepairMarquez,
    [switch]$CollectOnly,
    [string]$DockerPath = ''
)
# Compatibility launcher. Prefer scripts/windows/Start-CartcoInfrastructure.ps1.
& (Join-Path $PSScriptRoot 'windows/Start-CartcoInfrastructure.ps1') @PSBoundParameters
