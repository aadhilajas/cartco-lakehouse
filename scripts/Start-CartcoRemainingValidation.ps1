# Run after the assessed Compose acceptance has passed. Stop on any failure.
$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedRecovery.ps1')
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedTerraformValidation.ps1')
Write-Host 'Remaining local recovery and Terraform acceptance passed.'
