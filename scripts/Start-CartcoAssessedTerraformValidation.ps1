$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$terraform = Join-Path (Split-Path -Parent $repoRoot) 'work\infra-phase1\terraform\terraform.exe'
if (-not (Test-Path -LiteralPath $terraform)) { $terraform = (Get-Command terraform -ErrorAction Stop).Source }
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedStack.ps1') -TerraformPath $terraform
