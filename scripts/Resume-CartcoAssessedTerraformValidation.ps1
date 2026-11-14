$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$terraform = Join-Path (Split-Path -Parent $repoRoot) 'work\infra-phase1\terraform\terraform.exe'
if (-not (Test-Path -LiteralPath $terraform)) { $terraform = (Get-Command terraform -ErrorAction Stop).Source }
$result = Get-ChildItem -LiteralPath (Join-Path $repoRoot 'test-results\assessed') -Directory |
    Where-Object { (Test-Path -LiteralPath (Join-Path $_.FullName 'terraform-apply.log')) -and
                   (Test-Path -LiteralPath (Join-Path $_.FullName 'catalog-after-recreation.log')) -and
                   (-not (Test-Path -LiteralPath (Join-Path $_.FullName 'SUCCESS.txt'))) } |
    Sort-Object Name -Descending | Select-Object -First 1
if (-not $result) { throw 'No unfinished Terraform run with completed catalog recreation was found.' }
& (Join-Path $PSScriptRoot 'Start-CartcoAssessedStack.ps1') -TerraformPath $terraform -ResumeResultRoot $result.FullName
