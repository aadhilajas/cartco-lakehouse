param([string]$TerraformPath = '', [string]$PythonPath = '', [string]$DockerPath = '')
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $PSScriptRoot 'Tools.ps1')
$TerraformPath = Resolve-CartcoTerraform -ExplicitPath $TerraformPath -RepoRoot $repoRoot
$PythonPath = Resolve-CartcoPython -ExplicitPath $PythonPath -RepoRoot $repoRoot
$docker = Resolve-CartcoDocker -ExplicitPath $DockerPath
$resultRoot = Join-Path $repoRoot ('test-results\terraform-live\' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Force -Path $resultRoot | Out-Null
$oldPath = $env:PATH
$env:PATH = (Split-Path -Parent $docker) + [IO.Path]::PathSeparator + $env:PATH
$tfRoot = Join-Path $repoRoot 'infra\terraform'
$tfArgs = @('-var=docker_host=npipe:////./pipe/dockerDesktopLinuxEngine', '-var=buildx_builder=default')
$planPath = Join-Path $resultRoot 'foundation.tfplan'

function Invoke-Logged([string]$Exe, [string[]]$Arguments, [string]$LogName) {
    Write-Host ('Running: ' + $LogName)
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & $Exe @Arguments 2>&1 | ForEach-Object {
            $line = if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { $_.ToString() }
            if (-not [string]::IsNullOrWhiteSpace($line)) { $line }
        } | Tee-Object -FilePath (Join-Path $resultRoot $LogName) | ForEach-Object {
            if ($LogName -ne 'plan.log') { Write-Host $_ }
        }
        $code = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $oldPreference }
    if ($LogName -eq 'plan.log') { Get-Content -LiteralPath (Join-Path $resultRoot $LogName) -Tail 15 | ForEach-Object { Write-Host $_ } }
    if ($code -ne 0) { throw "Stage failed ($code): $LogName. Evidence: $resultRoot" }
}

try {
    Invoke-Logged $docker @('version') 'docker-version.log'
    Invoke-Logged $TerraformPath @('-version') 'terraform-version.log'
    Invoke-Logged $TerraformPath @("-chdir=$tfRoot", 'init', '-backend=false', '-input=false', '-no-color') 'init.log'
    Invoke-Logged $TerraformPath @("-chdir=$tfRoot", 'validate', '-no-color') 'validate.log'
    Invoke-Logged $TerraformPath @("-chdir=$tfRoot", 'test', '-no-color') 'mock-plans.log'
    Invoke-Logged $TerraformPath (@("-chdir=$tfRoot", 'plan', '-input=false', '-no-color', "-out=$planPath") + $tfArgs) 'plan.log'
    # The real plan is retained before any running service is stopped. Only the
    # new Compose project is stopped; all volumes and baseline containers remain.
    Invoke-Logged $docker @('compose', '-p', 'cartco-infra-p1', '-f', (Join-Path $repoRoot 'infra\compatibility\stack.json'), 'stop') 'stop-new-compose.log'
    Invoke-Logged $TerraformPath @("-chdir=$tfRoot", 'apply', '-parallelism=1', '-input=false', '-no-color', $planPath) 'initial-apply.log'
    Invoke-Logged $PythonPath @('-X', 'utf8', (Join-Path $repoRoot 'scripts\acceptance\phase1_terraform_acceptance.py'),
        '--docker', $docker, '--terraform', $TerraformPath, '--docker-host', 'npipe:////./pipe/dockerDesktopLinuxEngine',
        '--buildx-builder', 'default', '--evidence', $resultRoot) 'live-acceptance.log'
    Write-Host "Terraform acceptance finished: $resultRoot"
}
catch {
    $_ | Out-String | Set-Content -LiteralPath (Join-Path $resultRoot 'error.log')
    throw
}
finally { $env:PATH = $oldPath }
