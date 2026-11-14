param(
    [string]$ResultRoot = '',
    [switch]$SkipBuild,
    [switch]$RepairMarquez,
    [switch]$CollectOnly
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
if ($CollectOnly -and -not $ResultRoot) { throw 'CollectOnly requires the original run ResultRoot.' }
if ($CollectOnly) { $SkipBuild = $true }
if (-not $ResultRoot) {
    $ResultRoot = Join-Path $repoRoot ('test-results\infrastructure\' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
}
$ResultRoot = [IO.Path]::GetFullPath($ResultRoot)
New-Item -ItemType Directory -Force -Path $ResultRoot | Out-Null
$docker = (Get-Command docker -ErrorAction SilentlyContinue).Source
if (-not $docker) {
    foreach ($candidate in @(
        (Join-Path $env:USERPROFILE 'AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe'),
        'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
    )) { if (Test-Path -LiteralPath $candidate) { $docker = $candidate; break } }
}
if (-not $docker) { throw 'Docker CLI not found.' }
$oldPath = $env:PATH
$env:PATH = (Split-Path -Parent $docker) + [IO.Path]::PathSeparator + $env:PATH
$compose = @('compose', '-p', 'cartco-infra-p1', '-f', (Join-Path $repoRoot 'infra\phase1\stack.json'))
$statsJob = $null

function Invoke-Docker([string]$LogName, [string[]]$DockerArgs) {
    Write-Host ('Running: ' + $LogName)
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & $docker @DockerArgs 2>&1 | ForEach-Object {
            $line = if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { $_.ToString() }
            if (-not [string]::IsNullOrWhiteSpace($line)) { $line }
        } | Tee-Object -FilePath (Join-Path $ResultRoot $LogName)
        $code = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $oldPreference }
    if ($code -ne 0) { throw "Docker failed ($code): $LogName. Evidence: $ResultRoot" }
}

function Wait-Endpoint([string]$Url, [string]$Name) {
    $deadline = (Get-Date).AddMinutes(5)
    do {
        try {
            $result = Invoke-RestMethod -Uri $Url -TimeoutSec 5
            if ($Name -eq 'airflow') {
                if ($result.metadatabase.status -ne 'healthy' -or $result.scheduler.status -ne 'healthy') { throw 'Airflow not healthy yet' }
            }
            $result | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $ResultRoot ($Name + '-health.json')) -Encoding UTF8
            return
        }
        catch { Start-Sleep -Seconds 3 }
    } while ((Get-Date) -lt $deadline)
    throw "Readiness failed: $Url"
}

function Baseline-Identity {
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $lines = & $docker ps -a --format '{{json .}}' 2>&1
        if ($LASTEXITCODE -ne 0) { throw 'Cannot inventory baseline containers' }
        $identities = @($lines | ForEach-Object { $_ | ConvertFrom-Json } |
            Where-Object { $_.Names -like 'cartco-*' -and $_.Names -notlike 'cartco-infra-p1-*' } |
            ForEach-Object { $_.Names + '=' + $_.ID } | Sort-Object)
        return ($identities -join "`n")
    }
    finally { $ErrorActionPreference = $oldPreference }
}

function Read-VolumeJson([string]$FileName) {
    Write-Host ('Collecting: ' + $FileName)
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        # Compose progress is stderr. Keep it separate from the artifact's stdout.
        $output = & $docker @compose run --rm --no-deps --entrypoint cat spark-smoke ('/evidence/' + $FileName) 2> (Join-Path $ResultRoot ($FileName + '.stderr.log'))
        $code = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $oldPreference }
    if ($code -ne 0) { throw "Artifact copy failed: $FileName" }
    $payload = $output -join "`n"
    $parsed = ConvertFrom-Json -InputObject $payload
    # Retain literal JSON integer digits; avoid a serializer round trip.
    [IO.File]::WriteAllText((Join-Path $ResultRoot $FileName), $payload, [Text.UTF8Encoding]::new($false))
    return $parsed
}

try {
    $driveInfo = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($repoRoot))
    $requiredFree = if ($SkipBuild) { 2GB } else { 10GB }
    if ($driveInfo.AvailableFreeSpace -lt $requiredFree) { throw 'Insufficient disk space: full builds need 10 GB; cached acceptance needs 2 GB. Keep existing CartCo data and Docker volumes.' }
    Invoke-Docker 'version.log' @('version')
    if ($CollectOnly) {
        $before = (Get-Content -LiteralPath (Join-Path $ResultRoot 'baseline-before.txt') -Raw).TrimEnd([char[]]"`r`n")
        $catalogBefore = @(Get-Content -LiteralPath (Join-Path $ResultRoot 'catalog-before.log') | Where-Object { $_ -match '^[0-9a-f]{64}$' })
        $catalogAfter = @(Get-Content -LiteralPath (Join-Path $ResultRoot 'catalog-after.txt') | Where-Object { $_ -match '^[0-9a-f]{64}$' })
        if ($catalogBefore.Count -ne 3 -or $catalogAfter.Count -ne 3 -or @($catalogAfter | Where-Object { $catalogBefore -contains $_ }).Count -ne 0) {
            throw 'Original run has no valid three-container recreation proof.'
        }
    }
    else {
        $before = Baseline-Identity
        $before | Set-Content -LiteralPath (Join-Path $ResultRoot 'baseline-before.txt')
    }
    Invoke-Docker 'compose-config.log' ($compose + @('config', '--quiet'))
    if ($RepairMarquez -and -not $CollectOnly) {
        Invoke-Docker 'marquez-config-build.log' ($compose + @('build', 'marquez-api'))
    }
    if (-not $SkipBuild) {
        Invoke-Docker 'airflow-base-build.log' @('build', '-t', 'cartco/airflow:infra-p1-base', '-f', (Join-Path $repoRoot 'Dockerfile.airflow'), $repoRoot)
        Invoke-Docker 'build.log' ($compose + @('--profile', 'acceptance', 'build'))
    }
    if (-not $CollectOnly) { Invoke-Docker 'up.log' ($compose + @('up', '-d')) }
    Wait-Endpoint 'http://127.0.0.1:19120/api/v2/config' 'nessie'
    Wait-Endpoint 'http://127.0.0.1:18080/health' 'airflow'
    Wait-Endpoint 'http://127.0.0.1:15000/api/v1/namespaces' 'marquez'
    if (-not $CollectOnly) {
      $statsJob = Start-Job -ArgumentList $docker, (Join-Path $ResultRoot 'resource-samples.jsonl') -ScriptBlock {
        param($EnginePath, $SamplesPath)
        while ($true) {
            $samples = & $EnginePath stats --no-stream --format '{{json .}}' 2>$null
            if ($LASTEXITCODE -eq 0) {
                $observedAt = (Get-Date).ToUniversalTime().ToString('o')
                foreach ($line in $samples) {
                    $sample = $line | ConvertFrom-Json
                    if ($sample.Name -like 'cartco-infra-p1-*') {
                        @{ observed_at = $observedAt; stats = $sample } |
                            ConvertTo-Json -Depth 5 -Compress | Add-Content -LiteralPath $SamplesPath -Encoding UTF8
                    }
                }
            }
            Start-Sleep -Seconds 2
        }
    }
    Invoke-Docker 'spark-create.log' ($compose + @('run', '--rm', 'spark-smoke'))
    Invoke-Docker 'stats.log' @('stats', '--no-stream', '--format', '{{json .}}')
    Invoke-Docker 'catalog-before.log' ($compose + @('ps', '-q', 'nessie', 'catalog-db', 'minio'))
    $catalogBefore = @(& $docker @compose ps -q nessie catalog-db minio)
    if ($LASTEXITCODE -ne 0 -or $catalogBefore.Count -ne 3) { throw 'Expected three running persistence-test containers' }
    Invoke-Docker 'stop-catalog.log' ($compose + @('stop', 'nessie'))
    Invoke-Docker 'recreate-storage.log' ($compose + @('up', '-d', '--force-recreate', 'catalog-db', 'minio'))
    Invoke-Docker 'recreate-catalog.log' ($compose + @('up', '-d', '--force-recreate', 'nessie'))
    Wait-Endpoint 'http://127.0.0.1:19120/api/v2/config' 'nessie-recreated'
    $catalogAfter = @(& $docker @compose ps -q nessie catalog-db minio)
    if ($LASTEXITCODE -ne 0 -or $catalogAfter.Count -ne 3) { throw 'Recreated services are missing' }
    if (@($catalogAfter | Where-Object { $catalogBefore -contains $_ }).Count -ne 0) { throw 'Container recreation not proven' }
    $catalogAfter | Set-Content -LiteralPath (Join-Path $ResultRoot 'catalog-after.txt')
    Invoke-Docker 'spark-restart-verify.log' ($compose + @('run', '--rm', 'spark-smoke', '--verify'))
    }
    $created = Read-VolumeJson 'compatibility.json'
    $verified = Read-VolumeJson 'restart-verification.json'
    if (-not $verified.checkpoint_match -or $created.checkpoint.uuid -cne $verified.checkpoint.uuid -or $created.checkpoint.row_sha256 -cne $verified.checkpoint.row_sha256) {
        throw 'Copied creation/restart artifacts do not match.'
    }
    $sourceHash = (Get-FileHash -LiteralPath (Join-Path $repoRoot 'scripts/phase1_spark_smoke.py') -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($sourceHash -cne $created.acceptance_source_sha256) { throw 'Acceptance image does not match current source.' }
    Invoke-Docker 'dag-imports.log' ($compose + @('exec', '-T', 'airflow-scheduler', 'python', '/opt/airflow/scripts/check_dags.py'))
    $after = Baseline-Identity
    $after | Set-Content -LiteralPath (Join-Path $ResultRoot 'baseline-after.txt')
    if ($before -cne $after) { throw 'Baseline container identities changed; inspect evidence' }
    Invoke-Docker 'status.log' ($compose + @('ps', '-a'))
    'PASS: Spark/Nessie/MinIO compatibility and recreation; baseline identities unchanged. Terraform apply and ARM/cloud acceptance remain separate.' |
        Set-Content -LiteralPath (Join-Path $ResultRoot 'SUCCESS.txt')
    Write-Host "Phase 1 container acceptance passed: $ResultRoot"
}
catch {
    $_ | Out-String | Set-Content -LiteralPath (Join-Path $ResultRoot 'error.log')
    # Capture diagnostics without deleting containers or volumes.
    try { Invoke-Docker 'failure-services.log' ($compose + @('ps', '-a')) } catch {}
    try { Invoke-Docker 'failure-logs.log' ($compose + @('logs', '--no-color', '--tail', '100')) } catch {}
    throw
}
finally {
    if ($statsJob) {
        Stop-Job -Job $statsJob -ErrorAction SilentlyContinue
        Receive-Job -Job $statsJob -ErrorAction SilentlyContinue | Out-File (Join-Path $ResultRoot 'resource-sampler.log')
    }
    $env:PATH = $oldPath
}
