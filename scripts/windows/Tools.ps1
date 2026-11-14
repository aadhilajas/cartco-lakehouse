# Shared executable discovery. Explicit paths fail closed; workspace tools are last-resort compatibility only.
function Resolve-CartcoExecutable {
    param([string]$ExplicitPath = '', [string]$Name, [string[]]$Candidates = @())
    $choices = if ($ExplicitPath) { @($ExplicitPath) } else { @($Name) + $Candidates }
    foreach ($choice in $choices) {
        if (-not $choice) { continue }
        $command = Get-Command $choice -ErrorAction SilentlyContinue
        $resolved = if ($command -and $command.Source) { $command.Source } else { $choice }
        if (Test-Path -LiteralPath $resolved -PathType Leaf) { return [IO.Path]::GetFullPath($resolved) }
    }
    throw "$Name executable not found. Install it on PATH or supply its explicit path."
}

function Resolve-CartcoPython {
    param([string]$ExplicitPath = '', [string]$RepoRoot)
    $choices = if ($ExplicitPath) { @($ExplicitPath) } else {
        @((Join-Path $RepoRoot '.venv/Scripts/python.exe')) +
        $(if ($env:VIRTUAL_ENV) { @(Join-Path $env:VIRTUAL_ENV 'Scripts/python.exe') } else { @() }) +
        @('python', 'python3.11', 'python3', (Join-Path (Split-Path -Parent $RepoRoot) 'work/phase1-locked-venv/Scripts/python.exe'))
    }
    foreach ($choice in $choices) {
        $command = Get-Command $choice -ErrorAction SilentlyContinue
        $resolved = if ($command -and $command.Source) { $command.Source } else { $choice }
        if (-not $resolved -or $resolved -match '[\\/]Microsoft[\\/]WindowsApps[\\/]') { continue }
        if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) { continue }
        try {
            & $resolved -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)' *> $null
            if ($LASTEXITCODE -eq 0) { return [IO.Path]::GetFullPath($resolved) }
        } catch { }
    }
    throw 'Python 3.11 was not found. Install it, create .venv, or pass -PythonPath with a working Python 3.11 executable.'
}

function Resolve-CartcoTerraform {
    param([string]$ExplicitPath = '', [string]$RepoRoot)
    Resolve-CartcoExecutable -ExplicitPath $ExplicitPath -Name 'terraform' -Candidates @(
        (Join-Path (Split-Path -Parent $RepoRoot) 'work/infra-phase1/terraform/terraform.exe')
    )
}

function Resolve-CartcoDocker {
    param([string]$ExplicitPath = '')
    $candidates = @()
    if ($env:USERPROFILE) { $candidates += Join-Path $env:USERPROFILE 'AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe' }
    if ($env:ProgramFiles) { $candidates += Join-Path $env:ProgramFiles 'Docker/Docker/resources/bin/docker.exe' }
    Resolve-CartcoExecutable -ExplicitPath $ExplicitPath -Name 'docker' -Candidates $candidates
}
