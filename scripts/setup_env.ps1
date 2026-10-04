$ErrorActionPreference = 'Stop'

$conda = Get-Command conda -ErrorAction SilentlyContinue
if (-not $conda) {
    throw 'Conda was not found. Install Miniconda manually, initialize it for PowerShell, and run this script again.'
}

$envName = if ($env:BDS_ENV_NAME) { $env:BDS_ENV_NAME } else { 'bds-uber' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$envInfo = conda env list --json | ConvertFrom-Json
$existing = $envInfo.envs | Where-Object { (Split-Path -Leaf $_) -eq $envName }

if ($existing) {
    Write-Host "Conda environment '$envName' already exists; it will be reused."
} else {
    Write-Host "Creating Conda environment '$envName' with Python 3.11."
    conda create --name $envName python=3.11 --yes
}

conda run --name $envName python -m pip install --requirement (Join-Path $repoRoot 'requirements.txt')
Write-Host "Environment setup complete. Activate it with: conda activate $envName"
