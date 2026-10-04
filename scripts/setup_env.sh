#!/usr/bin/env bash
set -euo pipefail

env_name="${BDS_ENV_NAME:-bds-uber}"
if ! command -v conda >/dev/null 2>&1; then
	echo "Conda was not found. Install Miniconda manually and run this script again." >&2
	exit 1
fi

if conda env list | awk '{print $1}' | grep -Fxq "$env_name"; then
	printf 'Conda environment %s already exists; reusing it.\n' "$env_name"
else
	conda create --name "$env_name" python=3.11 --yes
fi
conda run --name "$env_name" python -m pip install --requirement requirements.txt
printf 'Environment %s is ready. Activate it with: conda activate %s\n' "$env_name" "$env_name"
