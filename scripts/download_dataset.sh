#!/usr/bin/env bash
set -euo pipefail

url="https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-apr14.csv"
output="${DATASET_PATH:-data/raw/uber-raw-data-apr14.csv}"
mkdir -p "$(dirname "$output")"
curl --fail --location --silent --show-error "$url" --output "$output"
printf 'Downloaded dataset to %s\n' "$output"
