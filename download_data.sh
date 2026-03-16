#!/usr/bin/env bash
# Download ONRC open data CSVs from data.gov.ro
# Source: https://data.gov.ro/dataset/lista-firme-din-registrul-comertului

set -euo pipefail
cd "$(dirname "$0")"

BASE="https://data.gov.ro/dataset/a3742920-0343-4bcc-a4c5-0921520b40a2/resource"

declare -A FILES=(
  ["od_firme.csv"]="3582b6a9-9363-4b6a-8122-6e0e12227b3a"
  ["od_stare_firma.csv"]="38806c6b-43a8-4e67-85f7-4078e722fdef"
  ["od_caen_autorizat.csv"]="8dc9e157-4c68-4a87-a4e1-2e61dc9e4785"
  ["n_stare_firma.csv"]="cd583e0d-5e2b-4e55-a7bf-336bd8ebfaf0"
  ["n_caen.csv"]="80a1e888-69e8-4ace-87a5-fe6364ced8c0"
)

for file in "${!FILES[@]}"; do
  if [ -f "$file" ]; then
    echo "Skipping $file (already exists)"
  else
    echo "Downloading $file..."
    curl -L -o "$file" "${BASE}/${FILES[$file]}/download/${file}"
  fi
done

echo "Done. Run prepare_d1.py and prepare_caen_slim.py to generate SQL chunks."
