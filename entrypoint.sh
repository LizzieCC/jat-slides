#!/usr/bin/env bash
set -euo pipefail
cd /app

echo "== Static paths in image =="
echo "POPULATION_GRIDS_PATH=$POPULATION_GRIDS_PATH" 
echo "GHSL_PATH=$GHSL_PATH"
echo "SEGREGATION_PATH=$SEGREGATION_PATH"
echo "JOBS_PATH=$JOBS_PATH"
echo "DATA_PATH=$DATA_PATH"
echo "DAGSTER_HOME=$DAGSTER_HOME"
mkdir -p "$DATA_PATH" "$DAGSTER_HOME"

# --- Materialize 'slides' + all upstream ---
SELECT='+key:"slides" or key:"slides"'
CMD=(dagster asset materialize --select "$SELECT" -f definitions.py)

# Optional partition: pass -e PARTITION=...
if [ -n "${PARTITION:-}" ]; then
  CMD+=(--partition "$PARTITION")
fi

echo "🚀 Running: ${CMD[*]}"
exec "${CMD[@]}"
