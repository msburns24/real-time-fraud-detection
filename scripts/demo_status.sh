#!/usr/bin/env bash
# Compact stack status for the screencast — the default `docker compose ps`
# table is too wide to read at screencast font sizes.
set -euo pipefail
printf '%-20s %-10s %s\n' SERVICE STATE STATUS
printf '%-20s %-10s %s\n' -------- ----- ------
docker compose ps -a --format '{{.Service}}\t{{.State}}\t{{.Status}}' \
  | sort | while IFS=$'\t' read -r svc state status; do
      printf '%-20s %-10s %s\n' "$svc" "$state" "$status"
    done
