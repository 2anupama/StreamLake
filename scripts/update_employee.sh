#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/update_employee.sh EMPLOYEE_ID [NEW_EMAIL] [SUM_INSURED]" >&2
  exit 2
fi
.venv/bin/python scripts/cdc_crud.py update --employee-id "$1" \
  --email "${2:-cdc.updated@example.test}" \
  --sum-insured "${3:-750000}"