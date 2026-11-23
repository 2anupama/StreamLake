#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/delete_employee.sh EMPLOYEE_ID" >&2
  exit 2
fi
.venv/bin/python scripts/cdc_crud.py delete --employee-id "$1"