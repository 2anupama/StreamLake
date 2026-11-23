#!/usr/bin/env bash
set -euo pipefail

.venv/bin/python scripts/cdc_crud.py insert --employee-id "${1:-EMP-CDC-0001}"