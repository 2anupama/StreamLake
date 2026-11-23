#!/usr/bin/env bash
set -euo pipefail

.venv/bin/python scripts/register_connector.py
.venv/bin/python scripts/verify_connector.py