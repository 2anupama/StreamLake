"""Wait for the registered connector and report its task state."""

from __future__ import annotations

import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from dotenv import load_dotenv

load_dotenv()
base_url = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/")
url = f"{base_url}/connectors/streamlake-postgres/status"

for attempt in range(30):
    try:
        with urlopen(url, timeout=10) as response:
            status = json.loads(response.read().decode("utf-8"))
        print(json.dumps(status, indent=2))
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        if connector_state == "RUNNING" and task_states and all(state == "RUNNING" for state in task_states):
            raise SystemExit(0)
    except HTTPError as error:
        if error.code != 404:
            raise
    except URLError:
        pass
    if attempt < 29:
        time.sleep(2)

print("Debezium connector did not reach RUNNING", file=sys.stderr)
raise SystemExit(1)