"""Register or update the local Debezium PostgreSQL connector."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR_FILE = ROOT / "config" / "debezium-postgres.json"
VARIABLE_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
LOCAL_DEFAULTS = {
    "POSTGRES_USER": "streamlake",
    "POSTGRES_PASSWORD": "streamlake_local_only",
    "POSTGRES_DB": "streamlake_source",
}


def expand_variables(value):
    if isinstance(value, dict):
        return {key: expand_variables(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_variables(item) for item in value]
    if isinstance(value, str):
        return VARIABLE_PATTERN.sub(
            lambda match: os.environ.get(match.group(1)) or LOCAL_DEFAULTS.get(match.group(1), ""),
            value,
        )
    return value


def register_connector() -> None:
    load_dotenv(ROOT / ".env")
    definition = expand_variables(json.loads(CONNECTOR_FILE.read_text(encoding="utf-8")))
    name = definition["name"]
    url = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/")
    endpoint = f"{url}/connectors/{name}/config"
    request = Request(
        endpoint,
        data=json.dumps(definition["config"]).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )

    for attempt in range(30):
        try:
            with urlopen(request, timeout=10) as response:
                result = json.loads(response.read().decode("utf-8"))
                print(
                    json.dumps(
                        {
                            "name": name,
                            "state": "registered",
                            "connector_type": result.get("connector.class"),
                        },
                        indent=2,
                    )
                )
                return
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if error.code not in {502, 503, 504}:
                raise RuntimeError(f"Kafka Connect rejected connector config: {body}") from error
        except (TimeoutError, URLError):
            pass
        if attempt < 29:
            time.sleep(2)

    raise RuntimeError(f"Kafka Connect at {url} did not become available")


if __name__ == "__main__":
    try:
        register_connector()
    except Exception as error:
        print(f"Connector registration failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error