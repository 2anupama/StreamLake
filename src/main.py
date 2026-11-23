"""Compatibility entry point for ``python -m src.main``."""

from streamlake.main import main

if __name__ == "__main__":
    raise SystemExit(main())