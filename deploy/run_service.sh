#!/usr/bin/env bash
set -euo pipefail

cd /opt/sofascore
source .venv/bin/activate
set -a
source .env
set +a

exec python -m schema_inspector.cli "$@"
