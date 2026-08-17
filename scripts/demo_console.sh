#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
export PYTHONPATH="${PYTHONPATH:-}:$ROOT/src:$ROOT"
exec python apps/console/main.py --demo "$@"
