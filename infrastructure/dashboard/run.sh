#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
case "${1:-serve}" in
  setup)
    python3 -m venv .venv
    .venv/bin/python -m pip install -r infrastructure/dashboard/requirements.txt
    (cd frontend && npm ci && npm run build)
    ;;
  serve) shift || true; exec .venv/bin/python -m bda.analytics.demo "$@" ;;
  *) echo 'usage: run.sh setup|serve [--kafka] [--port 8000]' >&2; exit 2 ;;
esac
