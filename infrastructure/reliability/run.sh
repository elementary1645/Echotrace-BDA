#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
action="${1:-help}"
if (( $# )); then shift; fi
case "$action" in
  verify)
    # New output name is mandatory; historical runs are never overwritten.
    output="${1:?usage: run.sh verify artifacts/phase5/UNIQUE [--campaign] [--port 19095]}"
    shift
    .venv/bin/python -m bda.reliability.run --output "$output" "$@"
    (cd frontend && npm run build)
    exec .venv/bin/python -m bda.reliability.presentation --run "$output"
    ;;
  preflight) exec .venv/bin/python -m bda.reliability.preflight ;;
  *) echo 'usage: run.sh preflight|verify artifacts/phase5/UNIQUE [--campaign] [--port 19095]' >&2; exit 2 ;;
esac
