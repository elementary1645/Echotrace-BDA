#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p artifacts/phase3
# No Compose dependency on Kafka; its containers and storage are never touched.
extra_mount=()
if [[ -n "${BATCH_PHASE2_VOLUME:-}" ]]; then
  docker volume inspect "$BATCH_PHASE2_VOLUME" >/dev/null
  extra_mount=(-v "$BATCH_PHASE2_VOLUME:/data:ro")
fi
action="${1:-help}"
if (( $# )); then shift; fi
case "$action" in
  generate|fixtures|baseline|plot|clean)
    module="$action"
    if [[ "$action" == clean ]]; then module=cleanup; fi
    exec .venv/bin/python -m "bda.batch.$module" "$@"
    ;;
  lake|benchmark|export|test)
    script="/workspace/bda/batch/$action.py"
    if [[ "$action" == export ]]; then script="/workspace/bda/analytics/export.py"; fi
    if [[ "$action" == test ]]; then
      exec docker run --rm --cpus 2 --memory 3g \
        -e RUN_BATCH_SPARK_TESTS=1 -e SPARK_LOCAL_IP=127.0.0.1 \
        -v "$PWD:/workspace:ro" -v "$PWD/artifacts/phase3:/artifacts" \
        --entrypoint python3 echotrace-spark:4.0.1 \
        -m pytest bda/batch/tests -q -p no:cacheprovider "$@"
    fi
    exec docker run --rm --cpus 2 --memory 3g --user "$(id -u):$(id -g)" \
      -e SPARK_LOCAL_IP=127.0.0.1 -e HOME=/tmp \
      -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
      -v "$PWD:/workspace:ro" -v "$PWD/artifacts/phase3:/artifacts" \
      "${extra_mount[@]}" --entrypoint /opt/spark/bin/spark-submit echotrace-spark:4.0.1 \
      --master "${BATCH_MASTER:-local[2]}" --driver-memory 1g \
      --conf spark.jars.ivy=/tmp/ivy --conf spark.sql.adaptive.enabled=false "$script" "$@"
    ;;
  *) echo 'usage: run.sh generate|fixtures|lake|benchmark|test|plot|clean [arguments]' >&2; exit 2 ;;
esac
