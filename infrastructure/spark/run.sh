#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
compose=(docker compose -f infrastructure/kafka/compose.yml -f infrastructure/spark/compose.yml)
case "${1:-run}" in
  build) "${compose[@]}" build spark ;;
  start) python3 infrastructure/spark/preflight.py; "${compose[@]}" up -d --wait kafka ;;
  stop) "${compose[@]}" stop ;;
  test) "${compose[@]}" run --rm --no-deps -e RUN_SPARK_TESTS=1 --entrypoint python3 spark -m pytest bda/streaming/tests -q -p no:cacheprovider ;;
  live-test) "${compose[@]}" run --rm -e RUN_SPARK_INTEGRATION=1 --entrypoint python3 spark -m pytest bda/streaming/tests/test_live.py -q -s -p no:cacheprovider ;;
  run|inspect)
    action="${1:-run}"
    if (( $# )); then shift; fi
    script=job
    if [[ "$action" == inspect ]]; then script=report; fi
    "${compose[@]}" run --rm spark --master 'local[2]' --driver-memory 1g \
      --conf spark.jars.ivy=/opt/spark/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
      "/workspace/bda/streaming/$script.py" "$@"
    ;;
  *) echo 'usage: run.sh build|start|stop|test|live-test|run|inspect [job arguments]' >&2; exit 2 ;;
esac
