#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:?usage: run.sh smoke|baseline kafka|kafgres}"
SYSTEM="${2:?usage: run.sh smoke|baseline kafka|kafgres}"
BOOTSTRAP="127.0.0.1:9292"
OUT="$ROOT/results/$SYSTEM"
mkdir -p "$OUT"
"$ROOT/scripts/start.sh" "$SYSTEM"
trap '"$ROOT/scripts/stop.sh" "$SYSTEM" || true' EXIT
if [ "$SYSTEM" = "kafka" ]; then
  COMPOSE="$ROOT/docker-compose.kafka.yml"; SERVICE="kafka"
else
  COMPOSE="$ROOT/.vendor/kafgres/docker-compose.yml"; SERVICE="postgres"
fi
CONTAINER="$(docker compose -f "$COMPOSE" ps -q "$SERVICE")"
for _ in $(seq 1 60); do
  if python -c "import socket; s=socket.create_connection((\"127.0.0.1\",9292),1); s.close()" 2>/dev/null; then break; fi
  sleep 1
done
python -c "import socket; s=socket.create_connection((\"127.0.0.1\",9292),3); s.close()"
if [ -z "$CONTAINER" ]; then echo "Could not determine broker container" >&2; exit 1; fi
if [ "$MODE" = "smoke" ]; then
  python benchmark/benchmark.py --bootstrap "$BOOTSTRAP" --system "$SYSTEM" --output "$OUT/smoke.json" --message-size 1024 --partitions 3 --producers 1 --consumers 1 --duration 10 --warmup 3 --container "$CONTAINER"
elif [ "$MODE" = "baseline" ]; then
  python benchmark/matrix.py --bootstrap "$BOOTSTRAP" --system "$SYSTEM" --output-dir "$OUT" --profile ci --duration 20 --warmup 5 --repetitions 3 --container "$CONTAINER"
else
  echo "unknown mode: $MODE" >&2; exit 2
fi
