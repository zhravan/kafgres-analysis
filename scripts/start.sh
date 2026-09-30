#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "\${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEM="\${1:?usage: start.sh kafka|kafgres}"

case "$SYSTEM" in
  kafka) docker compose -f "$ROOT/docker-compose.kafka.yml" up -d ;;
  kafgres)
    "$ROOT/scripts/setup.sh"
    docker compose -f "$ROOT/.vendor/kafgres/docker-compose.yml" up -d --build
    ;;
  *) echo "unknown system: $SYSTEM" >&2; exit 2 ;;
esac
