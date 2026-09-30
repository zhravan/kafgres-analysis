#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFGRES_DIR="$ROOT/.vendor/kafgres"
KAFGRES_REF="${KAFGRES_REF:-00168534b8899300896b5ca1582a6ecca3de81d1}"

mkdir -p "$ROOT/.vendor"
if [[ ! -d "$KAFGRES_DIR/.git" ]]; then
  git clone https://github.com/RayElg/kafgres.git "$KAFGRES_DIR"
fi

git -C "$KAFGRES_DIR" fetch --quiet origin
git -C "$KAFGRES_DIR" checkout --quiet "$KAFGRES_REF"

echo "Kafgres commit:"
git -C "$KAFGRES_DIR" rev-parse HEAD
