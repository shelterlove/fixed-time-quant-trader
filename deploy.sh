#!/usr/bin/env sh
set -eu

if [ ! -f .env ]; then
  echo "missing .env" >&2
  exit 1
fi

docker compose build
docker compose run --rm trader python -m fixed_time.cli live-check --root /app
docker compose run --rm trader python -m fixed_time.cli live-seed --root /app
docker compose up -d --wait
docker compose ps
