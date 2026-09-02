#!/usr/bin/env sh
set -eu

if [ ! -f .env ]; then
  echo "missing .env" >&2
  exit 1
fi

if docker compose ps --status running --services | grep -Fxq trader; then
  echo "existing trader detected: performing a controlled upgrade and skipping live-seed"
  docker compose up -d --build --wait
else
  echo "no running trader detected: validating the account and seeding P90 history"
  docker compose build
  docker compose run --rm trader python -m fixed_time.cli live-check --root /app
  docker compose run --rm trader python -m fixed_time.cli live-seed --root /app
  docker compose up -d --wait
fi
docker compose ps
