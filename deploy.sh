#!/usr/bin/env sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

case "${1:-}" in
  --pull)
    [ -z "$(git status --porcelain)" ] || { echo 'Commit/stash local changes before --pull.' >&2; exit 1; }
    git pull --ff-only
    exec sh ./deploy.sh
    ;;
  "") ;;
  *) echo 'Usage: sh deploy.sh [--pull]' >&2; exit 1 ;;
esac

if [ ! -f .env ]; then
  (umask 077; cp .env.example .env)
  echo 'Created .env. Fill testnet credentials and set TRADING_ENABLED=true, then rerun.' >&2
  exit 1
fi
command -v docker >/dev/null
docker compose version >/dev/null
docker info >/dev/null
SOURCE_REVISION=$(git rev-parse HEAD)
if [ -n "$(git status --porcelain)" ]; then
  SOURCE_REVISION="${SOURCE_REVISION}-dirty"
fi
export SOURCE_REVISION

docker compose config --quiet
# Tests use neither credentials nor the production runtime volume.
docker build --target test --build-arg "SOURCE_REVISION=$SOURCE_REVISION" -t fixed-time-deploy-tests .
docker run --rm --network none fixed-time-deploy-tests
docker compose build
docker compose run --rm --no-deps trader python -m fixed_time.cli live-deploy-check --root /app

restart_previous=false
restore_previous() {
  if [ "$restart_previous" = true ]; then docker compose start trader || true; fi
}
trap restore_previous EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if docker compose ps --status running --services | grep -Fxq trader; then
  restart_previous=true
  docker compose stop -t 60 trader
fi
# Check again after stopping the old trader, then make a WAL-safe SQLite backup.
docker compose run --rm --no-deps trader python -m fixed_time.cli live-deploy-check --root /app
docker compose run --rm --no-deps trader python -m fixed_time.cli live-backup --root /app
restart_previous=false
if ! docker compose up -d --wait --wait-timeout 180; then
  echo 'Startup failed. Inspect logs and account state; do not restore an old database over live fills.' >&2
  docker compose ps
  exit 1
fi
docker compose ps
