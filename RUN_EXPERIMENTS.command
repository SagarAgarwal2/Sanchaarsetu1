#!/bin/bash
# SanchaarSetu: build the stack in Docker, run the tests and the delivery experiment (Table 7).
# Double-click in Finder. Progress goes to results/mac_run_<time>.log; the last line of
# results/mac_run_status.txt says which step is running.
cd "$(dirname "$0")" || exit 1
export PATH="/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"
mkdir -p results
STAMP=$(date +%Y%m%d_%H%M%S)
LOG="results/mac_run_${STAMP}.log"
STATUS="results/mac_run_status.txt"
exec > >(tee -a "$LOG") 2>&1
step() { echo; echo "===== $(date +%H:%M:%S) $* ====="; echo "$(date +%H:%M:%S) $*" >> "$STATUS"; }
fail() { step "FAILED: $*"; echo "See $LOG"; exit 1; }
echo "run $STAMP" > "$STATUS"

step "1/9 waiting for Docker"
if ! docker info >/dev/null 2>&1; then
  open -a Docker 2>/dev/null
  for i in $(seq 1 90); do docker info >/dev/null 2>&1 && break; sleep 2; done
fi
docker info >/dev/null 2>&1 || fail "Docker is not running (install/start Docker Desktop)"
docker --version; docker compose version

step "2/9 building and starting api, postgres, redis, redpanda (first build can take 10+ min)"
docker compose down --remove-orphans
RETRY_DELAYS=0.3,0.3,0.3,0.3 docker compose up --build -d api || fail "docker compose up"

wait_api() {
  for i in $(seq 1 120); do
    curl -sf http://127.0.0.1:8000/health >/dev/null && return 0; sleep 2
  done
  docker compose logs --tail 80 api; return 1
}

reset_db() {
  docker compose exec -T postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS sanchaar WITH (FORCE)" -c "CREATE DATABASE sanchaar" || return 1
  docker compose exec -T postgres psql -U postgres -d sanchaar -v ON_ERROR_STOP=1 -q \
    -f /docker-entrypoint-initdb.d/init.sql >/dev/null || return 1
  docker compose exec -T redis redis-cli FLUSHALL
}

step "3/9 fresh database from db/init.sql"
docker compose stop api
reset_db || fail "database reset"
RETRY_DELAYS=0.3,0.3,0.3,0.3 docker compose up -d --force-recreate api
wait_api || fail "api did not become healthy"

step "4/9 installing evaluation packages in the api container"
docker compose exec -T api pip install -q -r requirements-eval.txt || fail "pip install"

step "5/9 downloading/loading the embedding model"
docker compose exec -T api python -c "
import app.transform as T
m = T.get_model()
print('matcher backend:', T.MATCHER_BACKEND)
assert m is not None, 'embedding model not available'
" || fail "embedding model"

step "6/9 unit tests"
docker compose exec -T api python -m pytest -q tests || fail "unit tests"

step "7/9 end-to-end checks (short retry delays)"
docker compose exec -T api python -m tests.check_fixes || fail "end-to-end checks"

step "8/9 fresh database, normal retry delays (2,4,8,16 s)"
docker compose stop api
reset_db || fail "database reset"
docker compose up -d --force-recreate api
wait_api || fail "api did not become healthy"
docker compose exec -T api pip install -q -r requirements-eval.txt

step "9/9 delivery experiment, 3 conditions x 300 updates (about 5-10 min)"
docker compose exec -T api python -m eval.load_test \
  --dsn postgres://postgres:postgres@postgres:5432/sanchaar || fail "load test"

step "DONE: results/table7_delivery.json written"
echo "The stack is still running (http://127.0.0.1:8000/docs). Stop it with: docker compose down"
