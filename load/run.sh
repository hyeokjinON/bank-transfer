#!/usr/bin/env bash
# P4-2 부하 측정 1회: cleanup → seed → (InnoDB 정리 대기) → k6 → verify.
# 결과: docs/perf/raw/<전략>-<시나리오>-<회차>.json (요약) · .full.json (k6 요약 원본). 정확성이 어긋나면 .json.invalid 로 바꾸고 실패한다.
#
# 사용 (Git Bash):
#   bash load/run.sh <conditional|pessimistic|optimistic|redis> <hotspot|spread> <회차>
#   bash load/run.sh cleanup          # 측정이 끝나면 반드시 (아래 경고)
# 먼저: infra up · 모의 은행 · 같은 전략으로 띄운 API (OPERATION.md "부하 측정"). 앱 재기동은 전략이 바뀔 때만.
# 경고: 측정용 출금 수만 건이 RECEIVED + 미발행 아웃박스로 남는다. cleanup 없이 평소 설정으로 앱을 띄우면 모두 은행으로 나간다.
# 환경변수 (짧게 시험할 때만 바꾼다): WARMUP=10s MEASURE=60s(초 단위만) VUS=50 API_URL=http://localhost:8080
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /scripts/... 를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")/.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
set -a; . "./$ENV_FILE"; set +a
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호는 명령행 대신 MYSQL_PWD 로 넘긴다 (프로세스 목록 · 경고 메시지에 남지 않게). 값은 infra/.env 에서만 읽는다
sql() { dc exec -T -e MYSQL_PWD="$MYSQL_PASSWORD" mysql mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N; }
rootsql() { dc exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot -N; }

if [ "${1:-}" = cleanup ]; then
  sql < load/cleanup.sql
  echo "cleanup 완료 (부하용 사용자 load-* 의 지갑 · 원장 · 출금 · 아웃박스 · 인증)"
  exit 0
fi

STRATEGY=${1:?전략: conditional | pessimistic | optimistic | redis}
SCENARIO=${2:?시나리오: hotspot | spread}
RUN_NO=${3:?회차: 1, 2, 3 …}
case "$STRATEGY" in conditional|pessimistic|optimistic|redis) ;; *) echo "모르는 전략: $STRATEGY" >&2; exit 2 ;; esac
case "$SCENARIO" in hotspot|spread) ;; *) echo "모르는 시나리오: $SCENARIO" >&2; exit 2 ;; esac
API_URL=${API_URL:-http://localhost:8080}
WARMUP=${WARMUP:-10s}
MEASURE=${MEASURE:-60s}
VUS=${VUS:-50}
RUN="$STRATEGY-$SCENARIO-$RUN_NO"
OUT="docs/perf/raw/$RUN.json"

# 떠 있는 앱의 전략이 인자와 같은지 먼저 본다 — 다른 전략의 숫자를 잘못 적지 않게
INFO=$(curl -sf "$API_URL/actuator/info") || { echo "API($API_URL)가 떠 있지 않다" >&2; exit 1; }
grep -q "\"debit-strategy\":\"$STRATEGY\"" <<<"$INFO" || { echo "앱의 전략이 $STRATEGY 가 아니다: $INFO" >&2; exit 1; }

mkdir -p docs/perf/raw
echo "[$RUN] 1/5 cleanup"
sql < load/cleanup.sql
echo "[$RUN] 2/5 seed"
sql < load/seed.sql
echo "[$RUN] 3/5 InnoDB 정리(purge) 대기 — 지운 행의 정리가 측정과 겹치지 않게 (최대 60초)"
for _ in $(seq 60); do
  # INNODB_METRICS 는 PROCESS 권한이 필요해 root 로 읽는다
  H=$(echo "SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME = 'trx_rseg_history_len'" | rootsql)
  [ "${H:-0}" -lt 1000 ] && break
  sleep 1
done
echo "[$RUN] 4/5 k6 (워밍업 $WARMUP + 측정 $MEASURE, $VUS VU)"
dc --profile load run --rm \
  -e RUN="$RUN" -e STRATEGY="$STRATEGY" -e SCENARIO="$SCENARIO" \
  -e WARMUP="$WARMUP" -e MEASURE="$MEASURE" -e VUS="$VUS" \
  k6 run --quiet "/scripts/$SCENARIO.js"
echo "[$RUN] 5/5 verify"
RESULT=$(sql < load/verify.sql)
echo "$RESULT"
BAD=$(awk '$1 != "transfers" && $2 != 0' <<<"$RESULT")
TRANSFERS=$(awk '$1 == "transfers" { print $2 }' <<<"$RESULT")
ACCEPTED=$(grep -o '"accepted_all": *[0-9]*' "$OUT" | grep -o '[0-9]*$')
if [ -n "$BAD" ] || [ "$ACCEPTED" != "$TRANSFERS" ]; then
  echo "[$RUN] 정확성 확인 실패 — 이 회차 숫자는 버리고 원인을 찾는다 (k6 202=$ACCEPTED, 출금 행=$TRANSFERS)" >&2
  mv "$OUT" "$OUT.invalid"
  exit 1
fi
echo "[$RUN] 끝 — $OUT"
