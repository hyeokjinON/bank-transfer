#!/usr/bin/env bash
# P4-2 부하 측정 1회: cleanup → seed → (InnoDB 정리 대기) → k6 → verify.
# 결과: docs/perf/raw/<전략>-<시나리오>-<회차>.json (요약) · .full.json (k6 요약 원본).
#       정확성 확인을 통과하기 전에 멈추면(어긋남 · Ctrl+C · 확인 쿼리 실패 · 오류) 요약을 .json.invalid 로 바꾼다 — summarize.sh 는 .json 만 읽는다.
#
# 사용 (Git Bash):
#   bash load/run.sh <conditional|pessimistic|optimistic|redis> <hotspot|spread> <회차>
#   bash load/run.sh cleanup          # 측정이 끝나면 반드시 (아래 경고)
# 먼저: infra up · 모의 은행 · 같은 전략으로 띄운 API (OPERATION.md "부하 측정"). 앱 재기동은 전략이 바뀔 때만.
# 경고: 측정용 출금 수만 건이 RECEIVED + 미발행 아웃박스로 남는다. cleanup 없이 평소 설정으로 앱을 띄우면 모두 은행으로 나간다.
# 환경변수 (짧게 시험할 때만 바꾼다): WARMUP=10s MEASURE=60s(초 단위만: 숫자 + s) VUS=50 API_URL=http://localhost:8080
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /scripts/... 를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")/.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호가 PC 쪽 명령행(프로세스 목록)에 나타나지 않게, mysql 컨테이너가 이미 가진 환경변수(MYSQL_PASSWORD · MYSQL_ROOT_PASSWORD)를
# 컨테이너 안 셸이 읽어 MYSQL_PWD 로 넘긴다. 이 스크립트는 비밀번호 값을 읽지도 출력하지도 않는다 (값은 infra/.env → compose → 컨테이너로만 간다)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
rootsql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -N'; }

if [ "${1:-}" = cleanup ]; then
  sql < load/cleanup.sql
  echo "cleanup 완료 (부하용 사용자 load-* 의 지갑 · 원장 · 출금 · 아웃박스 · 인증)"
  exit 0
fi

STRATEGY=${1:?전략: conditional | pessimistic | optimistic | redis}
SCENARIO=${2:?시나리오: hotspot | spread}
RUN_NO=${3:?회차: 1, 2, 3 …}
# 회차는 파일 이름 · 멱등키(64자 이내, ASCII)에 들어간다
[[ "$RUN_NO" =~ ^[A-Za-z0-9_-]{1,16}$ ]] || { echo "회차는 영문 · 숫자 · - _ 16자 이내: $RUN_NO" >&2; exit 2; }
case "$STRATEGY" in conditional|pessimistic|optimistic|redis) ;; *) echo "모르는 전략: $STRATEGY" >&2; exit 2 ;; esac
case "$SCENARIO" in hotspot|spread) ;; *) echo "모르는 시나리오: $SCENARIO" >&2; exit 2 ;; esac
API_URL=${API_URL:-http://localhost:8080}
WARMUP=${WARMUP:-10s}
MEASURE=${MEASURE:-60s}
VUS=${VUS:-50}
# MEASURE 는 common.js 가 parseInt 로 초를 뽑아 rps 를 나눈다 — 1m 이면 처리량이 60배로 부풀려지므로 "숫자 + s" 만 받는다
[[ "$MEASURE" =~ ^[1-9][0-9]*s$ ]] || { echo "MEASURE 는 초 단위만 (예: 60s): $MEASURE" >&2; exit 2; }
[[ "$WARMUP" =~ ^[1-9][0-9]*s$ ]] || { echo "WARMUP 은 초 단위만 (예: 10s): $WARMUP" >&2; exit 2; }
[[ "$VUS" =~ ^[1-9][0-9]*$ ]] || { echo "VUS 는 양의 정수: $VUS" >&2; exit 2; }
RUN="$STRATEGY-$SCENARIO-$RUN_NO"
OUT="docs/perf/raw/$RUN.json"
FULL="docs/perf/raw/$RUN.full.json"

# 떠 있는 앱의 전략이 인자와 같은지 먼저 본다 — 다른 전략의 숫자를 잘못 적지 않게
INFO=$(curl -sf "$API_URL/actuator/info") || { echo "API($API_URL)가 떠 있지 않다" >&2; exit 1; }
grep -q "\"debit-strategy\":\"$STRATEGY\"" <<<"$INFO" || { echo "앱의 전략이 $STRATEGY 가 아니다: $INFO" >&2; exit 1; }

mkdir -p docs/perf/raw
# 정확성 확인을 통과해 OK=1 이 되기 전에 어떤 이유로든 끝나면 요약을 .invalid 로 돌려놓는다 (k6 는 중단돼도 요약을 쓴다).
# 지난 회차의 같은 이름 파일이 통과한 것으로 오해되지 않게 시작 전에 지운다.
OK=0
discard() {
  if [ "$OK" != 1 ]; then
    [ -f "$OUT" ] && mv -f "$OUT" "$OUT.invalid" && echo "[$RUN] 확인을 통과하지 못해 요약을 $OUT.invalid 로 바꿨다 — 이 회차 숫자는 쓰지 않는다" >&2
    [ -f "$FULL" ] && mv -f "$FULL" "$FULL.invalid"
  fi
  return 0
}
trap discard EXIT
trap 'exit 130' INT TERM
rm -f "$OUT" "$OUT.invalid" "$FULL" "$FULL.invalid"
echo "[$RUN] 1/5 cleanup"
sql < load/cleanup.sql
echo "[$RUN] 2/5 seed"
sql < load/seed.sql
echo "[$RUN] 3/5 InnoDB 정리(purge) 대기 — 지운 행의 정리가 측정과 겹치지 않게 (최대 60초)"
H=0
for _ in $(seq 60); do
  # INNODB_METRICS 는 PROCESS 권한이 필요해 root 로 읽는다
  H=$(echo "SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME = 'trx_rseg_history_len'" | rootsql)
  [ "${H:-0}" -lt 1000 ] && break
  sleep 1
done
[ "${H:-0}" -lt 1000 ] || echo "[$RUN] 경고: 60초가 지나도 InnoDB 정리가 끝나지 않았다 (history length=$H) — 이 회차는 정리 작업과 겹쳤을 수 있다" >&2
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
  exit 1   # EXIT 처리기가 .invalid 로 바꾼다
fi
OK=1
echo "[$RUN] 끝 — $OUT"
