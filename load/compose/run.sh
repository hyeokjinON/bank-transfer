#!/usr/bin/env bash
# P8-2 실험 E3 · E4 (스펙 §7-3): Compose app 프로필(api 2대 + nginx)에 출금을 일정 속도로 넣으면서 api-1 을 재시작(E3) · 강제 종료(E4)한다.
# 확인: 첫 시도 5xx · 502 수, 같은 키 재시도 뒤 접수된 키 수 = 출금 행 수, 전부 COMPLETED, 은행 거래 기록 = 출금 행(같은 거래번호 두 번 0),
#       잔고 = 원장 합, 리밸런스 줄(api 로그), nginx 오류 줄(api-1 을 뺀 시각). 정확성 확인을 통과하지 못하면 요약을 .json.invalid 로 바꾼다.
# 사용 (Git Bash, 저장소 루트):
#   bash load/compose/run.sh <steady|restart|kill> <회차>      # steady = 아무것도 하지 않는 기준 회차
#   bash load/compose/run.sh cleanup                           # 끝나면 반드시
# 먼저: docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app up -d --build --wait
#       IntelliJ 의 API · 게이트웨이 · 모의 은행은 끈다 (같은 DB · 같은 컨슈머 그룹 — 스펙 §5-5)
# 환경변수: RATE(초당 출금, 20) DURATION(k6 시간, 60s) ACTION_AT(동작 시각 초, 20) SETTLE(완료 대기 최대 초, 240)
# 결과: docs/perf/raw/p8-2/<동작>-<회차>.json (요약) · .k6.json (k6), 작업 파일 build/load-p82/<동작>-<회차>/
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /scripts/... · /data/... 를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")/../.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호가 PC 명령행에 나오지 않게 mysql 컨테이너의 환경변수로 (load/run.sh 와 같다)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
RUN=setup
die() { echo "[$RUN] $*" >&2; exit 1; }
USERS="user_id LIKE 'load-p82-%' AND user_id REGEXP '^load-p82-u0(0[1-9]|1[0-9]|20)\$'"

if [ "${1:-}" = cleanup ]; then
  sql < load/compose/cleanup.sql
  echo "cleanup 완료 (load-p82-u001 ~ load-p82-u020 의 지갑 · 원장 · 출금 · 아웃박스 · 인증)"
  exit 0
fi

ACTION=${1:?동작: steady | restart | kill}
ROUND=${2:?회차: 1, 2 …}
case "$ACTION" in steady|restart|kill) ;; *) die "모르는 동작: $ACTION" ;; esac
[[ "$ROUND" =~ ^[0-9]{1,2}$ ]] || die "회차는 숫자 1 ~ 2자리: $ROUND"
RATE=${RATE:-20}; DURATION=${DURATION:-60s}; ACTION_AT=${ACTION_AT:-20}; SETTLE=${SETTLE:-240}
RUN="$ACTION-$ROUND"
OUT_DIR=docs/perf/raw/p8-2; OUT="$OUT_DIR/$RUN.json"; K6J="$OUT_DIR/$RUN.k6.json"; WORK="build/load-p82/$RUN"
rm -rf "$WORK"; mkdir -p "$OUT_DIR" "$WORK"; rm -f "$OUT" "$OUT.invalid" "$K6J"

# --- 1. 사전 확인 ---
for s in mock-bank bank-gateway api-1 api-2 nginx; do
  id=$(dc --profile app ps -q "$s")
  [ -n "$id" ] || die "$s 가 없다 — 먼저 --profile app up -d --build --wait"
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$id")" = healthy ] || die "$s 가 healthy 가 아니다"
done
OTHER=$(sql <<<"SELECT COUNT(*) FROM transfer WHERE status IN ('RECEIVED','SENDING','UNKNOWN') AND NOT ($USERS)" | tr -d '\r')
[ "$OTHER" = 0 ] || echo "[$RUN] 경고: 측정 사용자 밖에 처리 중인 출금 $OTHER 건 — 컨테이너 api 가 함께 처리한다 (측정 확인은 측정 사용자만 본다)" >&2

# --- 2. 시드 ---
sql < load/compose/cleanup.sql
sql < load/compose/seed.sql
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- 3. 부하 + 동작 ---
echo "[$RUN] k6 ${RATE}건/s × $DURATION → nginx:80, 동작 $ACTION @ ${ACTION_AT}s"
dc --profile load run --rm -e BASE_URL=http://nginx:80 -e RUN="$RUN" -e RATE="$RATE" -e DURATION="$DURATION" \
  k6 run --quiet /scripts/compose/withdraw.js > "$WORK/k6.out" 2>&1 &
K6_PID=$!
T_ACTION=null; AUTO_RESTART=null
if [ "$ACTION" != steady ]; then
  sleep "$ACTION_AT"
  T_ACTION="\"$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)\""
  case "$ACTION" in
    restart) dc --profile app restart api-1 >/dev/null ;;                     # SIGTERM → graceful(단계당 최대 20초) → 다시 기동
    kill)    docker kill "$(dc --profile app ps -q api-1)" >/dev/null ;;      # SIGKILL — 처리 중 요청 · 커밋 안 한 오프셋이 그대로 끊긴다
  esac
fi
wait "$K6_PID" || die "k6 가 실패했다 — $WORK/k6.out"
[ -f "$K6J" ] || die "k6 요약이 없다 — $WORK/k6.out"
if [ "$ACTION" = kill ]; then
  # restart: unless-stopped 가 kill 뒤에 다시 띄웠나 (Docker 는 사람이 멈춘 컨테이너를 다시 띄우지 않는다 — kill 이 그 경우인지 기록)
  st=$(docker inspect -f '{{.State.Status}}' "$(dc --profile app ps -aq api-1)")
  if [ "$st" = running ]; then AUTO_RESTART=true; else AUTO_RESTART=false; fi
fi
dc --profile app up -d --no-build --wait api-1 >/dev/null   # 내려가 있으면 올린다 — 정합성은 두 대로 끝까지 처리한 뒤 본다
# 다시 뜬 api-1 의 IP 가 바뀌었을 수 있다 — nginx 는 기동 때 한 번만 이름을 푼다. 부하는 끝났으므로 다음 회차 · E2 를 위해 다시 풀게 한다
[ "$ACTION" = steady ] || dc --profile app restart nginx >/dev/null

# --- 4. 완료 대기 (재배달 · SENDING 정리 · 결과조회까지) ---
P=-1; SETTLE_S=0
for SETTLE_S in $(seq "$SETTLE"); do
  P=$(sql <<<"SELECT COUNT(*) FROM transfer WHERE $USERS AND status <> 'COMPLETED'" | tr -d '\r')
  [ "$P" = 0 ] && break
  sleep 1
done

# --- 5. 정확성 ---
declare -A C
while IFS=$'\t' read -r k v; do C[$k]=$v; done < <(sql < load/compose/verify.sql | tr -d '\r')
k6v() { grep -o "\"$1\": *[0-9.]*" "$K6J" | head -1 | sed 's/.*: *//'; }
ACCEPTED=$(k6v accepted_keys)
# 은행 거래 기록 한 줄 = "D" + 거래번호 20자 + … (load/consumer 와 같다). 측정 출금의 거래번호마다 정확히 한 줄이어야 한다
sql < load/compose/refs.sql | tr -d '\r' | sort > "$WORK/refs.txt"
{ dc --profile app exec -T mock-bank sh -c 'cat /data/bank-files/journal/*.log 2>/dev/null' || true; } | grep -a '^D' | cut -c2-21 | sort > "$WORK/journal-refs.txt" || true
JOURNAL_MISSING=$(comm -23 "$WORK/refs.txt" <(sort -u "$WORK/journal-refs.txt") | wc -l | tr -d ' ')
JOURNAL_DUP=$({ grep -xF -f "$WORK/refs.txt" "$WORK/journal-refs.txt" || true; } | uniq -d | wc -l | tr -d ' ')

# --- 6. 로그: nginx 5xx · 오류, 리밸런스 ---
dc --profile app logs --no-color --no-log-prefix --since "$T0" nginx > "$WORK/nginx.log" 2>&1 || true
dc --profile app logs --no-color --timestamps --since "$T0" api-1 api-2 > "$WORK/api.log" 2>&1 || true
grep -E "partitions (assigned|revoked)|Adding newly assigned partitions|Revoke previously assigned partitions" "$WORK/api.log" > "$WORK/rebalance.txt" || true
grep -E "\[(error|warn)\].*upstream" "$WORK/nginx.log" > "$WORK/nginx-errors.txt" || true
NGINX_5XX=$(awk '$2 ~ /^5/ && $4 == "/api/v1/transfers"' "$WORK/nginx.log" | wc -l | tr -d ' ')

num() { if [[ "${1:-}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then echo "$1"; else echo null; fi; }
cat > "$OUT" <<JSON
{"run":"$RUN","action":"$ACTION","rate":$RATE,"duration":"$DURATION","action_at_s":$ACTION_AT,"t0":"$T0","t_action":$T_ACTION,
 "auto_restart_after_kill":$AUTO_RESTART,"settle_s":$SETTLE_S,
 "first_202":$(num "$(k6v first_202)"),"first_5xx":$(num "$(k6v first_5xx)"),"first_502":$(num "$(k6v first_502)"),"first_neterr":$(num "$(k6v first_neterr)"),
 "retried_requests":$(num "$(k6v retried_requests)"),"accepted_keys":$(num "$ACCEPTED"),"gave_up_keys":$(num "$(k6v gave_up_keys)"),
 "p95_ms":$(num "$(k6v p95_ms)"),"nginx_5xx":$NGINX_5XX,
 "transfers":$(num "${C[transfers]:-}"),"not_completed":$(num "${C[not_completed]:-}"),"refunds":$(num "${C[refunds]:-}"),
 "negative_balance":$(num "${C[negative_balance]:-}"),"balance_ne_ledger":$(num "${C[balance_ne_ledger]:-}"),
 "transfer_ne_withdraw":$(num "${C[transfer_ne_withdraw]:-}"),"unpublished_outbox":$(num "${C[unpublished_outbox]:-}"),
 "journal_missing":$JOURNAL_MISSING,"journal_dup":$JOURNAL_DUP,"rebalance_lines":$(wc -l < "$WORK/rebalance.txt" | tr -d ' ')}
JSON
cp "$WORK/rebalance.txt" "$OUT_DIR/$RUN.rebalance.txt"
cp "$WORK/nginx-errors.txt" "$OUT_DIR/$RUN.nginx-errors.txt"

FAILS=()
[ "${C[not_completed]:-x}" = 0 ] || FAILS+=("COMPLETED 아님 ${C[not_completed]:-?} (대기 ${SETTLE_S}초)")
for k in refunds negative_balance balance_ne_ledger transfer_ne_withdraw unpublished_outbox; do
  [ "${C[$k]:-x}" = 0 ] || FAILS+=("$k ${C[$k]:-?}")
done
[ "${C[transfers]:-x}" = "$ACCEPTED" ] || FAILS+=("출금 행 ${C[transfers]:-?} ≠ 접수된 키 $ACCEPTED")
[ "$JOURNAL_MISSING" = 0 ] || FAILS+=("은행 기록에 없는 출금 $JOURNAL_MISSING")
[ "$JOURNAL_DUP" = 0 ] || FAILS+=("은행에 두 번 간 거래번호 $JOURNAL_DUP")
if [ ${#FAILS[@]} -gt 0 ]; then
  mv -f "$OUT" "$OUT.invalid"
  die "정확성 확인 실패 — ${FAILS[*]} ($OUT.invalid, 작업 파일 $WORK)"
fi
# 5xx 는 판정이 아니라 결과다 (스펙: 재시작이면 0 이어야 한다 — 아니면 문서에 원인을 적는다)
if [ "$ACTION" = restart ] && [ "$(k6v first_5xx)" != 0 ]; then echo "[$RUN] 주의: 재시작인데 첫 시도 5xx $(k6v first_5xx) — nginx-errors · api 로그로 원인을 본다" >&2; fi
echo "[$RUN] 통과 — $OUT"
cat "$OUT"
