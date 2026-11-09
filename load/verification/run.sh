#!/usr/bin/env bash
# P6-2 웹 1원인증 측정 1회 (설계 §6): 정리 → 측정용 모의 은행(지연) → 앱(판) → 순차 워밍업 3건 → k6 열린 모델(워밍업 10초 + 측정 30초) → 정확성.
# 결과: docs/perf/raw/p6-2/<이름>.json (이름 = <판>-d<지연>-r<도착률>[-t<톰캣>]-<회차>).
#       정확성 확인을 통과하기 전에 끝나면(어긋남 · 오류 · Ctrl+C) .json.invalid 로 바꾼다 — summarize.sh 는 .json 만 읽는다.
#       k6 요약 · 로그 · 표본은 build/load-p62/<이름>/ (git 제외).
#
# 사용 (Git Bash, 저장소 루트):
#   [VT=1] [RAW_SUB=p7] bash load/verification/run.sh <blocking|async|coroutine|reactor> <지연ms 0~2999> <도착률 1~5000> <회차> [톰캣 최대 스레드=200]
#   VT=1: 가상 스레드 + JFR pinning 집계 (MVC 판만, P7 설계 §6-3). RAW_SUB: 원자료 폴더 docs/perf/raw/<RAW_SUB> (기본 p6-2)
#   bash load/verification/run.sh cleanup      # 측정이 끝나면 반드시
# 먼저 (OPERATION §12): infra up (MySQL · Kafka — apps/api 는 리스너를 꺼도 기동 때 Kafka 관리 클라이언트가 붙는다),
#   ./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist
#   측정 포트 18080 · 18081 · 9190 은 비어 있어야 한다. 개발용 API(8080) · 개발용 은행(9090)은 켜 둬도 된다 — 측정 앱은 다른 포트 · 다른 은행을 쓰고,
#   apps/api 측정 인스턴스는 Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존 스케줄을 끈다 (개발용 메시지 · 행을 건드리지 않는다).
# 경고: 측정 데이터는 사용자 p62-u001 ~ u100 의 1원인증 행뿐이다 (SENT, 30일 뒤 보존 배치 대상). 끝나면 cleanup.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /scripts/... 를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")/../.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호는 mysql 컨테이너가 가진 환경변수를 컨테이너 안 셸이 읽는다 — PC 쪽 명령행에 나타나지 않는다 (load/run.sh 와 같다)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
RUN=verification
die() { echo "[$RUN] $*" >&2; exit 1; }
. load/lib/proc.sh
USERS="user_id LIKE 'p62-u%' AND user_id REGEXP '^p62-u(00[1-9]|0[1-9][0-9]|100)\$'"   # cleanup.sql 과 같은 조건

if [ "${1:-}" = cleanup ]; then
  sql < load/verification/cleanup.sql
  echo "cleanup 완료 (측정 사용자 p62-u001 ~ u100 의 1원인증 행)"
  exit 0
fi

MODE=${1:?판: blocking | async | coroutine | reactor}
DELAY=${2:?은행 지연 ms: 0 ~ 2999}
RATE=${3:?도착률 (건/s): 1 ~ 5000}
ROUND=${4:?회차: 1, 2, 3, probe1, smoke …}
TOMCAT=${5:-200}
case "$MODE" in
  blocking|async) APP=api; PORT=18080 ;;
  coroutine|reactor) APP=api-reactive; PORT=18081 ;;
  *) echo "모르는 판: $MODE" >&2; exit 2 ;;
esac
{ [[ "$DELAY" =~ ^[0-9]{1,4}$ ]] && [ "$DELAY" -le 2999 ]; } || { echo "지연은 0 ~ 2999 (ms): $DELAY" >&2; exit 2; }
{ [[ "$RATE" =~ ^[0-9]{1,4}$ ]] && [ "$RATE" -ge 1 ] && [ "$RATE" -le 5000 ]; } || { echo "도착률은 1 ~ 5000: $RATE" >&2; exit 2; }
[[ "$ROUND" =~ ^[A-Za-z0-9_-]{1,16}$ ]] || { echo "회차는 영문 · 숫자 · - _ 16자 이내: $ROUND" >&2; exit 2; }
{ [[ "$TOMCAT" =~ ^[0-9]{1,4}$ ]] && [ "$TOMCAT" -ge 1 ]; } || { echo "톰캣 최대 스레드는 1 이상: $TOMCAT" >&2; exit 2; }
[ "$APP" = api ] || [ "$TOMCAT" = 200 ] || { echo "톰캣 스레드는 MVC 판(blocking · async)에만 준다" >&2; exit 2; }
. load/lib/vt.sh
vt_validate "$([ "$APP" = api ] && echo 1 || echo 0)"
RAW_SUB=${RAW_SUB:-p6-2}; raw_sub_validate "$RAW_SUB"
NAME="$MODE$(vt_tag)-d$DELAY-r$RATE"
[ "$TOMCAT" != 200 ] && NAME="$NAME-t$TOMCAT"
NAME="$NAME-$ROUND"
RUN=$NAME
RAW="docs/perf/raw/$RAW_SUB"
OUT="$RAW/$NAME.json"
WORK="build/load-p62/$NAME"
BANK_FILES="$WORK/bank-files"   # 회차마다 새로 — 1원 이체 수(DV 줄)를 이 회차만 센다. 개발용 ./bank-files 와 따로
vt_args "$WORK/app.jfr"   # VT=1 이면 VT_JVM_ARGS · VT_APP_ARGS (lib/vt.sh)
BANK_PORT=9190
API="http://localhost:$PORT"
JAR="apps/$APP/build/libs/$APP-0.1.0-SNAPSHOT.jar"
BANK_CP="apps/mock-bank/build/install/mock-bank/lib/*"
WARMUP_S=10
MEASURE_S=30
[ -f "$JAR" ] || die "jar 가 없다 — ./gradlew :apps:$APP:bootJar"
ls apps/mock-bank/build/install/mock-bank/lib/*.jar >/dev/null 2>&1 || die "모의 은행 배포본이 없다 — ./gradlew :apps:mock-bank:installDist"

# --- 프로세스 · 정리 ---
APP_PID=; BANK_PID=; SAMPLER=
OWN_APP=0; OWN_BANK=0   # 이 실행이 그 포트에서 프로세스를 띄운 뒤에만 1 — 사전 확인에서 멈출 때 남의 프로세스를 강제 종료하지 않게
OK=0
STARTED=0   # 이전 결과를 지우는 지점부터 1 — 그 앞에서 멈추면 이전 회차 결과를 .invalid 로 바꾸지 않는다
stop_pid() { [ -n "${1:-}" ] || return 0; kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }
free_port() {   # 그 포트를 쥔 프로세스가 끝날 때까지 (최대 15초). kill 이 듣지 않았으면 그 포트의 측정 JVM 을 Windows PID 로 강제 종료 (lib/proc.sh, load/consumer/run.sh 와 같다)
  for _ in $(seq 15); do port_busy "$1" || return 0; sleep 1; done
  local wpid; wpid=$(port_pid "$1")
  [ -n "$wpid" ] && taskkill /F /PID "$wpid" >/dev/null 2>&1 || true
  sleep 1; ! port_busy "$1"
}
finish() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  stop_pid "$APP_PID"; stop_pid "$BANK_PID"
  # 우리가 띄운 포트만 비운다 (사전 확인에서 멈췄다면 그 포트는 남의 것이다 — free_port 의 taskkill 이 닿으면 안 된다)
  [ "$OWN_APP" = 1 ] && { free_port "$PORT" >/dev/null 2>&1 || true; }
  [ "$OWN_BANK" = 1 ] && { free_port "$BANK_PORT" >/dev/null 2>&1 || true; }
  if [ "$OK" != 1 ] && [ "$STARTED" = 1 ] && [ -f "$OUT" ]; then mv -f "$OUT" "$OUT.invalid"; echo "[$RUN] 확인을 통과하지 못해 $OUT.invalid 로 바꿨다 — 이 회차 숫자는 쓰지 않는다" >&2; fi
  return 0
}
trap finish EXIT
trap 'exit 130' INT TERM

# --- 0. 사전 확인 (읽기만) ---
for p in 18080 18081 "$BANK_PORT"; do
  port_busy "$p" && die "포트 $p 를 이미 쓰고 있다 — 측정용 앱 · 모의 은행을 끈다 (아무것도 건드리지 않았다)"
done
mkdir -p "$RAW" "$WORK"
# 변수가 비면 멈추게 :? 를 쓴다 (빈 변수로 엉뚱한 경로를 지우지 않게)
rm -f "${OUT:?}" "${OUT:?}.invalid" "${RAW:?}/${NAME:?}.k6.json"
STARTED=1
rm -rf "${BANK_FILES:?}"

# --- 1. 정리 ---
echo "[$RUN] 1/6 정리 (측정 사용자 1원인증 행)"
sql < load/verification/cleanup.sql
LEFT=$(echo "SELECT COUNT(*) FROM account_verification WHERE $USERS" | sql)
[ "$LEFT" = 0 ] || die "정리 뒤에도 측정 사용자 행이 $LEFT 건 남았다"

# --- 2. 측정용 모의 은행 ---
echo "[$RUN] 2/6 모의 은행 :$BANK_PORT 지연 ${DELAY}ms"
java -Dstdout.encoding=UTF-8 -cp "$BANK_CP" io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt "$BANK_PORT" \
  --bank-files="$BANK_FILES" --response-delay="${DELAY}ms" > "$WORK/mock-bank.log" 2>&1 &
BANK_PID=$!; OWN_BANK=1
for _ in $(seq 30); do
  grep -q "mock bank listening on" "$WORK/mock-bank.log" && break
  kill -0 "$BANK_PID" 2>/dev/null || die "모의 은행이 기동 중에 끝났다 — $WORK/mock-bank.log"
  sleep 1
done
grep -q "mock bank listening on" "$WORK/mock-bank.log" || die "모의 은행이 30초 안에 뜨지 않았다"

# --- 3. 앱 ---
echo "[$RUN] 3/6 앱 $APP :$PORT 판 $MODE (톰캣 $TOMCAT)"
ARGS=(--server.port="$PORT" --bank.gateway.port="$BANK_PORT" --bank.maintenance.enabled=false
  --account.verification.web-mode="$MODE"
  --management.endpoints.web.exposure.include=health,info,metrics --logging.level.io.github.hyeokjinon.banktransfer=WARN)
if [ "$APP" = api ]; then
  # 개발용 컨슈머 그룹 메시지를 나눠 갖지 않게 · 개발 행을 건드리지 않게 (설계 §6 격리)
  ARGS+=(--spring.kafka.listener.auto-startup=false --transfer.outbox.relay.enabled=false --transfer.reconcile.enabled=false
    --daily-recon.schedule.enabled=false --data-retention.schedule.enabled=false --server.tomcat.threads.max="$TOMCAT")
fi
java -Xms1g -Xmx1g "${VT_JVM_ARGS[@]}" -Dstdout.encoding=UTF-8 -jar "$JAR" "${ARGS[@]}" "${VT_APP_ARGS[@]}" > "$WORK/app.log" 2>&1 &
APP_PID=$!; OWN_APP=1
for _ in $(seq 120); do
  curl -sf "$API/actuator/health" >/dev/null 2>&1 && break
  kill -0 "$APP_PID" 2>/dev/null || die "앱이 기동 중에 끝났다 — $WORK/app.log"
  sleep 1
done
curl -sf "$API/actuator/health" >/dev/null 2>&1 || die "앱이 120초 안에 뜨지 않았다 — $WORK/app.log"
INFO=$(curl -sf "$API/actuator/info") || die "actuator info 를 읽지 못했다"
grep -q "\"web-mode\":\"$MODE\"" <<<"$INFO" || die "앱의 판이 $MODE 가 아니다: $INFO"
grep -qE "\"maximum-pool-size\":\"?20\"?[,}]" <<<"$INFO" || die "앱의 Hikari 가 20 이 아니다: $INFO"
if [ "$APP" = api ]; then grep -qE "\"max\":\"?$TOMCAT\"?[,}]" <<<"$INFO" || die "앱의 톰캣 최대 스레드가 $TOMCAT 이 아니다: $INFO"; fi
if [ "$APP" = api ]; then grep -qE "\"virtual\":\"?$(vt_bool)\"?[,}]" <<<"$INFO" || die "앱의 가상 스레드가 $(vt_bool) 가 아니다: $INFO"; fi
sleep 3   # 게이트웨이가 측정 은행에 붙을 시간 (첫 접속 · 재접속 1초) — 워밍업 첫 요청의 503 을 줄인다 (워밍업은 집계하지 않는다)

# 순차 워밍업 (P7): 새 앱에 동시 요청이 곧바로 몰리면 Java 21 가상 스레드가 첫 직렬화기 생성(synchronized)에서 캐리어를 모두 붙잡아 멈출 수 있다
# (P7 결과 문서 — 냉시동 교착). 정상 상태를 재려고 판 · VT 와 상관없이 1원인증을 한 건씩 3번 보낸다. 정확성 확인은 워밍업 전후 차이로 한다.
WARMUP_N=3
WARMUP_BODY='{"bankCode":"090","accountNo":"1002123456789","holderName":"\ud64d\uae38\ub3d9"}'   # 홍길동 — JSON \u 이스케이프 (본문을 ASCII 로 유지)
printf '%s' "$WARMUP_BODY" > "$WORK/warmup.json"   # 본문은 파일로 — Git Bash(MSYS_NO_PATHCONV=1)에서 명령행 인자로 넘기면 JSON 따옴표가 깨져 400
for w in $(seq "$WARMUP_N"); do
  # -o /dev/null 은 MSYS_NO_PATHCONV=1 에서 Windows curl 이 열지 못한다 (exit 23)
  code=$(curl -s -o "$WORK/warmup-response.json" -m 10 -w '%{http_code}' -H 'Content-Type: application/json' -H "X-User-Id: p62-u00$w" --data-binary @"$WORK/warmup.json" "$API/api/v1/account-verifications") || code=000
  [ "$code" = 201 ] || die "워밍업 $w 번째 요청이 $code (201 이 아니다) — $WORK/warmup-response.json · $WORK/app.log"
done
ROWS0=$(echo "SELECT COUNT(*) FROM account_verification WHERE $USERS" | sql)
journal() { cat "$BANK_FILES"/journal/*.log 2>/dev/null || true; }
ONE_WON0=$(journal | grep -c '^DV' || true)

# --- 4. 표본 ---
# 1초 쉬고 지표 3개를 차례로 읽는다 (정확히 1초 간격이 아니다). 읽는 곳이 측정 대상 앱의 액추에이터라 앱이 포화되면
# 같은 서버를 거쳐 표본이 크게 성겨진다 (P6-2: 포화 칸 1 ~ 5개) — 그래서 curl 에 시간 제한을 둔다. 관리 포트 분리는 범위 밖.
metric() {   # 읽지 못하면 '-' — 빈 값이면 열이 밀린다
  local x; x=$(curl -sf --max-time 1 "$API/actuator/metrics/$1" | grep -o '"value":[-0-9.eE+]*' | head -1 | cut -d: -f2 || true)
  echo "${x:--}"
}
( while :; do echo "$(date +%s%3N) $(metric system.cpu.usage) $(metric process.cpu.usage) $(metric jvm.threads.live)"; sleep 1; done ) > "$WORK/samples.txt" 2>/dev/null &
SAMPLER=$!

# --- 5. k6 ---
echo "[$RUN] 4/6 k6 열린 모델 ${RATE}건/s (워밍업 ${WARMUP_S}s + 측정 ${MEASURE_S}s, 요청 시간 초과 10s)"
K6_START=$(date +%s%3N)
dc --profile load run --rm \
  -e RUN="$NAME" -e BASE_URL="http://host.docker.internal:$PORT" -e RATE="$RATE" \
  -e WARMUP_S="$WARMUP_S" -e MEASURE_S="$MEASURE_S" -e OUT_SUB="$RAW_SUB" \
  k6 run --quiet /scripts/verification/verification.js
kill "$SAMPLER" 2>/dev/null || true; SAMPLER=
K6_JSON="$RAW/$NAME.k6.json"
[ -f "$K6_JSON" ] || die "k6 요약이 없다: $K6_JSON"
mv -f "$K6_JSON" "$WORK/k6.json"
K6_JSON="$WORK/k6.json"
WARNS=$(grep -cE " (WARN|ERROR) " "$WORK/app.log" || true)
PINNED=null
if [ "${VT:-0}" = 1 ]; then PINNED=$(vt_pinned "$PORT" "$WORK/app.jfr" "$WORK/pinned.txt") || die "JFR 덤프 · pinning 집계 실패 — $WORK"; fi
stop_pid "$APP_PID"; APP_PID=; free_port "$PORT" || die "앱이 포트 $PORT 를 놓지 않는다"
stop_pid "$BANK_PID"; BANK_PID=; free_port "$BANK_PORT" || die "모의 은행이 포트 $BANK_PORT 를 놓지 않는다"

# --- 6. 계산 ---
echo "[$RUN] 5/6 계산"
k() { grep -o "\"$1\": *[-0-9.eE+]*" "$K6_JSON" | head -1 | sed 's/.*: *//'; }
# 측정 구간 표본만 (k6 시작 + 워밍업 ~ + 측정). docker run 의 컨테이너 기동만큼(1 ~ 2초, 추정) 앞당겨 잡힌다 — 결과 문서 조건 표에 적는다
S=$(( K6_START + WARMUP_S * 1000 )); E=$(( S + MEASURE_S * 1000 ))
stat() { awk -v s="$S" -v e="$E" -v c="$1" -v mul="$2" '$1 >= s && $1 <= e && $c ~ /^[0-9]/ { x = $c * mul; t += x; n++; if (x > m) m = x } END { if (n) printf "%.1f %.1f %d\n", t / n, m, n; else print "null null 0" }' "$WORK/samples.txt"; }
read -r SYS_AVG SYS_MAX SAMPLES < <(stat 2 100)
read -r APP_AVG APP_MAX _ < <(stat 3 100)
read -r _ THREADS_MAX _ < <(stat 4 1)
[ "${SAMPLES:-0}" -ge 20 ] || echo "[$RUN] 경고: 측정 구간 표본이 ${SAMPLES:-0}개 (< 20)" >&2

# --- 7. 정확성 (스펙과 다른 점 5) ---
echo "[$RUN] 6/6 정확성"
ROWS=$(( $(echo "SELECT COUNT(*) FROM account_verification WHERE $USERS" | sql) - ROWS0 ))
ONE_WON=$(( $(journal | grep -c '^DV' || true) - ${ONE_WON0:-0} ))
CREATED=$(k created_all); TIMEOUTS=$(k timeouts_all)
RMC=$(( ROWS - CREATED )); OMR=$(( ${ONE_WON:-0} - ROWS ))
echo "[$RUN] 201 $CREATED · 행 $ROWS · 1원 ${ONE_WON:-0} · 시간 초과 $TIMEOUTS (행−201 $RMC, 1원−행 $OMR)"
cat > "$OUT" <<EOF
{
  "run": "$NAME", "mode": "$MODE", "app": "$APP", "delay_ms": $DELAY, "rate": $RATE, "tomcat_threads": $TOMCAT, "round": "$ROUND", "warmup": $WARMUP_N, "virtual_threads": $(vt_bool), "pinned_events": $PINNED,
  "rps_201": $(k rps_201), "p50_201": $(k p50_201), "p95_201": $(k p95_201), "p99_201": $(k p99_201),
  "status_201": $(k status_201), "status_4xx": $(k status_4xx), "status_5xx": $(k status_5xx), "timeouts": $(k timeouts),
  "dropped_iterations": $(k dropped_iterations), "vus_max": $(k vus_max),
  "created_all": $CREATED, "timeouts_all": $TIMEOUTS, "rows": $ROWS, "one_won": ${ONE_WON:-0},
  "rows_minus_created": $RMC, "one_won_minus_rows": $OMR,
  "threads_max": $THREADS_MAX, "sys_cpu_avg": $SYS_AVG, "sys_cpu_max": $SYS_MAX, "app_cpu_avg": $APP_AVG, "app_cpu_max": $APP_MAX,
  "samples": $SAMPLES, "app_warn_error_lines": ${WARNS:-0}, "code": "$(git rev-parse --short HEAD)"
}
EOF
if [ "$TIMEOUTS" = 0 ]; then
  { [ "$RMC" = 0 ] && [ "$OMR" = 0 ]; } || die "정확성 확인 실패 — 시간 초과 0 인데 201 $CREATED · 행 $ROWS · 1원 ${ONE_WON:-0} 이 다르다"
else
  { [ "$RMC" -ge 0 ] && [ "$RMC" -le "$TIMEOUTS" ] && [ "$OMR" -ge 0 ] && [ "$OMR" -le "$TIMEOUTS" ]; } \
    || die "정확성 확인 실패 — 시간 초과 $TIMEOUTS 으로 설명되지 않는다 (행−201 $RMC, 1원−행 $OMR)"
  echo "[$RUN] 주의: k6 시간 초과 $TIMEOUTS 건 — 끊김 비용으로 기록 (행−201 $RMC: 끊겨도 저장 / 1원−행 $OMR: 끊겨 1원만)" >&2
fi
OK=1
echo "[$RUN] 끝 — 201 $(k rps_201)건/s · p99 $(k p99_201) ms · 스레드 최대 $THREADS_MAX · pinned $PINNED · 시스템 CPU 평균 $SYS_AVG% → $OUT"
