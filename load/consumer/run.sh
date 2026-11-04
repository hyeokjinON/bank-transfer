#!/usr/bin/env bash
# P6-1 컨슈머 측정 1회 (설계 §6): 정리 · 시드 → 측정 그룹 오프셋을 끝으로 → ① 발행만 → ② 처리만 → 계산(DB 시각) · 정확성 확인.
# 결과: docs/perf/raw/p6-1/<이름>.json (이름 = blocking-d<지연>-h<hikari>-<회차> | coroutine-d<지연>-c<상한>-h<hikari>-<회차>).
#       정확성 확인을 통과하기 전에 끝나면(어긋남 · 시간 초과 · Ctrl+C · 오류) .json.invalid 로 바꾼다 — summarize.sh 는 .json 만 읽는다.
#       로그 · CPU 표본은 build/load-p61/<이름>/ (git 제외).
#
# 사용 (Git Bash, 저장소 루트):
#   bash load/consumer/run.sh <blocking|coroutine> <지연ms 0~2999> <회차> [상한=64] [hikari=20]
#   bash load/consumer/run.sh cleanup      # 측정이 끝나면 반드시 (아래 경고)
# 먼저 (OPERATION §11): infra up, ./gradlew :apps:api:bootJar :apps:mock-bank:installDist.
#   포트 8080 · 9190 을 쓰는 프로세스와 다른 API 인스턴스(IntelliJ 등)는 끈다 — 같은 DB 의 측정 출금을 건드리거나 메시지를 나눠 가진다.
#   이 스크립트가 API 와 모의 은행을 직접 띄우고 내린다 (모의 은행은 회차마다 새 거래 기록 디렉터리).
# 경고: 측정 데이터는 사용자 load-p61-u001 ~ u100 으로만 만들고 지운다. 회차가 끝나면 그 출금은 COMPLETED 로 남는다 —
#       지우지 않고 두면 다음 날 개발용 API 의 일마감 대사가 "은행 파일에 없는 COMPLETED" 로 잡는다. 끝나면 cleanup.
# 환경변수: N(건수 — 기본은 설계 §6 표) TIMEOUT(처리 대기 최대 초, 900) API_PORT(8080) BANK_PORT(9190)
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /opt/... 를 Windows 경로로 바꾸지 않게 (그래서 taskkill 도 /F 로 쓴다)
cd "$(dirname "$0")/../.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호는 mysql 컨테이너가 이미 가진 환경변수를 컨테이너 안 셸이 읽는다 — PC 쪽 명령행에 나타나지 않는다 (load/run.sh 와 같다)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
rootsql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -N'; }
kgroups() { dc exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "$@"; }
RUN=consumer
die() { echo "[$RUN] $*" >&2; exit 1; }

if [ "${1:-}" = cleanup ]; then
  sql < load/consumer/cleanup.sql
  echo "cleanup 완료 (측정 사용자 load-p61-u001 ~ u100 의 지갑 · 원장 · 출금 · 아웃박스 · 인증)"
  exit 0
fi

MODE=${1:?모드: blocking | coroutine}
DELAY=${2:?은행 지연 ms: 0 ~ 2999}
ROUND=${3:?회차: 1, 2, 3, sweep1 …}
CAP=${4:-64}
HIKARI=${5:-20}
case "$MODE" in blocking|coroutine) ;; *) echo "모르는 모드: $MODE" >&2; exit 2 ;; esac
{ [[ "$DELAY" =~ ^[0-9]{1,4}$ ]] && [ "$DELAY" -le 2999 ]; } || { echo "지연은 0 ~ 2999 (ms): $DELAY" >&2; exit 2; }
[[ "$ROUND" =~ ^[A-Za-z0-9_-]{1,16}$ ]] || { echo "회차는 영문 · 숫자 · - _ 16자 이내: $ROUND" >&2; exit 2; }
{ [[ "$CAP" =~ ^[0-9]{1,4}$ ]] && [ "$CAP" -ge 1 ] && [ "$CAP" -le 1024 ]; } || { echo "상한은 1 ~ 1024: $CAP" >&2; exit 2; }
{ [[ "$HIKARI" =~ ^[0-9]{1,3}$ ]] && [ "$HIKARI" -ge 1 ]; } || { echo "hikari 는 1 ~ 999: $HIKARI" >&2; exit 2; }
if [ -z "${N:-}" ]; then
  # 설계 §6: 칸마다 20초 이상 걸리게 (예상 처리량 = 동시에 은행을 기다리는 수 ÷ 지연). 대조 실험(coroutine 상한 3)은 blocking 과 같은 N
  KIND=$MODE; { [ "$MODE" = coroutine ] && [ "$CAP" -le 3 ]; } && KIND=blocking
  case "$KIND-$DELAY" in blocking-100) N=1500 ;; blocking-500) N=400 ;; *) N=5000 ;; esac
fi
{ [[ "$N" =~ ^[0-9]{1,6}$ ]] && [ "$N" -ge 1 ] && [ "$N" -le 100000 ]; } || { echo "N 은 1 ~ 100000: $N" >&2; exit 2; }
TIMEOUT=${TIMEOUT:-900}
API_PORT=${API_PORT:-8080}
BANK_PORT=${BANK_PORT:-9190}
API="http://localhost:$API_PORT"
GROUP=transfer-processor-p61   # 측정 전용 그룹: 오프셋을 옮겨도 개발용 그룹(transfer-processor)의 밀린 메시지를 건너뛰지 않는다
if [ "$MODE" = blocking ]; then NAME="blocking-d$DELAY-h$HIKARI-$ROUND"; else NAME="coroutine-d$DELAY-c$CAP-h$HIKARI-$ROUND"; fi
RUN=$NAME
RAW=docs/perf/raw/p6-1
OUT="$RAW/$NAME.json"
WORK="build/load-p61/$NAME"
BANK_FILES="$WORK/bank-files"   # 개발용 ./bank-files 와 따로 — 측정 출금 수천 건이 다음 날 대사에 섞이지 않게 (설계 §6)
JAR=apps/api/build/libs/api-0.1.0-SNAPSHOT.jar
BANK_CP="apps/mock-bank/build/install/mock-bank/lib/*"
[ -f "$JAR" ] || die "API jar 가 없다 — ./gradlew :apps:api:bootJar"
ls apps/mock-bank/build/install/mock-bank/lib/*.jar >/dev/null 2>&1 || die "모의 은행 배포본이 없다 — ./gradlew :apps:mock-bank:installDist"

# --- 프로세스 · 정리 ---
API_PID=; BANK_PID=; SAMPLERS=()
OK=0
port_busy() { netstat -ano 2>/dev/null | grep -E "TCP +[^ ]+:$1 +[^ ]+ +LISTENING" >/dev/null; }
stop_pid() { [ -n "${1:-}" ] || return 0; kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }
free_port() {   # 그 포트를 쥔 프로세스가 끝날 때까지 (최대 15초). kill 이 듣지 않았으면 Windows PID 로 강제 종료
  for _ in $(seq 15); do port_busy "$1" || return 0; sleep 1; done
  local wpid; wpid=$(netstat -ano | grep -E "TCP +[^ ]+:$1 +[^ ]+ +LISTENING" | awk '{ print $NF }' | head -1)
  [ -n "$wpid" ] && taskkill /F /PID "$wpid" >/dev/null 2>&1 || true
  sleep 1; ! port_busy "$1"
}
finish() {
  for p in "${SAMPLERS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  stop_pid "$API_PID"; stop_pid "$BANK_PID"
  if [ "$OK" != 1 ] && [ -f "$OUT" ]; then mv -f "$OUT" "$OUT.invalid"; echo "[$RUN] 확인을 통과하지 못해 $OUT.invalid 로 바꿨다 — 이 회차 숫자는 쓰지 않는다" >&2; fi
  return 0
}
trap finish EXIT
trap 'exit 130' INT TERM

COMMON=(--server.port="$API_PORT" --bank.gateway.port="$BANK_PORT" --bank.maintenance.enabled=false
  --transfer.reconcile.enabled=false --daily-recon.schedule.enabled=false --data-retention.schedule.enabled=false
  --spring.kafka.consumer.group-id="$GROUP" --logging.level.io.github.hyeokjinon.banktransfer=WARN)
start_api() {   # $1 = 로그 파일, 나머지 = 추가 인자
  local log=$1; shift
  java -Xms1g -Xmx1g -Dstdout.encoding=UTF-8 -jar "$JAR" "${COMMON[@]}" "$@" > "$log" 2>&1 &
  API_PID=$!
  for _ in $(seq 120); do
    curl -sf "$API/actuator/health" >/dev/null 2>&1 && return 0
    kill -0 "$API_PID" 2>/dev/null || die "API 가 기동 중에 끝났다 — $log"
    sleep 1
  done
  die "API 가 120초 안에 뜨지 않았다 — $log"
}
stop_api() { stop_pid "$API_PID"; API_PID=; free_port "$API_PORT" || die "API 가 포트 $API_PORT 를 놓지 않는다"; }

# --- 0. 먼저 확인 ---
port_busy "$API_PORT" && die "포트 $API_PORT 를 이미 쓰고 있다 — 다른 API 인스턴스(IntelliJ 등)를 끈다"
port_busy "$BANK_PORT" && die "포트 $BANK_PORT 를 이미 쓰고 있다"
for g in transfer-processor "$GROUP"; do
  # 앞 회차의 API 를 강제로 내렸으면 Kafka 가 그 컨슈머를 세션 시간 초과(기본 45초)로 내보낼 때까지 멤버가 남는다 — 최대 90초 기다린다
  for _ in $(seq 90); do
    members=$(kgroups --describe --group "$g" --members 2>&1 || true)
    grep -qE "has no active members|does not exist|GroupIdNotFoundException|not found" <<<"$members" && break
    sleep 1
  done
  grep -qE "has no active members|does not exist|GroupIdNotFoundException|not found" <<<"$members" || die "그룹 $g 에 붙은 컨슈머가 있다 — 다른 API 인스턴스를 끈다: $members"
done
mkdir -p "$RAW" "$WORK"
rm -f "$OUT" "$OUT.invalid"
rm -rf "$BANK_FILES"

# --- 1. 정리 · 시드 ---
echo "[$RUN] 1/6 정리 · 시드 (N=$N)"
sql < load/consumer/cleanup.sql
{ echo "SET @n := $N;"; cat load/consumer/seed.sql; } | sql
H=0
for _ in $(seq 60); do
  # 지운 행의 InnoDB 정리(purge)가 측정과 겹치지 않게 (load/run.sh 와 같다). INNODB_METRICS 는 PROCESS 권한이라 root 로 읽는다
  H=$(echo "SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME = 'trx_rseg_history_len'" | rootsql)
  [ "${H:-0}" -lt 1000 ] && break
  sleep 1
done
[ "${H:-0}" -lt 1000 ] || echo "[$RUN] 경고: 60초가 지나도 InnoDB 정리가 끝나지 않았다 (history length=$H)" >&2

# --- 2. 측정 그룹 오프셋을 끝으로 ---
echo "[$RUN] 2/6 측정 그룹 $GROUP 의 오프셋을 끝으로 (원래 토픽 · 재시도 토픽 · DLT — 이전 회차 메시지가 섞이지 않게)"
TOPICS=$(dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | tr -d '\r')
reset_offsets() {   # $1 그룹 $2 토픽. 토픽이 아직 없으면(처음 띄운 인프라) 옛 메시지도 없다
  grep -qx "$2" <<<"$TOPICS" || return 0
  kgroups --group "$1" --topic "$2" --reset-offsets --to-latest --execute >/dev/null
}
reset_offsets "$GROUP" transfer.requested
for i in 0 1 2; do reset_offsets "$GROUP-retry-$i" "transfer.requested-retry-$i"; done
reset_offsets "$GROUP-dlt" transfer.requested-dlt

# --- 3. 발행만 ---
echo "[$RUN] 3/6 발행만 (리스너 끔)"
start_api "$WORK/api-publish.log" --spring.kafka.listener.auto-startup=false \
  --transfer.outbox.relay.enabled=true --transfer.outbox.relay.batch-size=500 --transfer.outbox.relay.interval-ms=100
PENDING_SQL="SELECT COUNT(*) FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id WHERE t.user_id LIKE 'load-p61-u%' AND o.published_at IS NULL"
P=-1
for _ in $(seq 300); do P=$(echo "$PENDING_SQL" | sql); [ "$P" = 0 ] && break; sleep 1; done
[ "$P" = 0 ] || die "300초 안에 발행이 끝나지 않았다 (남은 $P) — $WORK/api-publish.log"
stop_api

# --- 4. 처리만 ---
echo "[$RUN] 4/6 처리만 — 모의 은행 지연 ${DELAY}ms, $MODE (상한 $CAP · Hikari $HIKARI)"
java -Dstdout.encoding=UTF-8 -cp "$BANK_CP" io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt "$BANK_PORT" \
  --bank-files="$BANK_FILES" --response-delay="${DELAY}ms" > "$WORK/mock-bank.log" 2>&1 &
BANK_PID=$!
for _ in $(seq 30); do
  grep -q "mock bank listening on" "$WORK/mock-bank.log" && break
  kill -0 "$BANK_PID" 2>/dev/null || die "모의 은행이 기동 중에 끝났다 — $WORK/mock-bank.log"
  sleep 1
done
grep -q "mock bank listening on" "$WORK/mock-bank.log" || die "모의 은행이 30초 안에 뜨지 않았다"
start_api "$WORK/api-process.log" --transfer.outbox.relay.enabled=false \
  --transfer.consumer.mode="$MODE" --transfer.consumer.coroutine.max-in-flight-bank-calls="$CAP" \
  --spring.datasource.hikari.maximum-pool-size="$HIKARI" \
  --management.endpoints.web.exposure.include=health,info,metrics
INFO=$(curl -sf "$API/actuator/info") || die "actuator info 를 읽지 못했다"
grep -q "\"mode\":\"$MODE\"" <<<"$INFO" || die "앱의 모드가 $MODE 가 아니다: $INFO"
grep -qE "\"max-in-flight-bank-calls\":\"?$CAP\"?[,}]" <<<"$INFO" || die "앱의 상한이 $CAP 이 아니다: $INFO"
grep -qE "\"maximum-pool-size\":\"?$HIKARI\"?[,}]" <<<"$INFO" || die "앱의 Hikari 가 $HIKARI 가 아니다: $INFO"

metric() { curl -sf "$API/actuator/metrics/$1" | grep -o '"value":[-0-9.eE+]*' | head -1 | cut -d: -f2; }
( while :; do echo "$(date +%s%3N) $(metric system.cpu.usage) $(metric process.cpu.usage)"; sleep 1; done ) > "$WORK/cpu.txt" 2>/dev/null &
SAMPLERS+=($!)
MYSQL_NAME=$(dc ps --format '{{.Name}}' mysql)
KAFKA_NAME=$(dc ps --format '{{.Name}}' kafka)
# docker stats --no-stream 은 한 번에 1 ~ 2초 걸린다 — 쉬지 않고 이어 부른다
( while :; do T=$(date +%s%3N); docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' "$MYSQL_NAME" "$KAFKA_NAME" | sed "s/^/$T /"; done ) > "$WORK/docker.txt" 2>/dev/null &
SAMPLERS+=($!)

LEFT_SQL="SELECT COUNT(*) FROM transfer WHERE user_id LIKE 'load-p61-u%' AND status IN ('RECEIVED', 'SENDING')"
START=$(date +%s)
while :; do
  L=$(echo "$LEFT_SQL" | sql)
  [ "$L" = 0 ] && break
  [ $(( $(date +%s) - START )) -lt "$TIMEOUT" ] || die "${TIMEOUT}초 안에 끝나지 않았다 (남은 $L) — $WORK/api-process.log"
  kill -0 "$API_PID" 2>/dev/null || die "API 가 처리 중에 끝났다 — $WORK/api-process.log"
  sleep 1
done
sleep 2   # 마지막 표본
for p in "${SAMPLERS[@]}"; do kill "$p" 2>/dev/null || true; done
SAMPLERS=()
WARNS=$(grep -cE " (WARN|ERROR) " "$WORK/api-process.log" || true)
stop_api
stop_pid "$BANK_PID"; BANK_PID=

# --- 5. 계산 (DB 시각) ---
echo "[$RUN] 5/6 계산 (DB 시각)"
STATS=$(sql < load/consumer/stats.sql)
v() { awk -v k="$1" '$1 == k { print $2 }' <<<"$STATS"; }
START_MS=$(v start_ms); END_MS=$(v end_ms)
DURATION=$(awk -v s="$START_MS" -v e="$END_MS" 'BEGIN { printf "%.3f", (e - s) / 1000 }')
TPS=$(awk -v n="$N" -v d="$DURATION" 'BEGIN { printf "%.1f", (d > 0) ? n / d : 0 }')
# CPU 표본은 처리 구간(첫 sending_at ~ 마지막 updated_at) 안의 것만. Actuator 값은 0 ~ 1 → %, docker 는 이미 % (100% = 코어 1개)
cpu_stat() { awk -v s="$START_MS" -v e="$END_MS" -v c="$1" '$1 >= s && $1 <= e && $c != "" { x = $c * 100; t += x; n++; if (x > m) m = x } END { if (n) printf "%.1f %.1f %d\n", t / n, m, n; else print "null null 0" }' "$WORK/cpu.txt"; }
docker_stat() { awk -v s="$START_MS" -v e="$END_MS" -v name="$1" '$1 >= s && $1 <= e && $2 == name { x = $3; sub("%", "", x); x += 0; t += x; n++; if (x > m) m = x } END { if (n) printf "%.1f %.1f\n", t / n, m; else print "null null" }' "$WORK/docker.txt"; }
read -r SYS_AVG SYS_MAX SAMPLES < <(cpu_stat 2)
read -r API_AVG API_MAX _ < <(cpu_stat 3)
read -r MYSQL_AVG MYSQL_MAX < <(docker_stat "$MYSQL_NAME")
read -r KAFKA_AVG KAFKA_MAX < <(docker_stat "$KAFKA_NAME")
BANK_PEAK=$(grep -o 'mock bank peak pending responses: [0-9]*' "$WORK/mock-bank.log" | tail -1 | grep -o '[0-9]*$' || true)
cat > "$OUT" <<EOF
{
  "run": "$NAME", "mode": "$MODE", "delay_ms": $DELAY, "round": "$ROUND", "cap": $CAP, "hikari": $HIKARI, "n": $N,
  "seconds": $DURATION, "tps": $TPS, "p50_ms": $(v p50_ms), "p95_ms": $(v p95_ms),
  "sys_cpu_avg": $SYS_AVG, "sys_cpu_max": $SYS_MAX, "api_cpu_avg": $API_AVG, "api_cpu_max": $API_MAX,
  "mysql_cpu_avg": $MYSQL_AVG, "mysql_cpu_max": $MYSQL_MAX, "kafka_cpu_avg": $KAFKA_AVG, "kafka_cpu_max": $KAFKA_MAX,
  "cpu_samples": $SAMPLES, "bank_peak": ${BANK_PEAK:-0}, "api_warn_error_lines": ${WARNS:-0}, "code": "$(git rev-parse --short HEAD)"
}
EOF

# --- 6. 정확성 ---
echo "[$RUN] 6/6 정확성"
RESULT=$(sql < load/consumer/verify.sql)
echo "$RESULT"
BAD=$(awk '$1 != "transfers" && $2 != 0' <<<"$RESULT")
TRANSFERS=$(awk '$1 == "transfers" { print $2 }' <<<"$RESULT")
# 거래 기록 한 줄 = "D" + 거래번호 20자 + … → 측정 출금(P61…)이 정확히 N 줄, 같은 거래번호가 두 줄인 것은 0 (은행에 두 번 간 출금 없음)
journal() { cat "$BANK_FILES"/journal/*.log 2>/dev/null || true; }   # 기록이 없으면 빈 출력 (pipefail 로 여기서 끝나지 않게)
JOURNAL_LINES=$(journal | grep -c '^DP61' || true)
JOURNAL_DUP=$(journal | cut -c2-21 | sort | uniq -d | wc -l | tr -d ' ')
echo "journal_p61_lines ${JOURNAL_LINES:-0} (N=$N) · journal_duplicate_refs $JOURNAL_DUP"
if [ -n "$BAD" ] || [ "$TRANSFERS" != "$N" ] || [ "${JOURNAL_LINES:-0}" != "$N" ] || [ "$JOURNAL_DUP" != 0 ]; then
  die "정확성 확인 실패 — 이 회차 숫자는 버리고 원인을 찾는다 (출금 $TRANSFERS / N $N, 은행 기록 ${JOURNAL_LINES:-0}, 두 번 간 거래번호 $JOURNAL_DUP)"
fi
OK=1
if awk -v d="$DURATION" 'BEGIN { exit !(d < 20) }'; then
  echo "[$RUN] 주의: 처리 시간 ${DURATION}초 < 20초 — 설계 §6: 이 칸은 N 을 늘려 다시 잰다 (OPERATION §11-2)" >&2
fi
echo "[$RUN] 끝 — $TPS 건/s · p50 $(v p50_ms) ms · p95 $(v p95_ms) ms · 시스템 CPU 평균 $SYS_AVG% · 은행 최대 동시 ${BANK_PEAK:-0} → $OUT"
