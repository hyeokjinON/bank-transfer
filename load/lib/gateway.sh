# shellcheck shell=bash
# 측정용 게이트웨이 앱 (P8-1) — load/consumer/run.sh · load/verification/run.sh 가 source 한다 (lib/proc.sh 다음).
# 쓰는 변수: GATEWAY(direct|http, 기본 direct) SERVER(reactive|servlet) CLIENT(webclient|jdk) GW_POOL(WebClient 최대 연결) NET_DELAY(Toxiproxy 지연 ms)
#            BANK_PORT(측정용 모의 은행) — run.sh 가 정한다. 쓰는 함수: die · stop_pid · free_port (run.sh).
GW_PORT=18082
GW_JAR=apps/bank-gateway/build/libs/bank-gateway-0.1.0-SNAPSHOT.jar
GW_PID=
TOXI=http://localhost:8474
TOXI_PROXY=p81-gateway

gw_on() { [ "${GATEWAY:-direct}" = http ]; }

gw_validate() {
  case "${GATEWAY:-direct}" in direct|http) ;; *) die "GATEWAY 는 direct | http: '${GATEWAY}'";; esac
  case "${SERVER:-reactive}" in reactive|servlet) ;; *) die "SERVER 는 reactive | servlet: '${SERVER}'";; esac
  case "${CLIENT:-webclient}" in webclient|jdk) ;; *) die "CLIENT 는 webclient | jdk: '${CLIENT}'";; esac
  [ -z "${GW_POOL:-}" ] || [[ "$GW_POOL" =~ ^[1-9][0-9]*$ ]] || die "GW_POOL 은 양의 정수: '$GW_POOL'"
  [ -z "${NET_DELAY:-}" ] || [[ "$NET_DELAY" =~ ^[0-9]+$ ]] || die "NET_DELAY 는 ms 정수: '$NET_DELAY'"
  if ! gw_on && { [ -n "${SERVER:-}${CLIENT:-}${GW_POOL:-}${NET_DELAY:-}" ]; }; then die "SERVER · CLIENT · GW_POOL · NET_DELAY 는 GATEWAY=http 일 때만"; fi
  if gw_on; then
    [ -f "$GW_JAR" ] || die "게이트웨이 jar 가 없다 — ./gradlew :apps:bank-gateway:bootJar"
    port_busy "$GW_PORT" && die "포트 $GW_PORT 를 이미 쓰고 있다 (측정용 게이트웨이)"
    if [ -n "${NET_DELAY:-}" ]; then curl -fs "$TOXI/version" >/dev/null || die "Toxiproxy 가 없다 — docker compose ... --profile chaos up -d toxiproxy"; fi
  fi
  return 0
}

# 원자료 이름 접미사. direct 면 빈 문자열 (P6 · P7 원자료 이름과 같게)
gw_tag() { gw_on || return 0; printf -- '-gw-%s-%s%s%s' "${SERVER:-reactive}" "${CLIENT:-webclient}" "${GW_POOL:+-p$GW_POOL}" "${NET_DELAY:+-n${NET_DELAY}ms}"; }

gw_url() { if [ -n "${NET_DELAY:-}" ]; then echo "http://localhost:28082"; else echo "http://localhost:$GW_PORT"; fi; }

# API 에 더할 인자 (한 줄에 하나). direct 면 아무것도 없다
gw_api_args() {
  gw_on || return 0
  printf '%s\n' --bank.gateway.mode=http --bank.gateway.http.base-url="$(gw_url)" --bank.gateway.http.client="${CLIENT:-webclient}"
  [ -z "${GW_POOL:-}" ] || printf '%s\n' --bank.gateway.http.max-connections="$GW_POOL"
}

gw_start() {   # $1 = 로그 파일
  gw_on || return 0
  java -Xms512m -Xmx512m -Dstdout.encoding=UTF-8 -jar "$GW_JAR" --server.port="$GW_PORT" \
    --bank.gateway.port="$BANK_PORT" --bank.maintenance.enabled=false \
    --spring.main.web-application-type="${SERVER:-reactive}" > "$1" 2>&1 &
  GW_PID=$!
  local i
  for i in $(seq 1 120); do
    curl -fs "localhost:$GW_PORT/actuator/health/bankConnection" 2>/dev/null | grep -q '"UP"' && break
    kill -0 "$GW_PID" 2>/dev/null || die "게이트웨이가 기동 중에 끝났다 — $1"
    sleep 0.5
  done
  curl -fs "localhost:$GW_PORT/actuator/health/bankConnection" | grep -q '"UP"' || die "게이트웨이가 은행에 붙지 못했다 — $1"
  [ -z "${NET_DELAY:-}" ] || toxi_setup
}

# 이 실행이 띄운 게이트웨이(GW_PID)만 내린다 — 사전 확인에서 멈춘 경우 남의 18082 프로세스를 건드리지 않게.
# 끝내지 못하면 return 1 (finish 트랩 안에서 exit 하지 않게 die 를 쓰지 않는다 — 정상 흐름은 호출 쪽이 || die)
gw_stop() {
  gw_on || return 0
  [ -n "$GW_PID" ] || return 0
  [ -z "${NET_DELAY:-}" ] || toxi_clear
  stop_pid "$GW_PID"; GW_PID=
  free_port "$GW_PORT" || { echo "[$RUN] 게이트웨이가 포트 $GW_PORT 를 놓지 않는다" >&2; return 1; }
}

# 응답 방향(downstream)에 NET_DELAY ms — 한 번 왕복에 그만큼 더한다
toxi_setup() {
  curl -fs -X DELETE "$TOXI/proxies/$TOXI_PROXY" >/dev/null 2>&1 || true
  curl -fs -X POST "$TOXI/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$TOXI_PROXY\",\"listen\":\"0.0.0.0:28082\",\"upstream\":\"host.docker.internal:$GW_PORT\",\"enabled\":true}" >/dev/null \
    || die "Toxiproxy 프록시를 만들지 못했다"
  curl -fs -X POST "$TOXI/proxies/$TOXI_PROXY/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$NET_DELAY}}" >/dev/null \
    || die "지연 toxic 을 만들지 못했다"
}

toxi_clear() { curl -fs -X DELETE "$TOXI/proxies/$TOXI_PROXY" >/dev/null 2>&1 || true; }

# 게이트웨이 지표 한 줄: "<ms> <process.cpu.usage> <jvm.threads.live>" (읽지 못하면 '-' — 빈 값이면 열이 밀린다)
gw_metric() { local x; x=$(curl -fs --max-time 1 "localhost:$GW_PORT/actuator/metrics/$1" 2>/dev/null | grep -o '"value":[-0-9.eE+]*' | head -1 | cut -d: -f2 || true); echo "${x:--}"; }
gw_sample_line() { echo "$(date +%s%3N) $(gw_metric process.cpu.usage) $(gw_metric jvm.threads.live)"; }

# 게이트웨이 스레드 덤프 1회 (판 비교용). jcmd 는 Windows PID 가 필요하다 — 명령행의 --server.port 로 찾는다 (lib/proc.sh)
gw_dump() {   # $1 = 출력 파일
  gw_on || return 0
  local b pid; b=$(jdk_bin) || return 0; pid=$(jvm_pid "--server.port=$GW_PORT")
  rm -f "$1"   # dump_to_file 은 있는 파일에 쓰지 않는다
  [ -n "$pid" ] && "$b/jcmd" "$pid" Thread.dump_to_file -format=text "$(pwd -W)/$1" >/dev/null 2>&1 || true
}
