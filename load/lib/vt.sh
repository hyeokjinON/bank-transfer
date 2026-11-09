# shellcheck shell=bash
# P7 가상 스레드 회차 공통 (설계 §6-3) — load/verification/run.sh · load/consumer/run.sh 가 source 한다.
# VT=1 이면 앱에 --spring.threads.virtual.enabled=true, JVM 에 JFR 녹화(default 설정 = jdk.VirtualThreadPinned 20ms 기준).
# lib/proc.sh 를 먼저 source 해야 한다 (jvm_pid).
# Git Bash 의 kill 은 Windows 프로세스를 바로 끝내(종료 훅 없음) 녹화가 남지 않는다 → 앱을 내리기 전에 vt_pinned 로 덤프 · 집계한다.

vt_validate() {   # $1 = 이 판에 VT=1 을 허용하면 1
  case "${VT:-0}" in
    0) ;;
    1) [ "$1" = 1 ] || { echo "VT=1 은 웹 MVC 판(blocking · async) · 컨슈머 blocking 에만 쓴다 (설계 §6-3)" >&2; exit 2; } ;;
    *) echo "VT 는 0 또는 1: ${VT}" >&2; exit 2 ;;
  esac
}
vt_bool() { if [ "${VT:-0}" = 1 ]; then echo true; else echo false; fi; }
vt_tag() { if [ "${VT:-0}" = 1 ]; then echo "-vt"; fi; }
raw_sub_validate() {   # $1 = RAW_SUB 값
  [[ "$1" =~ ^p[0-9]+(-[0-9]+)?$ ]] || { echo "RAW_SUB 은 p7 · p6-2 처럼: $1" >&2; exit 2; }
}
# 배열 VT_JVM_ARGS · VT_APP_ARGS 를 채운다 (VT=0 이면 빈 배열). $1 = JFR 파일 (저장소 루트 기준 상대 경로)
vt_args() {
  VT_JVM_ARGS=(); VT_APP_ARGS=()
  [ "${VT:-0}" = 1 ] || return 0
  VT_JVM_ARGS=("-XX:StartFlightRecording=name=p7,settings=default,filename=$1")
  VT_APP_ARGS=(--spring.threads.virtual.enabled=true)
}
# 포트 $1 의 앱 JFR 을 $2 로 덤프하고 jdk.VirtualThreadPinned 수를 표준출력에. 0 이 아니면 스택(맨 위 8줄)을 $3 에. 실패하면 return 1
vt_pinned() {
  local port=$1 file=$2 stacks=$3 bin wpid n summary
  bin=$(dirname "$(command -v java)")
  wpid=$(jvm_pid "--server.port=$port")
  [ -n "$wpid" ] || { echo "vt_pinned: 포트 $port 의 앱 PID 를 jcmd -l 에서 찾지 못했다" >&2; return 1; }
  "$bin/jcmd" "$wpid" JFR.dump name=p7 filename="$(pwd -W)/$file" >/dev/null || return 1
  [ -s "$file" ] || { echo "vt_pinned: 덤프 파일이 없다: $file" >&2; return 1; }
  summary=$("$bin/jfr" summary "$file") || { echo "vt_pinned: jfr summary 실패" >&2; return 1; }
  grep -q "jdk\." <<<"$summary" || { echo "vt_pinned: jfr summary 출력이 이상하다" >&2; return 1; }
  n=$(tr -d '\r' <<<"$summary" | awk '$1 == "jdk.VirtualThreadPinned" { print $2 }')
  n=${n:-0}
  [[ "$n" =~ ^[0-9]+$ ]] || { echo "vt_pinned: 이벤트 수를 읽지 못했다: $n" >&2; return 1; }
  if [ "$n" != 0 ]; then "$bin/jfr" print --events jdk.VirtualThreadPinned --stack-depth 8 "$file" > "$stacks" || echo "vt_pinned: 스택 출력 실패 (수는 기록함)" >&2; fi
  echo "$n"
}
