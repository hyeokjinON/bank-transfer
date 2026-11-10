#!/usr/bin/env bash
# P6-1 측정 결과를 중앙값 표(마크다운)로 만든다 → docs/perf/p6-1-consumer-coroutines.md 에 붙인다.
# 사용: bash load/consumer/summarize.sh     (docs/perf/raw/p6-1/*.json — .invalid 제외)
#       RAW_SUB=p8-1 bash load/consumer/summarize.sh   (P8-1: 게이트웨이 · gw CPU · gw 스레드 열이 더해진다 — 그 폴더를 읽을 때만. P6 · P7 표 모양은 그대로)
# 표 ① 비교: 회차가 숫자인 것. 표 ② 용량 탐색: 회차가 sweep 으로 시작하는 것. 그 밖(drift · smoke)은 넣지 않는다.
# 지표마다 따로 중앙값을 낸다 (한 회차의 값 묶음이 아니다). 편차 = 처리량 · p95 중 중앙값에서 가장 먼 회차의 차이(%) — 20% 이상이면 조건을 다시 확인한다 (P4-2 와 같다).
# 예상(리틀의 법칙) = 동시에 은행을 기다리는 수 ÷ 지연: blocking 3, coroutine min(상한, 150 = 컨슈머 3 × poll 50). 지연 0 은 계산하지 않는다.
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=${RAW_DIR:-docs/perf/raw/${RAW_SUB:-p6-1}}   # RAW_DIR 은 시험용
case "$RAW" in */p8-1*) GWCOLS=1 ;; *) GWCOLS=0 ;; esac

num() { grep -o "\"$2\": *[-0-9.eE+]*" "$1" | head -1 | sed 's/.*: *//'; }
str() { grep -o "\"$2\": *\"[^\"]*\"" "$1" | head -1 | sed 's/.*: *"//; s/"$//'; }
# 숫자가 아닌 값(JSON null → num 이 빈 값을 낸다 · '-')은 건너뛴다. 값이 하나도 없으면 '-'
median() { awk '/^[-+]?[0-9]/' | sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "-"; else print v[int((NR + 1) / 2)] }'; }
fmt() { if [[ "$1" =~ ^[-+]?[0-9] ]]; then printf "%.${2}f" "$1"; else printf -- "-"; fi; }   # $1 = 값, $2 = 소수 자릿수
maxdev() { awk '/^[-+]?[0-9]/' | awk -v m="$1" '{ d = (m == 0) ? 0 : ($1 - m) / m * 100; if (d < 0) d = -d; if (d > x) x = d } END { printf "%.0f", x }'; }
gwname() { local g; g=$(str "$1" gateway || true); echo "${g:-direct}"; }   # 없으면(P6 · P7 원자료) direct
key() { echo "$(str "$1" mode) $(num "$1" delay_ms) $(num "$1" cap) $(num "$1" hikari) $(gwname "$1")"; }

table() {   # $1 = 회차 정규식, $2 = 제목
  local files=() f
  for f in "$RAW"/*.json; do
    [ -f "$f" ] || continue
    grep -q '"tps":' "$f" || continue   # 컨슈머 원자료만 (P8-1 은 웹 1원인증 원자료가 같은 폴더에 있다)
    [[ "$(str "$f" round)" =~ $1 ]] && files+=("$f")
  done
  echo
  echo "### $2"
  echo
  local gh="" gw2=""
  [ "$GWCOLS" = 1 ] && { gh="| 게이트웨이 "; gw2=" gw CPU 평균 / 최대 (%) | gw 스레드 최대 |"; }
  echo "| 모드 $gh| 지연 (ms) | 상한 | Hikari | N | 회차 | 처리량 (건/s) | 예상 (건/s) | p50 (ms) | p95 (ms) | 시스템 CPU 평균 / 최대 (%) | CPU 표본 | API CPU 평균 (%) | MySQL CPU 평균 (%) | 은행 최대 동시 | 편차 |$gw2"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|$([ "$GWCOLS" = 1 ] && echo "---|---|---|")"
  [ ${#files[@]} -gt 0 ] || return 0
  # 파이프 뒤 while 은 하위 셸이라 local 을 쓰지 않는다 (변수는 그 안에서만 산다)
  for f in "${files[@]}"; do key "$f"; done | sort -u -k1,1 -k2,2n -k3,3n -k4,4n -k5,5 | while read -r mode delay cap hikari gw; do
    group=()
    for g in "${files[@]}"; do [ "$(key "$g")" = "$mode $delay $cap $hikari $gw" ] && group+=("$g"); done
    med() { for g in "${group[@]}"; do num "$g" "$1"; done | median; }
    tps=$(med tps); p95=$(med p95_ms)
    if [ "$mode" = blocking ]; then conc=3; capcol="-"; else conc=$(( cap < 150 ? cap : 150 )); capcol=$cap; fi
    if [ "$delay" = 0 ]; then expect="-"; else expect=$(awk -v c="$conc" -v d="$delay" 'BEGIN { printf "%.0f", c * 1000 / d }'); fi
    d1=$(for g in "${group[@]}"; do num "$g" tps; done | maxdev "$tps")
    d2=$(for g in "${group[@]}"; do num "$g" p95_ms; done | maxdev "$p95")
    smp=$(med cpu_samples); smpcol=$(fmt "$smp" 0); [[ "$smp" =~ ^[0-9]+$ ]] && [ "$smp" -lt 10 ] && smpcol="$smpcol (<10)"
    gwcells=""; gwc1=""
    if [ "$GWCOLS" = 1 ]; then gwc1="| $gw "; gwcells=" $(fmt "$(med gw_cpu_avg)" 1) / $(fmt "$(med gw_cpu_max)" 1) | $(fmt "$(med gw_threads_max)" 0) |"; fi
    echo "| $mode $gwc1| $delay | $capcol | $hikari | $(fmt "$(med n)" 0) | ${#group[@]} | $(fmt "$tps" 1) | $expect | $(fmt "$(med p50_ms)" 1) | $(fmt "$p95" 1) | $(fmt "$(med sys_cpu_avg)" 1) / $(fmt "$(med sys_cpu_max)" 1) | $smpcol | $(fmt "$(med api_cpu_avg)" 1) | $(fmt "$(med mysql_cpu_avg)" 1) | $(fmt "$(med bank_peak)" 0) | $(( d1 > d2 ? d1 : d2 ))% |$gwcells"
  done
}

table '^[0-9]+$' "① 비교 — 모드 × 은행 지연 (대조: coroutine 상한 3)"
table '^sweep[0-9]+$' "② 용량 탐색 — coroutine · 지연 100ms · N 20,000"
