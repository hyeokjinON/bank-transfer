#!/usr/bin/env bash
# P6-1 측정 결과를 중앙값 표(마크다운)로 만든다 → docs/perf/p6-1-consumer-coroutines.md 에 붙인다.
# 사용: bash load/consumer/summarize.sh     (docs/perf/raw/p6-1/*.json — .invalid 제외)
# 표 ① 비교: 회차가 숫자인 것. 표 ② 용량 탐색: 회차가 sweep 으로 시작하는 것. 그 밖(drift · smoke)은 넣지 않는다.
# 지표마다 따로 중앙값을 낸다 (한 회차의 값 묶음이 아니다). 편차 = 처리량 · p95 중 중앙값에서 가장 먼 회차의 차이(%) — 20% 이상이면 조건을 다시 확인한다 (P4-2 와 같다).
# 예상(리틀의 법칙) = 동시에 은행을 기다리는 수 ÷ 지연: blocking 3, coroutine min(상한, 150 = 컨슈머 3 × poll 50). 지연 0 은 계산하지 않는다.
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=docs/perf/raw/p6-1

num() { grep -o "\"$2\": *[-0-9.eE+]*" "$1" | head -1 | sed 's/.*: *//'; }
str() { grep -o "\"$2\": *\"[^\"]*\"" "$1" | head -1 | sed 's/.*: *"//; s/"$//'; }
median() { sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "nan"; else print v[int((NR + 1) / 2)] }'; }
maxdev() { awk -v m="$1" '{ d = (m == 0) ? 0 : ($1 - m) / m * 100; if (d < 0) d = -d; if (d > x) x = d } END { printf "%.0f", x }'; }
key() { echo "$(str "$1" mode) $(num "$1" delay_ms) $(num "$1" cap) $(num "$1" hikari)"; }

table() {   # $1 = 회차 정규식, $2 = 제목
  local files=() f
  for f in "$RAW"/*.json; do
    [ -f "$f" ] || continue
    [[ "$(str "$f" round)" =~ $1 ]] && files+=("$f")
  done
  echo
  echo "### $2"
  echo
  echo "| 모드 | 지연 (ms) | 상한 | Hikari | N | 회차 | 처리량 (건/s) | 예상 (건/s) | p50 (ms) | p95 (ms) | 시스템 CPU 평균 / 최대 (%) | API CPU 평균 (%) | MySQL CPU 평균 (%) | 은행 최대 동시 | 편차 |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  [ ${#files[@]} -gt 0 ] || return 0
  # 파이프 뒤 while 은 하위 셸이라 local 을 쓰지 않는다 (변수는 그 안에서만 산다)
  for f in "${files[@]}"; do key "$f"; done | sort -u -k1,1 -k2,2n -k3,3n -k4,4n | while read -r mode delay cap hikari; do
    group=()
    for g in "${files[@]}"; do [ "$(key "$g")" = "$mode $delay $cap $hikari" ] && group+=("$g"); done
    med() { for g in "${group[@]}"; do num "$g" "$1"; done | median; }
    tps=$(med tps); p95=$(med p95_ms)
    if [ "$mode" = blocking ]; then conc=3; capcol="-"; else conc=$(( cap < 150 ? cap : 150 )); capcol=$cap; fi
    if [ "$delay" = 0 ]; then expect="-"; else expect=$(awk -v c="$conc" -v d="$delay" 'BEGIN { printf "%.0f", c * 1000 / d }'); fi
    d1=$(for g in "${group[@]}"; do num "$g" tps; done | maxdev "$tps")
    d2=$(for g in "${group[@]}"; do num "$g" p95_ms; done | maxdev "$p95")
    printf '| %s | %s | %s | %s | %.0f | %d | %.1f | %s | %.1f | %.1f | %.1f / %.1f | %.1f | %.1f | %.0f | %s%% |\n' \
      "$mode" "$delay" "$capcol" "$hikari" "$(med n)" "${#group[@]}" "$tps" "$expect" "$(med p50_ms)" "$p95" \
      "$(med sys_cpu_avg)" "$(med sys_cpu_max)" "$(med api_cpu_avg)" "$(med mysql_cpu_avg)" "$(med bank_peak)" "$(( d1 > d2 ? d1 : d2 ))"
  done
}

table '^[0-9]+$' "① 비교 — 모드 × 은행 지연 (대조: coroutine 상한 3)"
table '^sweep[0-9]+$' "② 용량 탐색 — coroutine · 지연 100ms · N 20,000"
