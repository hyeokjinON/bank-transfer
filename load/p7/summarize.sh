#!/usr/bin/env bash
# P7 측정 결과를 표(마크다운)로 → docs/perf/p7-virtual-threads.md (설계 §6).
# 사용: bash load/p7/summarize.sh   (docs/perf/raw/p7/*.json — .invalid · .k6.json 은 이름 규칙으로 빠진다. RAW_DIR 은 시험용)
# 회차가 숫자인 것만 표에 넣는다 (drift 는 아래 한 줄, smoke 는 빠짐). 지표마다 따로 중앙값, pinned 는 회차 중 최댓값 (VT 를 끈 판은 '-').
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=${RAW_DIR:-docs/perf/raw/p7}

num() { grep -o "\"$2\": *[-0-9.eE+]*" "$1" | head -1 | sed 's/.*: *//'; }   # null → 빈 값
median() { awk '/^[-+]?[0-9]/' | sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "-"; else print v[int((NR + 1) / 2)] }'; }
maxv() { awk '/^[-+]?[0-9]/' | sort -g | awk 'END { if (NR == 0) print "-"; else print $1 }'; }
fmt() { if [[ "$1" =~ ^[-+]?[0-9] ]]; then printf "%.${2}f" "$1"; else printf -- "-"; fi; }
maxdev() { awk '/^[-+]?[0-9]/' | awk -v m="$1" '{ d = (m == 0) ? 0 : ($1 - m) / m * 100; if (d < 0) d = -d; if (d > x) x = d } END { printf "%.0f", x }'; }
files() { ls "$RAW/$1-"[0-9]*.json 2>/dev/null || true; }   # $1 = 회차 앞까지의 이름
med() { local k=$1; shift; for f in "$@"; do num "$f" "$k"; done | median; }
mx() { local k=$1; shift; for f in "$@"; do num "$f" "$k"; done | maxv; }

echo; echo "### 웹 1원인증 — 은행 지연 500ms (요청 하나 ≈ 1,000ms + α)"; echo
echo "| 판 · 도착률 | 회차 | 201 처리량 (건/s) | p50 (ms) | p95 (ms) | p99 (ms) | 플랫폼 스레드 최대 | 시스템 CPU 평균 (%) | 시간 초과 | dropped | pinned (최대) |"
echo "|---|---|---|---|---|---|---|---|---|---|---|"
for r in 400 800; do
  for v in blocking blocking-vt async async-vt coroutine; do
    mapfile -t fs < <(files "$v-d500-r$r")
    [ ${#fs[@]} -gt 0 ] || continue
    echo "| $v · $r | ${#fs[@]} | $(fmt "$(med rps_201 "${fs[@]}")" 1) | $(fmt "$(med p50_201 "${fs[@]}")" 1) | $(fmt "$(med p95_201 "${fs[@]}")" 1) | $(fmt "$(med p99_201 "${fs[@]}")" 1) | $(fmt "$(med threads_max "${fs[@]}")" 0) | $(fmt "$(med sys_cpu_avg "${fs[@]}")" 1) | $(fmt "$(med timeouts "${fs[@]}")" 0) | $(fmt "$(med dropped_iterations "${fs[@]}")" 0) | $(fmt "$(mx pinned_events "${fs[@]}")" 0) |"
  done
done
d="$RAW/blocking-vt-d500-r400-drift.json"
if [ -f "$d" ]; then echo; echo "- drift (마지막에 blocking-vt · 400 한 번 더): 201 $(fmt "$(num "$d" rps_201)" 1)건/s · p99 $(fmt "$(num "$d" p99_201)" 1)ms · pinned $(fmt "$(num "$d" pinned_events)" 0)"; fi

echo; echo "### 출금 컨슈머 — 은행 지연 100ms · Hikari 20"; echo
echo "| 판 | 회차 | N | 처리량 (건/s) | p50 (ms) | p95 (ms) | 시스템 CPU 평균 (%) | 은행 최대 동시 | pinned (최대) | 편차 |"
echo "|---|---|---|---|---|---|---|---|---|---|"
for v in blocking blocking-vt; do
  mapfile -t fs < <(files "$v-d100-h20")
  [ ${#fs[@]} -gt 0 ] || continue
  tps=$(med tps "${fs[@]}"); p95=$(med p95_ms "${fs[@]}")
  d1=$(for f in "${fs[@]}"; do num "$f" tps; done | maxdev "$tps")
  d2=$(for f in "${fs[@]}"; do num "$f" p95_ms; done | maxdev "$p95")
  echo "| $v | ${#fs[@]} | $(fmt "$(med n "${fs[@]}")" 0) | $(fmt "$tps" 1) | $(fmt "$(med p50_ms "${fs[@]}")" 1) | $(fmt "$p95" 1) | $(fmt "$(med sys_cpu_avg "${fs[@]}")" 1) | $(fmt "$(med bank_peak "${fs[@]}")" 0) | $(fmt "$(mx pinned_events "${fs[@]}")" 0) | $(( d1 > d2 ? d1 : d2 ))% |"
done
