#!/usr/bin/env bash
# P6-2 측정 결과를 표(마크다운)로 만든다 → docs/perf/p6-2-web-reactive.md 에 붙인다.
# 사용: bash load/verification/summarize.sh   (docs/perf/raw/p6-2/*.json — .invalid 는 제외)
# 회차가 숫자면 본 측정 · 원인 확인 표, probe 로 시작하면 한계 탐색 표, 그 밖(smoke)은 빠진다. 지표마다 따로 중앙값.
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=docs/perf/raw/p6-2

val() { grep -o "\"$2\": *[-0-9.eE+a-z]*" "$1" | head -1 | sed 's/.*: *//'; }
median() { sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "nan"; else print v[int((NR + 1) / 2)] }'; }
row() {   # $1 = 파일 목록 (줄바꿈), 나머지 = 앞 칸들
  local files=$1; shift
  local n; n=$(wc -l <<<"$files" | tr -d ' ')
  local line="| $* | $n"
  for key in rps_201 p50_201 p95_201 p99_201 threads_max sys_cpu_avg app_cpu_avg; do
    line="$line | $(printf '%.1f' "$(for f in $files; do val "$f" "$key"; done | median)")"
  done
  for key in timeouts dropped_iterations status_5xx; do
    line="$line | $(printf '%.0f' "$(for f in $files; do val "$f" "$key"; done | median)")"
  done
  echo "$line |"
}
header() {
  echo "| $1 | 회차 | 201 처리량 (건/s) | p50 (ms) | p95 (ms) | p99 (ms) | 스레드 최대 | 시스템 CPU 평균 (%) | 앱 CPU 평균 (%) | 시간 초과 | dropped | 5xx |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
}

for d in 100 500; do
  echo; echo "### 은행 지연 ${d}ms (요청 하나 ≈ $(( d * 2 ))ms + α)"; echo
  header "판 · 도착률"
  for r in 100 200 400; do
    for m in blocking async coroutine reactor; do
      files=$(ls "$RAW/$m-d$d-r$r-"[0-9]*.json 2>/dev/null || true)
      [ -n "$files" ] && row "$files" "$m · $r"
    done
  done
done

echo; echo "### 원인 확인 — MVC 블로킹, Tomcat 최대 스레드 200 vs 400 (지연 500ms · 도착률 400)"; echo
header "톰캣 스레드"
f200=$(ls "$RAW/blocking-d500-r400-"[0-9]*.json 2>/dev/null || true); [ -n "$f200" ] && row "$f200" "200"
f400=$(ls "$RAW/blocking-d500-r400-t400-"[0-9]*.json 2>/dev/null || true); [ -n "$f400" ] && row "$f400" "400"

echo; echo "### 한계 탐색 — 비동기 세 판, 지연 500ms (회차마다 한 번)"; echo
header "판 · 도착률"
for m in async coroutine reactor; do
  for f in $(ls "$RAW/$m-d500-r"*"-probe"*.json 2>/dev/null | sort -V || true); do
    row "$f" "$m · $(val "$f" rate)"
  done
done
