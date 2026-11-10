#!/usr/bin/env bash
# P6-2 측정 결과를 표(마크다운)로 만든다 → docs/perf/p6-2-web-reactive.md 에 붙인다.
# 사용: bash load/verification/summarize.sh   (docs/perf/raw/p6-2/*.json — .invalid 는 제외)
#       RAW_SUB=p8-1 bash load/verification/summarize.sh   (P8-1: 게이트웨이 · 지연 · 도착률별 표 + gw CPU · gw 스레드 열 — 그 폴더를 읽을 때만. P6 · P7 표 모양은 그대로)
# 회차가 숫자면 본 측정 · 원인 확인 표, probe 로 시작하면 한계 탐색 표, 그 밖(smoke)은 빠진다. 지표마다 따로 중앙값.
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=${RAW_DIR:-docs/perf/raw/${RAW_SUB:-p6-2}}   # RAW_DIR 은 시험용
case "$RAW" in */p8-1*) GWCOLS=1 ;; *) GWCOLS=0 ;; esac

val() { grep -o "\"$2\": *[-0-9.eE+a-z]*" "$1" | head -1 | sed 's/.*: *//'; }
str() { grep -o "\"$2\": *\"[^\"]*\"" "$1" | head -1 | sed 's/.*: *"//; s/"$//' || true; }
gwmed() {   # $1 = 파일 목록, $2 = 키, $3 = 소수 자릿수 — 숫자가 아닌 값(null)은 건너뛴다. 값이 없으면 '-'
  local v; v=$(for f in $1; do val "$f" "$2"; done | awk '/^[-+]?[0-9]/' | median)
  if [[ "$v" =~ ^[-+]?[0-9] ]]; then printf "%.${3}f" "$v"; else printf -- "-"; fi
}
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
  [ "$GWCOLS" = 1 ] && line="$line | $(gwmed "$files" gw_cpu_avg 1) / $(gwmed "$files" gw_cpu_max 1) | $(gwmed "$files" gw_threads_max 0)"
  echo "$line |"
}
header() {
  local gh="" gs=""
  [ "$GWCOLS" = 1 ] && { gh=" | gw CPU 평균 / 최대 (%) | gw 스레드 최대"; gs="|---|---"; }
  echo "| $1 | 회차 | 201 처리량 (건/s) | p50 (ms) | p95 (ms) | p99 (ms) | 스레드 최대 | 시스템 CPU 평균 (%) | 앱 CPU 평균 (%) | 시간 초과 | dropped | 5xx$gh |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---$gs|"
}

if [ "$GWCOLS" = 1 ]; then
  # P8-1: 게이트웨이 · 지연 · 도착률별 (회차가 숫자인 것). 이름 접미사(-gw-…)가 달라도 JSON 의 gateway 값으로 묶는다
  echo; echo "### P8-1 — 게이트웨이 · 은행 지연 · 도착률 (async 판)"; echo
  header "게이트웨이 · 지연 · 도착률"
  keys=$(for f in "$RAW"/*.json; do
    [ -f "$f" ] || continue
    [[ "$(str "$f" round)" =~ ^[0-9]+$ ]] || continue
    g=$(str "$f" gateway); echo "${g:-direct} $(val "$f" delay_ms) $(val "$f" rate)"
  done | sort -u -k1,1 -k2,2n -k3,3n)
  while read -r g d r; do
    [ -n "$g" ] || continue
    files=$(for f in "$RAW"/*.json; do
      [ -f "$f" ] || continue
      [[ "$(str "$f" round)" =~ ^[0-9]+$ ]] || continue
      fg=$(str "$f" gateway); [ "${fg:-direct}" = "$g" ] && [ "$(val "$f" delay_ms)" = "$d" ] && [ "$(val "$f" rate)" = "$r" ] && echo "$f"
    done)
    row "$files" "$g · ${d}ms · $r"
  done <<<"$keys"
  exit 0
fi

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
