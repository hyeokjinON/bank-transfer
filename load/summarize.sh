#!/usr/bin/env bash
# P4-2 측정 결과를 시나리오별 · 전략별 중앙값 표(마크다운)로 만든다 → docs/perf/p4-2-lock-comparison.md 에 붙인다.
# 사용: bash load/summarize.sh     (docs/perf/raw/<전략>-<시나리오>-<회차>.json 을 모두 읽는다. .full.json · .invalid 는 제외)
# 지표마다 따로 중앙값을 낸다 (한 회차의 값 묶음이 아니다). 편차 = 처리량 · p95 중 중앙값에서 가장 먼 회차의 차이(%) —
# 20% 이상이면 조건을 다시 확인한다 (스펙 §9).
set -euo pipefail
cd "$(dirname "$0")/.."
RAW=docs/perf/raw

val() { grep -o "\"$2\": *[-0-9.eE+]*" "$1" | head -1 | sed 's/.*: *//'; }
median() { sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "nan"; else print v[int((NR + 1) / 2)] }'; }
maxdev() { awk -v m="$1" '{ d = (m == 0) ? 0 : ($1 - m) / m * 100; if (d < 0) d = -d; if (d > x) x = d } END { printf "%.0f", x }'; }

for scenario in hotspot spread; do
  echo
  echo "### $scenario"
  echo
  echo "| 전략 | 회차 | 처리량 (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | 202 | 409 | 503 | 그 외 | 편차 |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|"
  for strategy in conditional pessimistic optimistic redis; do
    files=$(ls "$RAW/$strategy-$scenario-"*.json 2>/dev/null | grep -v '\.full\.json$' || true)
    [ -n "$files" ] || continue
    n=$(wc -l <<<"$files" | tr -d ' ')
    row="| $strategy | $n"
    for key in rps p50 p95 p99; do
      row="$row | $(printf '%.1f' "$(for f in $files; do val "$f" "$key"; done | median)")"
    done
    for key in status_202 status_409 status_503 other; do
      row="$row | $(printf '%.0f' "$(for f in $files; do val "$f" "$key"; done | median)")"
    done
    rps_m=$(for f in $files; do val "$f" rps; done | median)
    p95_m=$(for f in $files; do val "$f" p95; done | median)
    d1=$(for f in $files; do val "$f" rps; done | maxdev "$rps_m")
    d2=$(for f in $files; do val "$f" p95; done | maxdev "$p95_m")
    echo "$row | $(( d1 > d2 ? d1 : d2 ))% |"
  done
done
