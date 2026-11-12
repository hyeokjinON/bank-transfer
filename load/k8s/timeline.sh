#!/usr/bin/env bash
# 1초 남짓마다 Pod · 노드 상태를 한 줄씩 남긴다 (load/k8s/run.sh 가 배경으로 띄우고 끝나면 죽인다).
# 줄: P <epoch_ms> <app> <pod> <node> <Ready> <재시작 수>   ·   N <epoch_ms> <node> <Ready>   (빈 값은 -)
# 간격은 kubectl 호출 시간만큼 늘어난다 (Windows 에서 1 ~ 2초) — 시간 지표의 해상도
set -uo pipefail
. "$(dirname "$0")/../../infra/k8s/lib.sh"
OUT=${1:?출력 파일}
TAB=$'\t'
while :; do
  t=$(date +%s%3N)
  k get pods -o jsonpath='{range .items[*]}{.metadata.labels.app}{"\t"}{.metadata.name}{"\t"}{.spec.nodeName}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.status.containerStatuses[0].restartCount}{"\n"}{end}' 2>/dev/null \
    | awk -F"$TAB" -v t="$t" 'NF >= 2 { printf "P %s %s %s %s %s %s\n", t, $1, $2, ($3 == "" ? "-" : $3), ($4 == "" ? "-" : $4), ($5 == "" ? 0 : $5) }' >> "$OUT"
  ka get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null \
    | awk -F"$TAB" -v t="$t" 'NF >= 2 { printf "N %s %s %s\n", t, $1, $2 }' >> "$OUT"
  sleep 1
done
