#!/usr/bin/env bash
# P9a 대시보드 검사 (check.sh 의 [4/4]). JSON 으로 읽히는지 · uid · 모든 패널 PromQL 이 문법상 맞는지 (promtool promql format).
# JSON 파서는 python3 · python 중 되는 것 (jq 는 PC 에 없다). 둘 다 없으면 실패 — CI 러너에는 python3 가 있다.
set -euo pipefail
. "$(dirname "$0")/../lib.sh"
G=infra/k8s/monitoring/grafana
FILES=("$G/transfer-flow.json" "$G/gateway-jvm.json")

json_ok() {
  local py
  for py in python3 python; do
    if "$py" -c 'import json' >/dev/null 2>&1; then
      "$py" -c 'import json, sys; [json.load(open(f, encoding="utf-8")) for f in sys.argv[1:]]' "$@"
      return
    fi
  done
  echo "python 이 없어 대시보드 JSON 을 검사하지 못한다" >&2
  return 1
}
json_ok "${FILES[@]}" || { echo "대시보드 JSON 이 깨졌다" >&2; exit 1; }
grep -q '"uid": "bt-transfer-flow"' "$G/transfer-flow.json" || { echo "transfer-flow uid 없음" >&2; exit 1; }
grep -q '"uid": "bt-gateway-jvm"' "$G/gateway-jvm.json" || { echo "gateway-jvm uid 없음" >&2; exit 1; }

# PromQL 꺼내기: "expr":"…" (JSON 문자열 — \" 만 되돌린다). 표적(refId) 수와 같아야 한다 (정규식이 놓친 것이 없게)
mkdir -p build/k8s
OUT=build/k8s/dashboard-exprs.txt
: > "$OUT"
for f in "${FILES[@]}"; do
  n_ref=$(grep -o '"refId":"[A-Z]"' "$f" | wc -l | tr -d ' ')
  grep -oE '"expr":"([^"\\]|\\.)*"' "$f" | sed -E 's/^"expr":"//; s/"$//; s/\\"/"/g' > build/k8s/exprs.one
  n_expr=$(wc -l < build/k8s/exprs.one | tr -d ' ')
  [ "$n_expr" = "$n_ref" ] && [ "$n_expr" -gt 0 ] || { echo "$f: PromQL $n_expr 개 ≠ 표적 $n_ref 개" >&2; exit 1; }
  cat build/k8s/exprs.one >> "$OUT"
done
B=$(host_path build/k8s)
d run --rm -v "$B:/b:ro" --entrypoint sh "prom/prometheus:$PROMETHEUS_VERSION" -c \
  'while IFS= read -r q; do promtool --experimental promql format "$q" >/dev/null || { echo "PromQL 오류: $q" >&2; exit 1; }; done < /b/dashboard-exprs.txt'
echo "  대시보드 2장 — PromQL $(wc -l < "$OUT" | tr -d ' ')개 문법 통과"
