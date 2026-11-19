#!/usr/bin/env bash
# P9a 대시보드 검사 (check.sh 의 [4/6]). JSON 으로 읽히는지 · uid · 표적마다 식이 있는지, PromQL 은 promtool promql format 으로 문법 검사.
# P9c: Loki 표적(datasource.type = loki)의 LogQL 은 build/k8s/dashboard-logql.txt 로 꺼내 두고 check.sh [5/6] 이 Loki 로 검사한다.
# JSON 파서는 python3 · python 중 되는 것 (jq 는 PC 에 없다). 둘 다 없으면 실패 — CI 러너에는 python3 가 있다.
set -euo pipefail
. "$(dirname "$0")/../lib.sh"
G=infra/k8s/monitoring/grafana
FILES=("$G/transfer-flow.json" "$G/gateway-jvm.json")

# 진짜 Python 만 — Git Bash 의 python3 는 Windows Store 스텁일 수 있어 출력으로 확인한다 (P9b run_py 와 같은 방식)
py() {
  local p
  for p in python3 python; do
    if [ "$("$p" -c 'print(1)' 2>/dev/null | tr -d '\r')" = 1 ]; then "$p" "$@"; return; fi
  done
  echo "python 이 없어 대시보드 JSON 을 검사하지 못한다" >&2
  return 1
}
grep -q '"uid": "bt-transfer-flow"' "$G/transfer-flow.json" || { echo "transfer-flow uid 없음" >&2; exit 1; }
grep -q '"uid": "bt-gateway-jvm"' "$G/gateway-jvm.json" || { echo "gateway-jvm uid 없음" >&2; exit 1; }

mkdir -p build/k8s
OUT=build/k8s/dashboard-exprs.txt
LOGQL=build/k8s/dashboard-logql.txt
# 표적마다 데이터 소스 종류(표적 → 없으면 패널)로 나눈다. 식이 빈 표적 · 여러 줄 식은 실패 (P9a 의 refId 수 비교를 대신한다)
py - "$OUT" "$LOGQL" "${FILES[@]}" <<'PY'
import json, sys
out, logql, files = sys.argv[1], sys.argv[2], sys.argv[3:]
prom, loki = [], []
def walk(panels):
    for p in panels:
        yield p
        yield from walk(p.get("panels", []))
for f in files:
    d = json.load(open(f, encoding="utf-8"))
    n = 0
    for p in walk(d["panels"]):
        for t in p.get("targets", []):
            kind = (t.get("datasource") or p.get("datasource") or {}).get("type")
            e = t.get("expr", "")
            if not e or "\n" in e:
                sys.exit(f"{f}: 패널 {p.get('id')} 표적 {t.get('refId')} 의 식이 비었거나 여러 줄")
            (loki if kind == "loki" else prom).append(e)
            n += 1
    if n == 0:
        sys.exit(f"{f}: 표적이 없다")
open(out, "w", encoding="utf-8", newline="\n").write("".join(e + "\n" for e in prom))
open(logql, "w", encoding="utf-8", newline="\n").write("".join(e + "\n" for e in loki))
PY
B=$(host_path build/k8s)
d run --rm -v "$B:/b:ro" --entrypoint sh "prom/prometheus:$PROMETHEUS_VERSION" -c \
  'while IFS= read -r q; do promtool --experimental promql format "$q" >/dev/null || { echo "PromQL 오류: $q" >&2; exit 1; }; done < /b/dashboard-exprs.txt'
echo "  대시보드 2장 — PromQL $(grep -c . "$OUT")개 문법 통과, LogQL $(grep -c . "$LOGQL" || true)개는 [5/6] 에서"
