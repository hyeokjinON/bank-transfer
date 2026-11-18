#!/usr/bin/env python3
"""P9b T2 — 출금 trace 모양 (load/k8s/p9b.sh 가 부른다, PC 전용).

입력: <동작>-<회차>.traces.txt — 한 줄 = "<출금 id>\t<Tempo /api/traces/<traceID> JSON>" (못 찾은 건은 {}).
출력: JSON 한 줄.
  traces          trace 를 찾은 출금 수
  complete        서비스 bank-transfer · bank-gateway 둘 다 + 구간 5개 이상인 trace 수 (성공 기준 2)
  spans_min/max   trace 하나의 구간 수
  accept_to_publish_ms  접수 server 끝 → outbox.publish 시작 (발행 주기 0.5초 근처여야 — 아웃박스 틈)
  consumer_to_bank_ms   첫 소비 시작 → bank.request 시작 (컨슈머 · HTTP · 게이트웨이 경로)
  bank_ms               bank.request 길이 (게이트웨이 → 은행 → 응답)
  e2e_ms                접수 server 시작 → bank.request 끝
  각각 _p50 · _p95 (가장 가까운 순위)
"""
import json
import math
import sys


def spans_of(doc):
    batches = doc.get("batches") or doc.get("resourceSpans") or (doc.get("trace") or {}).get("resourceSpans") or []
    out = []
    for batch in batches:
        attrs = (batch.get("resource") or {}).get("attributes") or []
        service = next((a.get("value", {}).get("stringValue", "") for a in attrs if a.get("key") == "service.name"), "")
        for scope in batch.get("scopeSpans") or batch.get("instrumentationLibrarySpans") or []:
            for span in scope.get("spans") or []:
                out.append({
                    "service": service,
                    "name": span.get("name", ""),
                    "start": int(span.get("startTimeUnixNano", 0)),
                    "end": int(span.get("endTimeUnixNano", 0)),
                })
    return out


def first(spans, pred):
    found = [s for s in spans if pred(s)]
    return min(found, key=lambda s: s["start"]) if found else None


def pct(values, p):
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, math.ceil(p / 100.0 * len(ordered)) - 1))
    return round(ordered[k], 1)


def main(path):
    traces = complete = 0
    counts, accept_to_publish, consumer_to_bank, bank, e2e = [], [], [], [], []
    with open(path, encoding="utf-8") as f:
        for line in f:
            _, _, raw = line.rstrip("\n").partition("\t")
            try:
                spans = spans_of(json.loads(raw or "{}"))
            except ValueError:
                continue
            if not spans:
                continue
            traces += 1
            counts.append(len(spans))
            if {"bank-transfer", "bank-gateway"} <= {s["service"] for s in spans} and len(spans) >= 5:
                complete += 1
            server = first(spans, lambda s: s["service"] == "bank-transfer" and s["name"].startswith("http post /api/v1/transfers"))
            publish = first(spans, lambda s: s["name"] == "outbox.publish")
            consumer = first(spans, lambda s: s["name"] == "transfer.requested process")
            request = first(spans, lambda s: s["name"] == "bank.request")
            if server and publish:
                accept_to_publish.append((publish["start"] - server["end"]) / 1e6)
            if consumer and request:
                consumer_to_bank.append((request["start"] - consumer["start"]) / 1e6)
            if request:
                bank.append((request["end"] - request["start"]) / 1e6)
            if server and request:
                e2e.append((request["end"] - server["start"]) / 1e6)
    out = {"traces": traces, "complete": complete, "spans_min": min(counts) if counts else None, "spans_max": max(counts) if counts else None}
    for name, values in (("accept_to_publish_ms", accept_to_publish), ("consumer_to_bank_ms", consumer_to_bank), ("bank_ms", bank), ("e2e_ms", e2e)):
        out[name + "_p50"] = pct(values, 50)
        out[name + "_p95"] = pct(values, 95)
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1])
