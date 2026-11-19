#!/usr/bin/env bash
# P9a: 알림 규칙 단위 테스트 · 설정 검사 · 대시보드 검사 (스펙 §9). P9c: Loki 설정 · 로그 알림 규칙 · LogQL, Alloy 설정 문법 · 가리기 표본 (P9c 스펙 §8).
# PC · CI 같은 명령: bash infra/k8s/monitoring/check.sh
# promtool · amtool · loki · alloy 는 versions.env 로 고정한 이미지 안의 것을 쓴다 (PC 에 따로 설치하지 않는다).
set -euo pipefail
. "$(dirname "$0")/../lib.sh"
M=$(host_path infra/k8s/monitoring)
PROM="prom/prometheus:$PROMETHEUS_VERSION"
AM="prom/alertmanager:$ALERTMANAGER_VERSION"
LOKI="grafana/loki:$LOKI_VERSION"
ALLOY="grafana/alloy:$ALLOY_VERSION"
promtool() { d run --rm -v "$M:/m:ro" -w /m/prometheus --entrypoint promtool "$PROM" "$@"; }
amtool() { d run --rm -v "$M:/m:ro" -w /m/alertmanager --entrypoint amtool "$AM" "$@"; }
mkdir -p build/k8s

echo "[1/6] Prometheus 설정 · 규칙 문법"
promtool check config --syntax-only prometheus.yml
promtool check rules alerts.yml
n=$(grep -c '^      - alert: ' infra/k8s/monitoring/prometheus/alerts.yml || true)
[ "$n" = 11 ] || { echo "알림 규칙이 11개가 아니다: $n" >&2; exit 1; }

echo "[2/6] 규칙 단위 테스트 (울림 · 안 울림 · 점검 제외 · 시계열 없음)"
promtool test rules alerts-test.yml

echo "[3/6] Alertmanager 설정 · 경로 · 억제"
amtool check-config alertmanager.yml
for a in BankDisconnected ErrorLogSurge; do
  r=$(amtool config routes test --config.file=alertmanager.yml "alertname=$a" | tr -d '\r')
  [ "$r" = alert-sink ] || { echo "$a 가 alert-sink 로 가지 않는다: $r" >&2; exit 1; }
done
grep -q 'target_matchers: \[alertname="ErrorLogSurge"\]' infra/k8s/monitoring/alertmanager/alertmanager.yml \
  || { echo "ErrorLogSurge 억제 규칙이 없다 (결정 F9)" >&2; exit 1; }

echo "[4/6] 대시보드"
bash infra/k8s/monitoring/check-dashboards.sh

echo "[5/6] Loki 설정 · 로그 알림 규칙 · LogQL (P9c)"
d run --rm -v "$M/loki:/etc/loki/config:ro" "$LOKI" -config.file=/etc/loki/config/loki.yml -verify-config
# 규칙 · LogQL 은 Loki 를 잠깐 띄워 묻는다 (이미지에 셸 · lint 도구가 없다). 규칙 파일은 kind 와 같은 자리 (auth 끔 테넌트 = fake).
# 질의는 같은 네트워크의 curl 컨테이너로 (PC 포트를 열지 않는다). 틀린 LogQL 이면 규칙 그룹이 0개가 된다
LC="p9c-loki-check-$$"
trap 'd rm -f "$LC" >/dev/null 2>&1 || true' EXIT
d run -d --name "$LC" --tmpfs /var/loki:uid=10001 -v "$M/loki:/etc/loki/config:ro" -v "$M/loki/rules.yml:/etc/loki/rules/fake/rules.yml:ro" \
  "$LOKI" -config.file=/etc/loki/config/loki.yml >/dev/null
lq() { d run --rm --network "container:$LC" "$CURL_IMAGE" -s "$@"; }
ready=
for _ in $(seq 40); do
  [ "$(lq http://127.0.0.1:3100/ready 2>/dev/null || true)" = ready ] && { ready=1; break; }
  sleep 2
done
[ -n "$ready" ] || { d logs "$LC" 2>&1 | tail -20 >&2; echo "Loki 가 80초 안에 Ready 가 아니다" >&2; exit 1; }
RULES=$(lq http://127.0.0.1:3100/prometheus/api/v1/rules || true)
grep -q '"name":"ErrorLogSurge"' <<<"$RULES" || { d logs "$LC" 2>&1 | grep -i rule | tail -5 >&2; echo "로그 알림 ErrorLogSurge 가 적재되지 않았다 (규칙 파일 문법 · LogQL)" >&2; exit 1; }
n=$({ grep -o '"type":"alerting"' <<<"$RULES" || true; } | wc -l | tr -d ' ')
[ "$n" = 1 ] || { echo "로그 알림 규칙이 1개가 아니다: $n (결정 F9)" >&2; exit 1; }
# LogQL 문법: 규칙 식 + 대시보드의 Loki 표적 (check-dashboards.sh 가 build/k8s/dashboard-logql.txt 로 꺼낸다)
{ sed -n 's/^ *expr: //p' infra/k8s/monitoring/loki/rules.yml; [ ! -f build/k8s/dashboard-logql.txt ] || cat build/k8s/dashboard-logql.txt; } > build/k8s/logql.txt
while IFS= read -r q; do
  [ -n "$q" ] || continue
  out=$(lq -G http://127.0.0.1:3100/loki/api/v1/format_query --data-urlencode "query=$q" || true)
  grep -q '"status":"success"' <<<"$out" || { echo "LogQL 오류: $q → $out" >&2; exit 1; }
done < build/k8s/logql.txt
d rm -f "$LC" >/dev/null
echo "  Loki 설정 · 로그 알림 1 · LogQL $(grep -c . build/k8s/logql.txt)개 통과"

echo "[6/6] Alloy 설정 문법 · 가리기 표본 (P9c)"
if [ -f infra/k8s/monitoring/alloy/check.alloy ]; then
  for f in config.alloy pipeline.alloy check.alloy; do d run --rm -v "$M:/m:ro" "$ALLOY" fmt "/m/alloy/$f" >/dev/null; done
  # 수집 설정과 같은 처리(pipeline.alloy)를 표본 파일(kind 의 CRI 로그 모양)에 10초 돌려 loki.echo 출력으로 본다
  OUTF=build/k8s/alloy-check.txt
  d run --rm -v "$M:/m:ro" --entrypoint sh "$ALLOY" -c 'timeout 10 alloy run /m/alloy/check.alloy --storage.path=/tmp/alloy --disable-reporting 2>&1; true' > "$OUTF"
  ENTRIES=build/k8s/alloy-entries.txt
  { grep -a 'msg="received log entry"' "$OUTF" || true; } > "$ENTRIES"
  n=$(grep -c . "$ENTRIES" || true)
  [ "$n" = 6 ] || { tail -30 "$OUTF" >&2; echo "표본 6줄 중 ${n}줄만 나왔다 (잘린 줄 P + F 는 한 줄)" >&2; exit 1; }
  n=$(grep -cF 'labels="{app=\"api\", container=\"api\", namespace=\"bank-transfer\"}"' "$ENTRIES" || true)
  [ "$n" = 6 ] || { echo "라벨이 app · container · namespace 셋이 아닌 줄이 있다 (${n}/6 — stage.label_keep)" >&2; exit 1; }
  sed -n 's/.* entry="\(.*\)" entry_timestamp=.*/\1/p' "$ENTRIES" > build/k8s/alloy-lines.txt
  if grep -aE '(^|[^0-9A-Za-z])[0-9]{10,14}([^0-9A-Za-z]|$)|BT[0-9]{4}|[0-9]원' build/k8s/alloy-lines.txt >&2; then
    echo "가리지 않은 줄이 있다 (위)" >&2; exit 1
  fi
  for want in '*********6789' '*********6780,******3333' 'BT****' '***원' '거래번호=T2611190123456789AB' '1234567890123abcdef0123456789abc'; do
    grep -aqF -- "$want" build/k8s/alloy-lines.txt || { echo "가린 결과에 '$want' 가 없다 (과하게 · 덜 가렸다)" >&2; exit 1; }
  done
  for tid in 1234567890123abcdef0123456789abc 4bf92f3577b34da6a3ce929d0e0e4736 7ec3e15e708ee2d039aa42ee04193141 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; do
    grep -aq "structured_metadata=.*$tid" "$ENTRIES" || { echo "trace_id $tid 를 메타데이터로 꺼내지 못했다 (가리기 전에 꺼내야 한다)" >&2; exit 1; }
  done
  n=$({ grep -ao 'structured_metadata="[^ ]*level' "$ENTRIES" || true; } | wc -l | tr -d ' ')
  [ "$n" = 6 ] || { echo "level 메타데이터가 6줄이 아니다: $n" >&2; exit 1; }
  echo "  Alloy 설정 3개 문법 · 표본 6줄 — 라벨 3개, 금지 패턴 0, 거래번호 · traceId 그대로, trace_id 4 · level 6"
else
  echo "  (Alloy 없음 — Task 5 에서 생긴다)"
fi
echo "규칙 · 설정 · 대시보드 · 로그 검사 통과"
