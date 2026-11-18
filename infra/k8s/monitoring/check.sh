#!/usr/bin/env bash
# P9a: 알림 규칙 단위 테스트 · 설정 검사 · 대시보드 검사 (스펙 §9). PC · CI 같은 명령: bash infra/k8s/monitoring/check.sh
# promtool · amtool 은 versions.env 로 고정한 이미지 안의 것을 쓴다 (PC 에 따로 설치하지 않는다).
set -euo pipefail
. "$(dirname "$0")/../lib.sh"
M=$(host_path infra/k8s/monitoring)
PROM="prom/prometheus:$PROMETHEUS_VERSION"
AM="prom/alertmanager:$ALERTMANAGER_VERSION"
promtool() { d run --rm -v "$M:/m:ro" -w /m/prometheus --entrypoint promtool "$PROM" "$@"; }
amtool() { d run --rm -v "$M:/m:ro" -w /m/alertmanager --entrypoint amtool "$AM" "$@"; }

echo "[1/4] Prometheus 설정 · 규칙 문법"
promtool check config --syntax-only prometheus.yml
promtool check rules alerts.yml
n=$(grep -c '^      - alert: ' infra/k8s/monitoring/prometheus/alerts.yml || true)
[ "$n" = 11 ] || { echo "알림 규칙이 11개가 아니다: $n" >&2; exit 1; }

echo "[2/4] 규칙 단위 테스트 (울림 · 안 울림 · 점검 제외 · 시계열 없음)"
promtool test rules alerts-test.yml

echo "[3/4] Alertmanager 설정 · 경로"
amtool check-config alertmanager.yml
r=$(amtool config routes test --config.file=alertmanager.yml alertname=BankDisconnected | tr -d '\r')
[ "$r" = alert-sink ] || { echo "BankDisconnected 가 alert-sink 로 가지 않는다: $r" >&2; exit 1; }

echo "[4/4] 대시보드"
if [ -d infra/k8s/monitoring/grafana ]; then
  bash infra/k8s/monitoring/check-dashboards.sh
else
  echo "  (대시보드 없음 — Task 8 에서 생긴다)"
fi
echo "규칙 · 설정 · 대시보드 검사 통과"
