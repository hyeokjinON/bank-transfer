#!/usr/bin/env bash
# P8-3a 스모크 (스펙 §10 · §11). up.sh 뒤 입구(127.0.0.1:8000, Envoy Gateway)로 확인한다:
#   0) 워크로드 전부 준비, Gateway Programmed      1) 1원인증 → 모의 은행 로그의 적요 코드 → VERIFIED
#   2) 충전 → 출금 → COMPLETED                      3) 같은 멱등키로 다시 → 같은 출금 id, 200
#   4) 요청 20번이 api Pod 두 개로 (/actuator/info 의 instance = Pod 이름)
#   5) api 두 Pod 가 서로 다른 워커                  6) 게이트웨이 readiness UP, Service 는 전부 ClusterIP, (CI) PC 에 내부 포트 없음
#   7) 컨테이너 사용자 ≠ root
#   8) NetworkPolicy: 허용 목록에 없는 Pod 에서 bank-gateway:8082 · mysql:3306 에 못 붙는다 (N0, 스펙 §8)
# 사용 (Git Bash · Linux): bash infra/k8s/smoke.sh
# 클러스터 DB 에는 사용자 load-p83a-smoke-<시각> 의 행만 쓰고 끝에서 지운다.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
json() { { grep -o "\"$1\":\"[^\"]*\"" || true; } | head -1 | cut -d'"' -f4; }
code() { tail -1 <<<"$1"; }
body() { sed '$d' <<<"$1"; }

BASE=http://127.0.0.1:8000
USER_ID="load-p83a-smoke-$(date +%s)"
ACCOUNT=1002123456789

cleanup() {
  sql >/dev/null 2>&1 <<SQL || echo "  (정리 실패 — 사용자 $USER_ID 의 행을 손으로 지운다: load/compose/cleanup.sql 과 같은 순서)" >&2
DELETE o FROM outbox_event o JOIN transfer t ON t.id = o.aggregate_id WHERE t.user_id = '$USER_ID';
DELETE FROM wallet_ledger WHERE user_id = '$USER_ID';
DELETE FROM transfer WHERE user_id = '$USER_ID';
DELETE FROM account_verification WHERE user_id = '$USER_ID';
DELETE FROM wallet WHERE user_id = '$USER_ID';
SQL
}
trap cleanup EXIT

post() {   # $1 = 경로, $2 = JSON 본문(표준입력으로 — 한글이 셸 인자 변환을 거치지 않게), 나머지 = curl 인자
  local path=$1 data=$2; shift 2
  printf '%s' "$data" | curl -s -w '\n%{http_code}' -X POST "$BASE$path" -H 'Content-Type: application/json' -H "X-User-Id: $USER_ID" "$@" --data-binary @-
}

echo "[0/8] 준비"
# 롤아웃 직후 옛 api Pod 가 Terminating 으로 남아 있으면 [4] 의 Pod 목록이 어긋난다 — 사라질 때까지 기다린다 (최대 90초)
terminating_api() { k get pods -l app=api -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"\n"}{end}' | grep -c . || true; }
for _ in $(seq 45); do
  [ "$(terminating_api)" = 0 ] && break
  sleep 2
done
[ "$(terminating_api)" = 0 ] || fail "90초가 지나도 종료 중인 api Pod 가 남아 있다 (kubectl -n bank-transfer get pods -l app=api)"
for dpl in redis mock-bank bank-gateway api; do
  r=$(k get deploy "$dpl" -o jsonpath='{.status.readyReplicas}'); w=$(k get deploy "$dpl" -o jsonpath='{.spec.replicas}')
  [ "${r:-0}" = "$w" ] || fail "$dpl 준비 ${r:-0}/$w — bash infra/k8s/up.sh"
done
for sts in mysql kafka; do
  [ "$(k get statefulset "$sts" -o jsonpath='{.status.readyReplicas}')" = 1 ] || fail "$sts 가 준비되지 않았다"
done
[ "$(k get gateways.gateway.networking.k8s.io bank-transfer -o jsonpath='{.status.conditions[?(@.type=="Programmed")].status}')" = True ] || fail "Gateway 가 Programmed 가 아니다"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/actuator/health")" = 200 ] || fail "입구 $BASE/actuator/health 가 200 이 아니다"
ok "종료 중인 api Pod 없음, 워크로드 준비, Gateway Programmed, 입구 200"

echo "[1/8] 1원인증"
R=$(post /api/v1/account-verifications "{\"bankCode\":\"090\",\"accountNo\":\"$ACCOUNT\",\"holderName\":\"홍길동\"}")
[ "$(code "$R")" = 201 ] || fail "1원인증 요청 $(code "$R"): $(body "$R")"
VID=$(body "$R" | json id)
VCODE=$(k logs deploy/mock-bank --tail=2000 2>/dev/null | grep -a "$ACCOUNT" | grep -ao "BT[0-9]\{4\}" | tail -1 | cut -c3- || true)
[ -n "$VCODE" ] || fail "모의 은행 로그에서 적요 코드(BT + 4자리)를 찾지 못했다"
R=$(post "/api/v1/account-verifications/$VID/confirm" "{\"code\":\"$VCODE\"}")
[ "$(body "$R" | json status)" = VERIFIED ] || fail "확인 $(code "$R"): $(body "$R")"
ok "1원인증 → VERIFIED"

echo "[2/8] 충전 → 출금 → COMPLETED"
R=$(post /api/v1/wallet/deposits '{"amount":10000}')
[ "$(code "$R")" = 200 ] || fail "충전 $(code "$R"): $(body "$R")"
KEY="smoke-$USER_ID"
TRANSFER="{\"bankCode\":\"090\",\"accountNo\":\"$ACCOUNT\",\"amount\":1000,\"memo\":\"smoke\"}"
R=$(post /api/v1/transfers "$TRANSFER" -H "Idempotency-Key: $KEY")
[ "$(code "$R")" = 202 ] || fail "출금 접수 $(code "$R"): $(body "$R")"
TID=$(body "$R" | json id)
S=
for _ in $(seq 60); do
  S=$(curl -s "$BASE/api/v1/transfers/$TID" -H "X-User-Id: $USER_ID" | json status)
  [ "$S" = COMPLETED ] && break
  sleep 1
done
[ "$S" = COMPLETED ] || fail "60초 안에 COMPLETED 가 아니다 (지금 $S)"
ok "출금 $TID → COMPLETED"

echo "[3/8] 같은 멱등키로 다시"
R=$(post /api/v1/transfers "$TRANSFER" -H "Idempotency-Key: $KEY")
[ "$(code "$R")" = 200 ] || fail "같은 키 재요청 $(code "$R"): $(body "$R")"
[ "$(body "$R" | json id)" = "$TID" ] || fail "다른 출금 id: $(body "$R")"
ok "같은 출금 id, 200"

echo "[4/8] 분배"
PODS=$(k get pods -l app=api -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort | tr '\n' ' ')
INST=$(for _ in $(seq 20); do curl -s "$BASE/actuator/info" | json instance; done | sort -u | tr '\n' ' ')
[ "$INST" = "$PODS" ] || fail "instance 가 api Pod 두 개가 아니다: [$INST] (Pod: [$PODS])"
ok "instance $INST"

echo "[5/8] 분산 배치"
NODES=$(k get pods -l app=api -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | tr '\n' ' ')
[ "$(wc -w <<<"$NODES" | tr -d ' ')" = 2 ] || fail "api 두 Pod 가 한 노드에 있다: $NODES (ScheduleAnyway 라 생길 수 있다 — kubectl rollout restart deploy/api)"
ok "api 노드 $NODES"

echo "[6/8] 게이트웨이 · 포트"
k exec deploy/bank-gateway -- wget -qO- http://127.0.0.1:8082/actuator/health/readiness | grep -q '"status":"UP"' || fail "게이트웨이 readiness 가 UP 이 아니다"
NOT_CIP=$(k get svc -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.type}{"\n"}{end}' | awk '$2 != "ClusterIP"')
[ -z "$NOT_CIP" ] || fail "ClusterIP 가 아닌 Service: $NOT_CIP"
if [ "${CI:-false}" = true ]; then
  for p in 8080 8082 9090 3306 9092 6379; do port_open "$p" && fail "PC 포트 $p 가 열려 있다"; done
fi
ok "게이트웨이 readiness UP, Service 전부 ClusterIP (PC 포트는 입구 8000 하나)"

echo "[7/8] 비root"
for dpl in mock-bank bank-gateway api; do
  u=$(k exec "deploy/$dpl" -- id -u | tr -d '\r')
  [ "$u" != 0 ] || fail "$dpl 가 root 로 돈다"
done
ok "세 앱 모두 root 아님"

echo "[8/8] NetworkPolicy"
[ -n "$(k get networkpolicy default-deny-ingress -o name 2>/dev/null)" ] || fail "기본 거부 정책이 없다"
for hp in "bank-gateway 8082" "mysql 3306"; do
  set -- $hp
  tc=$(np_probe "$1" "$2")
  awk -v x="${tc:-0}" 'BEGIN { exit !(x > 0) }' && fail "허용 안 된 Pod 가 $1:$2 에 붙었다 (연결 ${tc}초) — CNI 가 정책을 집행하지 않는다 (kindnet → Calico, 스펙 §8)"
done
ok "허용 안 된 Pod 는 bank-gateway:8082 · mysql:3306 에 못 붙는다"

echo "k8s smoke 통과"
