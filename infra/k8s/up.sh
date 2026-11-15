#!/usr/bin/env bash
# P8-3a: kind 클러스터 + Envoy Gateway + 인프라 + 앱 + 입구를 한 번에 (스펙 §10). 다시 불러도 된다 (클러스터가 있으면 그대로 쓰고 apply 만)
# 사용 (Git Bash · Linux): bash infra/k8s/up.sh [local|ci]
#   SKIP_BUILD=1    이미지 빌드를 건너뜀 (CI — 앞 단계가 이미 만들었다). kind load 는 한다
#   IMAGE_TAG=…     태그 직접 지정 (기본: lib.sh image_tag)
#   UP_ONLY=cluster 클러스터 · Envoy Gateway 까지만
# 먼저: Docker Desktop 켜기, Compose app 프로필 내리기 (둘 다 127.0.0.1:8000)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
OVERLAY=${1:-local}
[ -d "infra/k8s/overlays/$OVERLAY" ] || { echo "overlay 없음: $OVERLAY" >&2; exit 1; }

echo "[1/7] 사전 확인"
for t in kind kubectl docker; do command -v "$t" >/dev/null || { echo "$t 가 없다 — SETUP §2-9" >&2; exit 1; }; done
d info >/dev/null 2>&1 || { echo "Docker 가 꺼져 있다" >&2; exit 1; }
EXISTS=false
kindc get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME" && EXISTS=true
if [ "$EXISTS" = false ] && port_open 8000; then
  echo "127.0.0.1:8000 을 이미 쓰고 있다 — Compose app 프로필이면: docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app down" >&2
  exit 1
fi

echo "[2/7] 클러스터 ($CLUSTER_NAME, $KIND_NODE_IMAGE)"
if [ "$EXISTS" = true ]; then
  echo "  있음 — 그대로 쓴다"
else
  kindc create cluster --config infra/k8s/kind.yaml --image "$KIND_NODE_IMAGE" --wait 180s
fi
ka -n kube-system patch deploy coredns --type=strategic -p "$(pin_patch)" >/dev/null
ka -n kube-system rollout status deploy/coredns --timeout=180s

echo "[3/7] Envoy Gateway $ENVOY_GATEWAY_VERSION (Gateway API CRD 포함)"
ka apply --server-side --force-conflicts -f "https://github.com/envoyproxy/gateway/releases/download/$ENVOY_GATEWAY_VERSION/install.yaml" >/dev/null
ka -n envoy-gateway-system patch deploy envoy-gateway --type=strategic -p "$(pin_patch)" >/dev/null
ka -n envoy-gateway-system rollout status deploy/envoy-gateway --timeout=300s
ka apply -f infra/k8s/cluster/
[ "${UP_ONLY:-}" = cluster ] && { echo "클러스터 · Envoy Gateway 까지 (UP_ONLY=cluster)"; exit 0; }

TAG=$(image_tag)
echo "[4/7] 이미지 (태그 $TAG)"
for t in api bank-gateway mock-bank; do
  if [ "${SKIP_BUILD:-0}" != 1 ]; then d build --target "$t" -t "bank-transfer/$t:$TAG" . ; fi
  kindc load docker-image "bank-transfer/$t:$TAG" --name "$CLUSTER_NAME"
done

echo "[5/7] Secret db-credentials (infra/.env — 저장소에 비밀을 커밋하지 않는다)"
ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
mkdir -p build/k8s
SECRET_ENV=$(mktemp build/k8s/db-credentials.XXXXXX)
trap 'rm -f "$SECRET_ENV"' EXIT
grep -E '^(MYSQL_DATABASE|MYSQL_USER|MYSQL_PASSWORD|MYSQL_ROOT_PASSWORD)=' "$ENV_FILE" | tr -d '\r' > "$SECRET_ENV"
[ "$(wc -l < "$SECRET_ENV" | tr -d ' ')" = 4 ] || { echo "$ENV_FILE 에 MYSQL_DATABASE · MYSQL_USER · MYSQL_PASSWORD · MYSQL_ROOT_PASSWORD 가 모두 있어야 한다" >&2; exit 1; }
k create secret generic db-credentials --from-env-file="$SECRET_ENV" --dry-run=client -o yaml | ka apply -f - >/dev/null
rm -f "$SECRET_ENV"

echo "[6/7] 매니페스트 (overlays/$OVERLAY)"
apply_overlay "$OVERLAY" "$TAG"

echo "[7/7] 준비 대기"
wait_ready
k wait gateways.gateway.networking.k8s.io/bank-transfer --for=condition=Programmed --timeout=180s
ka -n envoy-gateway-system wait pod -l "gateway.envoyproxy.io/owning-gateway-name=bank-transfer" --for=condition=Ready --timeout=180s
echo "올라왔다 (태그 $TAG) — 확인: bash infra/k8s/smoke.sh · 입구 http://127.0.0.1:8000"
