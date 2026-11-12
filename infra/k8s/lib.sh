# infra/k8s/*.sh · load/k8s/*.sh 공통. source 하면 저장소 루트로 cd 한다 — 이후 경로는 모두 상대 경로
# (Windows 의 kubectl.exe 는 Git Bash 의 /c/... 경로를 못 읽고, MSYS_NO_PATHCONV 를 걸면 변환도 안 된다)
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
# shellcheck source=versions.env
. infra/k8s/versions.env
CTX="kind-$CLUSTER_NAME"

# MSYS_NO_PATHCONV 는 이 래퍼에만 (P8-2 규칙 — 전역이면 curl -o /dev/null 이 깨진다)
k() { MSYS_NO_PATHCONV=1 kubectl --context "$CTX" -n "$NAMESPACE" "$@"; }
ka() { MSYS_NO_PATHCONV=1 kubectl --context "$CTX" "$@"; }
d() { MSYS_NO_PATHCONV=1 docker "$@"; }
kindc() { MSYS_NO_PATHCONV=1 kind "$@"; }
# 비밀번호는 mysql Pod 가 이미 가진 환경변수로만 (PC 명령행에 나오지 않게 — P8-2 와 같은 방식)
sql() { k exec -i mysql-0 -- sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
ok() { printf '  \033[32mOK\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }
port_open() { timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" 2>/dev/null; }
# docker -v 에 넘길 PC 경로 (Git Bash 는 C:/..., Linux 는 그대로)
host_path() { if pwd -W >/dev/null 2>&1; then (cd "$1" && pwd -W); else (cd "$1" && pwd); fi; }

# 이미지 태그: 이미지에 들어가는 경로가 깨끗하면 dev-<짧은 커밋>, 수정 중이면 dev-<커밋>-<시각>.
# 같은 태그로 다시 apply 하면 Pod 스펙이 그대로라 롤링이 일어나지 않는다 (R3) — 수정 중인 코드를 같은 태그로 덮지 않게
image_tag() {
  if [ -n "${IMAGE_TAG:-}" ]; then echo "$IMAGE_TAG"; return; fi
  local sha; sha=$(git rev-parse --short HEAD)
  if [ -n "$(git status --porcelain -- apps modules Dockerfile .dockerignore build.gradle.kts settings.gradle.kts gradle)" ]; then
    echo "dev-$sha-$(date +%s)"
  else
    echo "dev-$sha"
  fi
}
# 지금 배포된 태그: api 는 R1 이 -r<시각> 태그로 바꾼다, 게이트웨이는 up.sh · apply_overlay 의 태그 그대로
api_tag() { k get deploy api -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'; }
deployed_tag() { k get deploy bank-gateway -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'; }

# overlay 를 태그와 함께 적용: build/k8s/<overlay>/kustomization.yaml 이 images 만 덮는다 (저장소 파일은 그대로)
apply_overlay() {   # $1 = overlay (local | ci | experiment-*), $2 = 태그
  local dir="build/k8s/$1"
  [ -d "infra/k8s/overlays/$1" ] || { echo "overlay 없음: $1" >&2; return 1; }
  mkdir -p "$dir"
  cat > "$dir/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../../infra/k8s/overlays/$1
images:
  - {name: bank-transfer/api, newTag: "$2"}
  - {name: bank-transfer/bank-gateway, newTag: "$2"}
  - {name: bank-transfer/mock-bank, newTag: "$2"}
YAML
  ka apply -k "$dir"
}

wait_ready() {
  k rollout status statefulset/mysql --timeout=300s
  k rollout status statefulset/kafka --timeout=300s
  k rollout status deploy/redis --timeout=180s
  k rollout status deploy/mock-bank --timeout=180s
  k rollout status deploy/bank-gateway --timeout=300s
  k rollout status deploy/api --timeout=420s
}

# 제어 노드에 고정하는 패치 (CoreDNS · Envoy Gateway 컨트롤러 — 워커 장애 실험에서 DNS · 입구가 같이 죽지 않게, 스펙 §16)
pin_patch() {
  printf '%s' '{"spec":{"template":{"spec":{"nodeSelector":{"bank-transfer/role":"infra"},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}]}}}}'
}

# 허용 목록에 없는 임시 Pod 에서 TCP 연결 시도 (N0). 붙으면 연결에 걸린 초(> 0), 막히면 0.000000
np_probe() {   # $1 = 호스트, $2 = 포트
  k run "np-probe-$RANDOM" --image="$CURL_IMAGE" --restart=Never --rm -i --quiet --command -- \
    sh -c "curl -s -o /dev/null --connect-timeout 3 -m 4 -w '%{time_connect}' telnet://$1:$2 || true" </dev/null 2>/dev/null | tr -d '\r' | tail -1
}
