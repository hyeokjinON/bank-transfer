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
# 리더 라벨이 붙은 게이트웨이 Pod 이름 (P8-3b). 없으면 빈 줄 — 선출을 끈 H0 대조 등
gw_leader() { k get pods -l app=bank-gateway,bank-transfer/gateway-role=leader -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true; }

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
  - {name: prom/prometheus, newTag: "$PROMETHEUS_VERSION"}
  - {name: prom/alertmanager, newTag: "$ALERTMANAGER_VERSION"}
  - {name: grafana/grafana, newTag: "$GRAFANA_VERSION"}
  - {name: busybox, newTag: "$BUSYBOX_VERSION"}
  - {name: grafana/tempo, newTag: "$TEMPO_VERSION"}
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

# 허용 목록에 없는 임시 Pod 에서 TCP 연결 시도 (N0). "<time_connect 초> <curl 종료 코드>" 를 출력한다.
#   막힘 = "0.000000 28" (연결 시간 초과 — 정책이 패킷을 버렸다). 붙음 = time_connect > 0 (telnet:// 은 붙은 뒤 -m 4 로 끊겨 종료 코드 28 이 같이 나온다 — 종료 코드만으로는 못 가른다).
#   6(이름 풀이 실패) · 7(연결 거부) 등은 막힘이 아니다. 관측: 짧은 이름이 없으면 검색 도메인 때문에 풀이가 시간 초과(28)로 끝날 수 있다 → 대상은 FQDN(<svc>.<ns>.svc.cluster.local)으로 준다 (없는 FQDN 은 6).
np_probe() {   # $1 = 호스트, $2 = 포트
  k run "np-probe-$RANDOM" --image="$CURL_IMAGE" --restart=Never --rm -i --quiet --command -- \
    sh -c "curl -s -o /dev/null --connect-timeout 3 -m 4 -w '%{time_connect}' telnet://$1:$2; echo \" \$?\"" </dev/null 2>/dev/null | tr -d '\r' | tail -1
}

# --- P9a 관측 (monitoring namespace) ---
km() { MSYS_NO_PATHCONV=1 kubectl --context "$CTX" -n monitoring "$@"; }
wait_monitoring() { local dpl; for dpl in prometheus alertmanager alert-sink grafana tempo; do km rollout status "deploy/$dpl" --timeout=300s; done; }
# PromQL 을 URL 에 넣는다 (PromQL 은 ASCII — jq · python 없이)
urlenc() {
  local s=$1 out= c i
  for ((i = 0; i < ${#s}; i++)); do
    c=${s:i:1}
    case "$c" in [a-zA-Z0-9.~_-]) out+=$c ;; *) out+=$(printf '%%%02X' "'$c") ;; esac
  done
  printf '%s' "$out"
}
# Prometheus HTTP API — Prometheus Pod 안의 busybox wget 으로 (PC 포트를 열지 않는다)
prom_get() { km exec deploy/prometheus -- wget -qO- "http://127.0.0.1:9090$1"; }
prom_query() { prom_get "/api/v1/query?query=$(urlenc "$1")${2:+&time=$2}"; }   # $2 = epoch 초 (없으면 지금)
prom_range() { prom_get "/api/v1/query_range?query=$(urlenc "$1")&start=$2&end=$3&step=${4:-5}"; }
# 즉시 질의 결과의 첫 값 (시계열이 없으면 빈 줄)
# prom_query 실패(일시적 kubectl exec 오류)는 빈 값으로 — pipefail · set -e 로 호출 스크립트가 말없이 끝나지 않게 한다 (재시도 루프 · 판정은 호출 쪽)
prom_value() { { prom_query "$1" "${2:-}" || true; } | { grep -o '"value":\[[^]]*\]' || true; } | head -1 | sed -E 's/.*,"([^"]*)"\]$/\1/'; }

# --- P9b 추적 ---
# Tempo HTTP API — Prometheus Pod 의 busybox wget 으로 (PC 포트를 열지 않는다). 검색 결과의 traceID 는 앞 0 이 빠질 수 있다 — 쓰는 쪽이 32자로 채운다
tempo_get() { km exec deploy/prometheus -- wget -qO- "http://tempo.monitoring.svc.cluster.local:3200$1"; }
