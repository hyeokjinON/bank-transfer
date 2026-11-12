# Kubernetes (kind) — P8-3a

kind 클러스터(제어 1 + 워커 2)에 인프라 · 모의 은행 · 게이트웨이 · api 2대 · Envoy Gateway 입구를 띄운다. 이유 · 결정은 [ADR-0017](../../docs/adr/0017-kubernetes-kind.md), 설계는 [스펙](../../docs/superpowers/specs/2026-11-12-p8-3a-k8s-design.md), 실험 숫자는 [측정 결과](../../docs/perf/p8-3a-k8s.md), 운영 절차는 [OPERATION §15](../../OPERATION.md#15-kubernetes-실행-p8-3a), 설치는 [SETUP §2-9](../../SETUP.md). 모든 명령은 저장소 루트의 Git Bash(또는 Linux bash)에서.

## 올리기 · 확인 · 내리기
```bash
# 먼저: Docker Desktop 켜기, IntelliJ 의 API · 게이트웨이 · 모의 은행 끄기, Compose app 프로필 내리기 (둘 다 127.0.0.1:8000)
bash infra/k8s/up.sh            # 클러스터 + Envoy Gateway + 이미지 빌드 · kind load + Secret + 매니페스트 + 준비 대기 (클러스터가 있을 때 재실행 약 2분 7초 — 이미지 빌드 · `kind load` 포함. 처음 생성 시간은 재지 않았다)
bash infra/k8s/smoke.sh         # [0] ~ [8] 입구로 1원인증 · 출금 · 멱등 · 분배 · 분산 배치 · 게이트웨이 · 비root · NetworkPolicy
kubectl --context kind-bank-transfer -n bank-transfer get pods -o wide
bash infra/k8s/down.sh          # 클러스터 삭제 (PV · 데이터까지)
```
- `up.sh [local|ci]` — 다시 불러도 된다 (클러스터가 있으면 그대로 쓰고 apply 만). `SKIP_BUILD=1` 이미지 빌드 생략(CI), `IMAGE_TAG=…` 태그 지정, `UP_ONLY=cluster` 클러스터 · Envoy Gateway 까지만.
- 입구는 `http://127.0.0.1:8000` (제어 노드 NodePort 30080 → PC). 앱 입구는 이것 하나다 (kind API 서버도 `127.0.0.1` 의 임의 포트에 열린다 — kubectl 용).
- 내려받는 것: kind 바이너리(SETUP §2-9), 노드 이미지 · 인프라 이미지, Envoy Gateway `install.yaml`. 처음 한 번만 받는다.

## 파일
| 경로 | 역할 |
|---|---|
| `versions.env` | 버전 · 이름 고정 한 곳 (kind · 노드 이미지(digest) · kubectl · Envoy Gateway · curl · k6 · 클러스터 · namespace) |
| `kind.yaml` | 클러스터 설정: 제어 노드(라벨 `bank-transfer/role=infra`, NodePort 30080 → `127.0.0.1:8000`) + 워커 2(`role=app`) |
| `lib.sh` | 스크립트 공통: 저장소 루트로 `cd`, 래퍼 `k`(namespace) · `ka`(전체) · `d` · `kindc`(여기에만 `MSYS_NO_PATHCONV`), `sql`, `image_tag`, `apply_overlay`, `wait_ready`, `pin_patch`, `np_probe` |
| `up.sh` · `down.sh` | 올리기 · 삭제 |
| `smoke.sh` | 입구 기준 확인 [0] ~ [8] (CI `k8s-smoke` 도 같은 파일) |
| `cluster/` | 클러스터 범위: `namespace.yaml` · `gatewayclass.yaml` (Kustomize namespace 변환 밖에서 `up.sh` 가 apply) |
| `base/config.yaml` | ConfigMap `app-config` (주소 · 점검 설정 — 비밀번호 없음) |
| `base/{mysql,kafka,redis}.yaml` | 인프라: MySQL · Kafka(KRaft) StatefulSet + PVC + headless Service, Redis Deployment |
| `base/mock-bank.yaml` · `bank-gateway.yaml` · `api.yaml` | 앱 3개 (Service · Deployment · 프로브 · preStop). 모의 은행은 PVC `mock-bank-files` |
| `base/gateway.yaml` | `EnvoyProxy`(NodePort 30080 패치 · 제어 노드) + `Gateway` + `HTTPRoute` (재시도 없음) |
| `base/networkpolicy.yaml` | ingress 기본 거부 + 허용 7개 (스펙 §8) |
| `base/pdb.yaml` | api `minAvailable: 1` |
| `overlays/local` · `ci` | 평소 · CI (`ci` 는 점검 시간 503 을 끈다) |
| `overlays/experiment-prestop0` | R1 대조: api preStop 제거 |
| `overlays/experiment-liveness-bank` | B2 대조: 게이트웨이 liveness 에도 `bankConnection` |
| `overlays/experiment-fast-evict` | D2' 대조: api 퇴거 대기 30초 |
| `../../load/k8s/` | 실험 도구 `run.sh` · `timeline.sh` · `timeline.awk` (OPERATION §15-7) |

## overlay 바꾸기 · 태그 규칙
```bash
bash -c '. infra/k8s/lib.sh; apply_overlay <이름> "$(deployed_tag)"'      # 예: apply_overlay experiment-prestop0 … → 끝나면 local 로 되돌린다
for o in infra/k8s/overlays/*/; do kubectl kustomize "$o" > /dev/null && echo "ok $o"; done   # 전부 빌드되는지 (CI 도 한다)
```
- `apply_overlay` 는 `build/k8s/<overlay>/kustomization.yaml` 을 만들어 이미지 태그만 덮고 `kubectl apply -k` 한다 (저장소 파일은 그대로).
- **태그는 커밋마다 바뀐다**: `dev-<짧은 커밋>`, 이미지에 들어가는 경로(`apps` · `modules` · `Dockerfile` · `.dockerignore` · 빌드 스크립트 · `gradle`)가 수정 중이면 `dev-<커밋>-<시각>`. CI 는 `ci`, `latest` 는 쓰지 않는다. **같은 태그로 다시 apply 하면 Pod 스펙이 같아 롤링이 일어나지 않는다** (R3) — 코드를 고치고 같은 태그로 올리면 옛 이미지가 그대로 돈다.
- 지금 배포된 태그: `deployed_tag`(게이트웨이 기준) · `api_tag`(api — 실험 R1 은 `-r<시각>` 태그로 바꾼다).

## 비밀
- Secret `db-credentials` 는 **`up.sh` 가** `infra/.env`(없으면 `infra/.env.example`)의 `MYSQL_DATABASE` · `MYSQL_USER` · `MYSQL_PASSWORD` · `MYSQL_ROOT_PASSWORD` 4줄로 만든다. 저장소에는 비밀이 없고 ConfigMap 에도 넣지 않는다. CI 는 실행마다 임의 비밀번호 (P8-2 `app-smoke` 와 같은 방식).
- **MySQL PV 가 이미 초기화됐으면 `.env` 의 비밀번호를 바꿔도 DB 에 반영되지 않는다** (이미지는 빈 데이터 디렉터리에서만 초기화한다). 바꾸려면 `down.sh` 부터 다시 만든다.

## 배치
| 대상 | 노드 | 방법 |
|---|---|---|
| mysql · kafka · redis · mock-bank | 제어 노드 | `nodeSelector: {bank-transfer/role: infra}` + toleration `node-role.kubernetes.io/control-plane:NoSchedule` |
| CoreDNS · Envoy Gateway 컨트롤러 | 제어 노드 | `up.sh` 가 `pin_patch` 로 패치 |
| Envoy 프록시 | 제어 노드 | `EnvoyProxy.envoyDeployment.pod` |
| api ×2 · bank-gateway | 워커 | `nodeSelector: {bank-transfer/role: app}`. api 는 `topologySpreadConstraints` (`maxSkew 1` · `ScheduleAnyway` · `matchLabelKeys: [pod-template-hash]`) 로 워커에 하나씩 |

PV(`data-mysql-0` · `data-kafka-0` · `mock-bank-files`)는 제어 노드에 묶인다 (`kubectl get pv -o yaml` 의 nodeAffinity).

## 자주 막히는 곳
| 증상 | 원인 · 해결 |
|---|---|
| `up.sh` 가 `127.0.0.1:8000 을 이미 쓰고 있다` | Compose `app` 프로필(nginx)이 떠 있다 — `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile app down`. 둘을 동시에 띄우지 않는다 |
| Docker Desktop 엔진이 안 뜸: `initializing Secrets Engine … engine.sock … The file cannot be accessed by the system` | 깨진 소켓 파일이 든 `%LOCALAPPDATA%\docker-secrets-engine` 폴더 이름을 바꾸고 Docker Desktop 을 다시 시작 (측정 중 한 번 겪음) |
| Git Bash 에서 `kubectl`·`docker` 의 `/…` 인자(컨테이너 안 경로)가 `C:/Program Files/Git/…` 으로 바뀜 | Git Bash 의 경로 자동 변환 — `MSYS_NO_PATHCONV=1` 을 그 명령 **앞에만** 붙인다 (`lib.sh` 래퍼가 이미 한다). 전역 `export` 하면 `curl -o /dev/null` 이 깨진다. 스크립트는 `lib.sh` 가 저장소 루트로 `cd` 해 상대 경로만 쓴다 (Windows `kubectl.exe` 는 `/c/…` 경로를 못 읽는다) |
| 앱이 `REDIS_PORT` · `BANK_GATEWAY_PORT` · Kafka 설정을 이상하게 읽음 | K8s 가 Service 마다 `REDIS_PORT=tcp://10.x.x.x:6379` 같은 환경변수를 넣는다. 모든 Pod 에 `enableServiceLinks: false` 를 둔 이유 — 새 Pod 를 추가할 때도 잊지 않는다 |
| 입구가 연결 거부 · `Connection refused` | Envoy Service 의 NodePort 가 30080 인지: `kubectl --context kind-bank-transfer -n envoy-gateway-system get svc`. `EnvoyProxy.envoyService` 에는 `nodePort` 필드가 없어 `base/gateway.yaml` 이 `patch`(StrategicMerge)로 덧씌운다 — 패치가 안 먹으면 Envoy Gateway 버전 · `EnvoyProxy` 를 본다 |
| 스모크 [4] · [5] 가 실패 | 롤아웃 직후 옛 api Pod 가 Terminating 으로 남았거나 두 Pod 가 한 워커에 몰렸다. [0] 이 종료 중 Pod 를 90초까지 기다린다. 몰렸으면 `kubectl --context kind-bank-transfer -n bank-transfer rollout restart deploy/api` |
| 스모크 [8] FAIL `N0 확인 실패 … curl 종료 코드 N` | `np_probe` 는 `<time_connect> <curl 종료 코드>` 를 낸다. 막힘 = `0.000000 28` 뿐이다. 6(이름 풀이 실패)·7(연결 거부)은 대상 오류 — FQDN 을 확인한다 (짧은 이름이 없으면 풀이 시간 초과로 28 이 나올 수 있다) |
| 스모크 [8] FAIL `N0 확인용 Pod 가 동작하지 않는다` | `curlimages/curl` 임시 Pod 가 못 떴다 (이미지 받기 · 클러스터 상태). 양성 대조(API 서버 443)가 실패하면 정책 판정을 하지 않고 닫는다 |
| 이미지를 고쳤는데 그대로 | 같은 태그로 apply — 롤링이 안 일어난다. 이미지 경로를 수정하면 `image_tag` 가 시각을 붙인다. 커밋 없이 같은 태그를 쓰는 `IMAGE_TAG=` 직접 지정은 피한다 |
| 메모리 부족 (Pod `Pending` · 노드 `NotReady`) | Docker(WSL2) 메모리 한도 — SETUP §2-9. 클러스터 전체 약 4.5GiB (제어 노드 3.09 · worker 0.58 · worker2 0.80, 부하 없음 `docker stats --no-stream` 실측 — 부하 중은 재지 않음) |
