# ADR-0017: Kubernetes (kind) 로 옮기기 — Envoy Gateway 입구, 프로브 3종 · preStop, 게이트웨이 readiness 에만 은행 연결, NetworkPolicy, 배치 고정

- 상태: 제안 (2026-11-12, 머지 뒤 정리 PR 에서 `채택`)
- 관련: [설계](../superpowers/specs/2026-11-12-p8-3a-k8s-design.md) · [계획](../superpowers/plans/2026-11-12-p8-3a-k8s.md) · [측정 결과](../perf/p8-3a-k8s.md) · [ADR-0015](0015-bank-gateway-service.md) (게이트웨이 분리 — 연결 주인 하나) · [ADR-0016](0016-app-images-compose.md) (앱 이미지 · Compose — "다음 (P8-3)" 이 이 단계의 출발점) · [ADR-0004](0004-idempotency-key-and-transactional-outbox.md) (멱등키) · [ADR-0006](0006-inquire-instead-of-resend.md) (모르면 묻는다) · [ADR-0007](0007-retry-topic-and-bounded-republish.md) (재시도 토픽 · 제한 재발행)

## 맥락
- ADR-0016 이 P8-3 으로 넘긴 것: Deployment · Service · 입구, readiness 에 게이트웨이 `bankConnection`(liveness 제외), NetworkPolicy, 게이트웨이 이중화. 이중화는 리더 선출 같은 새 코드가 필요해 **P8-3b 로 나누고**, 이 단계(P8-3a)는 **같은 구성을 Kubernetes 로 옮기고** 실무에서 사고가 나는 지점(배포 · 외부 장애 · 망 정책 · 노드)을 실험 5단계로 점검한다.
- 이 단계의 코드 변경은 게이트웨이 `application.yml` 의 readiness 그룹과 테스트 1개(`ProbeGroupsTest`)뿐이다. 나머지는 `infra/k8s/**` · `load/k8s/**` · CI 잡 · 문서다. 돈 불변식(접수 때 차감, 잔고 = 원장 합, 같은 거래번호 은행 송신 1번, 안 보냄만 다시 보내고 모르면 묻는다)은 건드리지 않는다.
- **처리량을 재는 단계가 아니다** (한 PC — ADR-0016 과 같은 이유). 확인하는 것은 "배포 · 장애 · 정책 · 노드 손실에서 돈이 맞고 무엇이 얼마나 끊기나" 이다.

## 결정
스펙 §3 의 13개 결정 + 계획 단계에서 고친 것(스펙 §16). 각각 이유와 버린 대안.

| # | 결정 | 이유 | 버린 대안 |
|---|---|---|---|
| 1 | 클러스터는 **kind** (제어 1 + 워커 2), 설정 `infra/k8s/kind.yaml` | 노드 단위 실험(drain · 정지)이 된다. 기본 네트워크(kindnet)가 NetworkPolicy 를 집행한다 (N0 로 확인). CI 에서 같은 설정 | Docker Desktop 내장 K8s (노드 1, 정책 집행 불확실), minikube · k3d |
| 2 | 인프라(MySQL · Kafka · Redis)도 **클러스터 안**, namespace `bank-transfer` | 개발 DB 와 분리(IntelliJ 와 충돌 없음), CI 그대로 | 클러스터 밖 Compose (Kafka 주소 · 네트워크가 복잡, 개발 DB 공유) |
| 3 | 입구는 **Gateway API + Envoy Gateway**, **재시도를 설정하지 않는다** (`retry` 필드 · `BackendTrafficPolicy` 금지) | ingress-nginx 는 2026-03 은퇴 → Gateway API 가 현재 표준. 보낸 POST 를 다시 보내지 않는다 — 1원인증 요청에는 멱등키가 없다 (ADR-0016 결정 3 의 `non_idempotent` 금지와 같은 규칙) | nginx 를 Deployment 로 (K8s 방식 아님), ingress-nginx (은퇴) |
| 4 | 매니페스트는 **Kustomize** (`base` + `overlays/{local,ci,experiment-*}`) | `kubectl` 내장, 설치 없음. 실험 overlay 가 `base` 의 한 줄씩만 덮는다 | Helm (배포할 차트가 아니다) |
| 5 | 이미지는 P8-2 Dockerfile 그대로, **`kind load docker-image`**, 태그 `dev-<짧은 커밋>` (이미지 경로가 수정 중이면 `-<시각>`), CI 는 `ci`, `latest` 금지 | 레지스트리 불필요. **같은 태그로 다시 apply 하면 Pod 스펙이 같아 롤링이 일어나지 않는다** (R3 로 확인) | 로컬 레지스트리 |
| 6 | 게이트웨이 · 모의 은행 replicas 1 | 이중화는 P8-3b | — |
| 7 | 인프라 · **모의 은행** · CoreDNS · Envoy Gateway 컨트롤러 · Envoy 프록시는 **제어 노드**, 앱(api · 게이트웨이)은 **워커 2** | 워커 실험 때 DB · 은행 · DNS · 입구가 같이 죽지 않게. 은행은 원래 클러스터 밖에 있는 존재다 (스펙 §16) | 노드 하나 더 (메모리) |
| 8 | 대사 파일 공유는 범위 밖 | RWX 없음 (`local-path` 는 RWO), 실무도 공유 디스크가 아니라 SFTP · 파일 전문 | NFS 서버 Pod |
| 9 | api readiness 에 외부 의존성(DB · Kafka · 게이트웨이)을 **넣지 않는다** (Spring 기본 `readinessState` 만) | 외부 장애 때 api 2대가 동시에 Service 에서 빠져 원인 없는 503 이 된다. api 가 떠서 "안 보냄 → 재시도" · 명확한 오류를 내는 편이 낫다 | 넣기 |
| 10 | **게이트웨이 readiness 에만 `bankConnection`**, liveness 에는 넣지 않는다 | 은행 장애 = 트래픽 제외이지 재시작 사유가 아니다 (B1 · B2 로 숫자 확인) | liveness 에도 넣기 (재시작 루프) |
| 11 | PDB 는 api 만 `minAvailable: 1` | 1대짜리에 걸면 drain 이 끝나지 않는다 | 전부에 걸기 |
| 12 | NetworkPolicy 는 **ingress** 기본 거부 + 허용 목록 7개 | 가장 효과 큰 경계 (게이트웨이 · 모의 은행 · DB). egress 까지는 DNS 허용 등이 복잡하다 | egress 까지 (다음 과제) |
| 13 | 입구도 PC `127.0.0.1:8000` (제어 노드 NodePort 30080 → `extraPortMappings`, `listenAddress: 127.0.0.1`) | Postman · 스크립트 그대로. Compose `app` 프로필과 동시에 띄우지 않는다 | 다른 포트 |
| 14 | (§16) **모의 은행을 제어 노드에 + PVC + `Recreate`**, 게이트웨이도 `Recreate` | 모의 은행 거래 기록이 Pod 와 같이 지워지면 이중 송신을 판정할 수 없다 (PVC 라 B1 의 replicas 0 → 1 에도 남는다). 게이트웨이는 replicas 1 에 롤링(`maxSurge 1`)이면 배포 때 새 Pod 와 옛 Pod 가 동시에 은행에 붙는다 | 모의 은행 워커 배치 · `emptyDir` · 게이트웨이 롤링 |
| 15 | (§16) **모든 Pod `enableServiceLinks: false`** | K8s 가 Service 마다 `REDIS_PORT=tcp://…` 같은 환경변수를 넣는데 api 설정 `${REDIS_PORT:6379}` · 게이트웨이 `bank.gateway.port`(← `BANK_GATEWAY_PORT`) · Kafka 이미지(`KAFKA_*` 를 브로커 설정으로 바꾼다)가 잘못 읽는다 | 앱 설정 이름을 바꾸기 (앱 코드 변경 금지) |

**프로브 · 종료 시간 (결정 9 · 10 에 딸린 값)**: startup `/actuator/health/liveness` 5s × 24, readiness 5s × 2, liveness 10s × 3 (timeout 3s). `preStop` sleep 10s → SIGTERM → Spring graceful 단계 20s (바꾸지 않음) → `terminationGracePeriodSeconds: 45`. api 롤링 `maxUnavailable: 0` · `maxSurge: 1`. preStop 은 이미지의 셸에 기대지 않는 K8s 내장 `sleep` 동작이다 (1.34 이상 — 아래 버전 선택 근거).

## 버전 선택 근거
모두 `infra/k8s/versions.env` 한 곳에 고정한다 (스크립트 · CI 가 source). 2026-11-12 확인.

| 항목 | 고른 것 | 이유 · 버린 것 |
|---|---|---|
| kind | `v0.33.0` | 최신 안정 (2026-08-26). 바이너리는 체크섬을 확인한다 (SETUP §2-9 · CI) |
| 노드 이미지 | `kindest/node:v1.36.4@sha256:099e0493…faed` (digest 고정) | **kind v0.33.0 의 기본(1.37)을 쓰지 않는다**: Envoy Gateway v1.9 가 시험한 K8s 범위가 1.33 ~ 1.36 이다. 같은 릴리스에 1.36.4 이미지가 있다. 태그가 아니라 **digest 로 고정**해 같은 바이트를 받는다 (재현성) |
| Envoy Gateway | `v1.9.3` (2026-10-26) | `v1.10.0` 은 2026-11-06 릴리스라 2주가 안 됐다. `v1.8.x` 는 2026-11-08 EOL (계획 단계에서 Envoy Gateway 공식 호환표로 확인: <https://gateway.envoyproxy.io/news/releases/matrix/>). 2주 이상 지난 안정 릴리스 중 최신 |
| Gateway API CRD | `v1.6.1` — **EG 설치(`install.yaml`)에 같이 들어 있다** | 따로 설치하지 않는다 (EG 가 시험한 짝을 그대로) |
| kubectl | 로컬 `v1.36.1` (Docker Desktop 동봉), CI `v1.36.4` | API 서버와 **±1 minor** 안이어야 한다. 1.36 노드와 맞다 |
| preStop sleep | K8s 1.34 이상 필요 | `lifecycle.preStop.sleep` 이 1.30 베타 · **1.34 GA**. 노드가 1.36 이라 쓴다. 1.33 이하로 내리면 `exec: sleep` 으로 바꿔야 한다 |
| 그 밖 | `curlimages/curl:8.23.0` · `grafana/k6:2.3.0` · 인프라 이미지는 Compose 와 같다 (`mysql:8.4` · `apache/kafka:4.1.0` · `redis:7.4-alpine`) | 버전을 올릴 때는 EG 호환표 (gateway.envoyproxy.io/news/releases/matrix/) 의 K8s 범위부터 본다 |

## 결과
측정 (2026-11-12, 한 PC — 운영 수치가 아니다, [측정 결과](../perf/p8-3a-k8s.md)). 판정은 모든 회차 공통: 접수된 키 = 출금 행 = 거래번호, 전부 `COMPLETED`, 은행에 빠진 것 0 · 두 번 간 것 0, 잔고 = 원장 합. **돈은 전 회차 정확했다.**

- **단계 1 이식**: `up.sh` 한 번으로 3노드 클러스터 · 인프라 · 앱 · Envoy 입구가 떴고 스모크가 통과했다 (클러스터가 있을 때 재실행 2분 7초, 이미지 빌드 · `kind load` 포함 — 처음 생성 시간은 재지 않았다). api 두 Pod 가 동시에 떠도 Flyway 마이그레이션은 버전마다 1번(6행 · 실패 0). `EnvoyProxy.envoyService.patch`(StrategicMerge, 공식 문서 예시 없음)로 NodePort 30080 이 그대로 고정돼 폴백이 필요 없었다. ShedLock 은 측정 시각에 대사 cron(01 ~ 06시)이 없어 **확인 못 했다**.
- **단계 2 프로브 · 종료 (R1 · R2 · R3)**: 롤링 업데이트 중 첫 시도 5xx — **preStop 10s: 0 · 0, preStop 0s: 4 · 9** (전부 Envoy `503 UF` — 이미 포트를 닫은 옛 Pod 로 연결 시도). 둘 다 같은 키 재시도로 전부 접수, 이중 0. 즉 **preStop 은 앱이 아니라 입구가 늦게 아는 문제를 덮는 시간**이다. Pod 강제 삭제 3회: 5xx 0, 대체 Pod Ready 까지 10.8 ~ 11.2초, Kafka 파티션 넘김 1.5 ~ 3.6초 (`--force --grace-period=0` 도 kubelet 이 SIGTERM 을 먼저 보내 정상 탈퇴 — P8-2 E4 의 45.6초와 다른 실험). 같은 태그 재적용은 롤링을 일으키지 않았다 (generation 8 → 8).
- **단계 3 은행 장애 (B1 · B2)**: readiness 에만 `bankConnection`(B1) — 은행 60 · 180초 장애 모두 **게이트웨이 재시작 0**, Ready=False 감지 약 8 ~ 10초, 출금 전부 `COMPLETED` · 이중 0 (180초는 재시도 예산 약 95초를 넘겨 DLT → 제한 재발행 경로까지). 장애 중 1원인증은 503. liveness 에도 넣으면(B2) 60초 장애에 재시작 1, 180초에 **재시작 2**. 은행 복구 뒤 Ready 까지는 B1 6.7 ~ 34.0초 · B2 6.9 ~ 7.5초 — 차이는 설정이 아니라 **게이트웨이 재접속 백오프(1 → 30초 상한)** 때문이다 (재시작이 백오프를 초기화). 그래서 판단 기준은 복구 시간이 아니라 재시작의 부작용이다. 대상 0 인 Service 로의 연결은 **즉시 `Connection refused`** (스펙 §7 의 추정이 맞았다).
- **단계 4 NetworkPolicy (N0 · N1)**: 정책 전 허용 목록 밖 Pod 가 bank-gateway:8082 · mysql:3306 에 **연결됨**(0.0011 · 0.0010초), 정책 후 둘 다 **연결 안 됨**(`0.000000`, curl 종료 코드 28) — **kindnet 이 정책을 집행한다**(Calico 불필요). Pod 7개 전부 Ready 유지, 스모크 [0]~[8] 통과.
- **단계 5 PDB · 노드**: drain(D1) 3회 5xx 0. api 는 요청을 받는 대상 기준 최소 1대(새 Pod Ready 까지 약 11초), Pod Ready 기준 2대(종료 중 Pod 포함) — PDB 는 이번 배치(워커당 1대)에서 개입하지 않았다(두 api 가 한 워커에 있을 때 drain 을 막는 효과는 미검증). 단 게이트웨이가 **`Recreate` 인데도 퇴거 때는 4 ~ 7초간 두 대가 은행에 붙었다**. 노드 정지(D2): 감지까지 약 50초 동안 Envoy 가 사라진 Pod 로 보내 첫 시도 5xx 512 · 포기 144, 대체 api 는 **363초**(기본 퇴거 300초). D2'(퇴거 30초): 대체 **88초**, 5xx 439 · 포기 113 — 퇴거 시간은 복구를 당기지만 감지 전 구간은 그대로다. 포기한 요청은 api 에 닿지 않아 출금 행이 없다 (돈은 정확).

## 구현 · 측정 중 바뀐 것
계획대로 되지 않은 것. 설계 문서를 고친 것은 §16 (계획 단계), 아래는 구현 · 측정 단계의 일이다.
- **`enableServiceLinks: false` 는 앱뿐 아니라 인프라 Pod(MySQL · Kafka · Redis · 모의 은행)에도** (결정 15). 특히 Kafka 는 이미지가 `KAFKA_*` 환경변수를 브로커 설정으로 바꾸므로 Service 환경변수가 섞이면 브로커 설정이 오염된다.
- **모의 은행 = 제어 노드 + PVC `mock-bank-files` + `Recreate`** (결정 14). PV 는 제어 노드에 묶이므로(`kubectl get pv` 의 nodeAffinity) 이 Pod 들은 다른 노드로 못 간다 — 워커 실험에서 제어 노드는 건드리지 않았다.
- **게이트웨이 `Recreate`**: 롤아웃 때 두 대가 동시에 은행에 붙지 않게. 단 D1 에서 **퇴거(eviction)에는 효과가 없음**을 발견했다 (아래 위험).
- **api 분산: `topologySpreadConstraints` 에 `matchLabelKeys: [pod-template-hash]`** (커밋 8aadd27). 롤링 중에는 옛 ReplicaSet 의 Pod 까지 세어 새 Pod 가 옛 Pod 가 있는 워커를 피해 반대쪽으로 몰렸다 — 세 번 새 api 두 Pod 가 한 워커에 떴다. 같은 리비전 Pod 끼리만 세도록 고쳤고, `ScheduleAnyway` 는 유지했다 (워커 하나가 죽어도 두 번째 api 를 띄울 수 있게). 이 문제가 남긴 사례 node-1: 정지한 워커에 api 두 대가 모두 있어 api 0대 구간(+53 ~ +363초)에 첫 시도 5xx 883 · 포기 883 — **분산 배치가 깨지면 이중화는 0 이 된다**의 실측. (돈은 정확: 접수 406 = 출금 행.)
- **`.gitattributes` 에 `*.yaml` · `*.env` · `*.awk` 를 LF 로** (커밋 515e351). Windows 체크아웃의 CRLF 가 `source` 하는 `versions.env` 와 awk 를 깨뜨린다.
- **실험 도구 수정**: 실행 전 리뷰가 찾은 결함 4가지(결과 파일을 판정 전에 씀 · api 분산 사전 확인 없음 · 롤링 기본 부하 60초가 롤링보다 짧을 수 있음 · 장애 중 1원인증 응답 코드 `0000` 이 JSON 을 깨뜨림, 커밋 b9a8f2e). 측정 중에는 사전 확인과 복구 도구가 **종료 중(Terminating) Pod 를 세어** 한 워커에 몰린 상태를 "두 워커"로 통과시킨 것을 고쳤다 (종료 중 Pod 를 빼고 사라질 때까지 기다리고, 몰렸으면 Pod 하나를 지워 다시 나눈다). node-2 는 이 때문에 **무효 회차**로 남겼다.
- **스모크 N0 는 실패 쪽으로 닫는다** (커밋 0633e3c): 확인용 임시 Pod 가 못 뜨거나 빈 값을 내면 "막힘(`0.000000`)"으로 오해해 통과할 수 있었다. 정책이 걸리지 않는 API 서버 `kubernetes.default:443` 에 먼저 붙는지 **양성 대조**를 하고, 숫자일 때만 판정한다. 막힘은 `time_connect` 0 **그리고** curl 종료 코드 28(연결 시간 초과)일 때만이다 — 이름 풀이 실패(6) · 연결 거부(7)는 막힘이 아니다 (관측: 없는 짧은 이름도 풀이 시간 초과로 28 이 나와 대상은 FQDN 으로 준다). probe 를 일부러 깨뜨려 FAIL 로 닫히는 것을 확인했다. 스모크 [0] 은 종료 중인 api Pod 가 없어질 때까지(최대 90초) 기다린다 (롤아웃 직후 [4] 가 가짜로 실패하던 것).
- **B2 는 180초 장애를 추가로 돌렸다**: 60초 장애는 startupProbe 유예(120초) 안이라 재시작 루프가 드러나지 않았고, 재시도 예산(약 95초)을 넘겨 DLT · 재발행 경로가 K8s 에서도 맞는지도 볼 수 있다.
- **대사 파일 볼륨 없음은 그대로** (결정 8). api Pod 에는 `bank-files` 볼륨이 없어 K8s 에서 일마감 대사는 `FILE_NOT_READY` 로 끝난다 (돈 영향 없음).
- Docker Desktop 엔진이 처음에 뜨지 않았다 (secrets-engine 의 깨진 소켓 파일) — 측정과 무관, 해결은 [infra/k8s/README.md](../../infra/k8s/README.md) 문제 해결.

## 위험 · 다음
- **`Recreate` 는 퇴거를 막지 못한다 → 연결 주인이 둘이 될 수 있다.** drain 에서 새 게이트웨이가 +6.0 ~ 7.0초에 Ready, 옛 게이트웨이는 +10.9 ~ 11.8초까지 Ready 라 **4 ~ 7초간 두 대가 은행에 붙었다**. 이번에는 이중 송신 0 (출금 하나는 한 대로만 갔다)이었지만 은행이 기관당 세션 하나만 허용하면 새 연결이 거부되거나 옛 연결이 끊긴다. "은행 연결 주인은 하나" 는 Deployment 설정만으로 지킬 수 없다 → **P8-3b: 리더 선출 · Lease 로 연결 주인 하나** (직접 근거).
- **노드가 사라진 뒤 약 50초는 입구가 모른다.** Envoy 가 사라진 Pod 로 보내 Envoy 연결 시간 초과 약 10초(측정: k6 최대 10.0초 · Envoy 로그 duration 10000ms) → 503 (D2: 5xx 512 · D2': 439). `tolerationSeconds` 는 몇 분 한 대로 버티느냐를 정하고, 첫 50초의 오류는 입구가 이상 대상을 빼야 줄어든다. 다음: Envoy outlier detection(연속 연결 실패 대상 제외) — 재시도가 아니라 대상 제외라 결정 3 과 충돌하지 않는다. 퇴거 30초의 대가는 짧은 네트워크 끊김에도 Pod 를 옮기는 것이다.
- **게이트웨이 재접속 백오프 상한 30초**가 B1 복구 편차(6.7 ~ 34.0초)의 원인이다. 상한을 줄이거나 not ready 동안 짧은 주기로 재접속하는 것은 이번 범위 밖 (앱 코드 변경).
- 대사 파일 공유(RWX · SFTP)는 범위 밖 — api 에 `bank-files` 볼륨이 없어 K8s 에서 일마감 대사는 `FILE_NOT_READY`.
- 범위 밖: egress 정책, 서비스 간 인증(mTLS), 이미지 레지스트리 · 서명 · 스캔, HPA, 파티션 수 늘리기, 관리형 K8s, 지표 · 추적(P9).
- 한 PC · 표본이 작다 (회차당 2 ~ 3회, 노드 정지 D2 · D2' 각 1회). 시간 지표는 kubectl 표본(약 1.4초 간격) 기준 ±1.5초. D2 에서 Kafka 는 2.6초 만에 정상 탈퇴했다(관측). 기제는 kind 의 `docker stop` 이 노드 안 프로세스에 SIGTERM 을 보내기 때문으로 **추정**하며(따로 확인하지 않음), 그렇다면 D2 는 "깔끔한 종료 + 네트워크 소실"이고 전원이 갑자기 꺼지는 진짜 즉사에서는 Kafka 세션 시간 초과(45초, P8-2 E4)가 겹친다 (추정 — 따로 재지 않음). 메모리: 제어 노드 3.09GiB · worker 0.58GiB · worker2 0.80GiB, 합 약 4.5GiB (2026-11-12 부하 없는 상태에서 `docker stats --no-stream` 실측, 부하 중은 재지 않음 · WSL 한도 15.2GiB) — 스펙 §13 의 "5 ~ 7GB (추정)" 은 실측으로 바뀌었다.

## 검토한 대안
- **Helm**: 배포할 차트가 아니라 한 저장소의 매니페스트다. Kustomize 의 overlay 로 실험 변형(`experiment-*`)을 한 줄 단위로 덮는 쪽이 읽기 쉽다.
- **api readiness 에 DB · Kafka · 게이트웨이를 넣기**: 외부 장애에 api 가 동시에 빠져 원인이 안 보이는 503 이 된다 (결정 9).
- **게이트웨이 liveness 에 `bankConnection`**: 은행 점검이 길면 재시작 루프다 (B2 가 재시작 1 → 2 로 보였다).
- **`DoNotSchedule` 분산**: 워커 하나가 죽으면 남은 노드에 두 번째 api 를 못 띄운다 — 이중화가 복구를 막는다.
- **Calico**: N0 에서 kindnet 이 집행함을 확인해 필요 없었다.
- **게이트웨이 롤링 + `maxSurge`**: 배포 중 잠깐 두 대가 은행에 붙는다 (그래도 퇴거에는 `Recreate` 가 못 막는다 — 위 위험).
