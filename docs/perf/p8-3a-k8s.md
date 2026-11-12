# P8-3a Kubernetes (kind) 실험 결과

> 스펙 `docs/superpowers/specs/2026-11-12-p8-3a-k8s-design.md` · 도구 `load/k8s/run.sh` · 원자료 `raw/p8-3a/` (`<동작>-<회차>.json` 요약 · `.k6.json` · `.timeline.txt` 상태 표본 · `.rebalance.txt`)
> 환경: AMD Ryzen 7 9700X (8코어 16스레드), Docker Desktop 4.95.0 (엔진 29.8.2, WSL2 메모리 한도 15.2GB · CPU 16), kind v0.33.0 · 노드 `kindest/node:v1.36.4` · Envoy Gateway v1.9.3 (Envoy 프록시 1대, 제어 노드).
> 부하: k6 2.3.0 (도커 네트워크 `kind` 에서 제어 노드 NodePort 30080 으로), 출금 20건/s 열린 모델, 실패(5xx · 연결 실패)는 같은 멱등키로 1초 뒤 최대 3번 다시. 측정 사용자 `load-p82-u001 ~ u020` (클러스터 DB).
> 판정(모든 회차 공통): 접수된 키 = 출금 행 = 거래번호, 전부 `COMPLETED`, 은행 거래 기록에 빠진 것 0 · 두 번 간 것 0, 잔고 = 원장 합. 통과 못 하면 `.json.invalid`.
> 시간 지표는 kubectl 표본(약 1.4초 간격) 기준이라 ±1.5초. 5xx 는 판정이 아니라 결과다.

## 요약
| 단계 | 결론 |
|---|---|
| 1 이식 | `up.sh` 한 번(이미지 빌드 포함 약 2분)으로 3노드 클러스터 · 인프라 · 앱 · Envoy 입구가 뜨고 스모크 [0]~[7] 통과. api 두 Pod 가 동시에 떠도 Flyway 마이그레이션은 버전마다 1번 |
| 2 프로브 · 종료 | **preStop 10s: 롤링 중 첫 시도 5xx 0 · 0.** preStop 0s: **4 · 9** (전부 Envoy `503 UF` — 이미 포트를 닫은 Pod 로 연결 시도). 둘 다 같은 키 재시도로 전부 접수, 이중 0. Pod 강제 삭제 3회: 5xx 0, 대체 Pod Ready 까지 10.8 ~ 11.2초, Kafka 파티션 넘김 1.5 ~ 3.6초. 같은 태그 재적용은 롤링을 일으키지 않음 |
| 3 은행 장애 | readiness 에만 `bankConnection`(B1): 은행 60 · 180초 장애 모두 **게이트웨이 재시작 0**, 출금 전부 `COMPLETED` · 이중 0 (180초는 DLT → 재발행 경로까지). liveness 에도 넣으면(B2): 60초 장애에 재시작 1, 180초에 **재시작 2**. 은행 복구 뒤 다시 Ready 까지는 B1 6.7 ~ 34.0초 · B2 6.9 ~ 7.5초 — 차이는 설정이 아니라 **게이트웨이 재접속 백오프(1 → 30초 상한)** 때문(재시작이 백오프를 초기화). 대상 0 인 Service 로의 연결은 **즉시 `Connection refused`** |
| 4 NetworkPolicy | (단계 4 에서 채움) |
| 5 PDB · 노드 | (단계 5 에서 채움) |

## 단계 1 — 이식
- `bash infra/k8s/up.sh` (클러스터 있음, 이미지 빌드 · `kind load` 포함) 약 2분 7초 → `bash infra/k8s/smoke.sh` [0]~[7] 통과 (1원인증 VERIFIED · 출금 COMPLETED · 멱등 200 같은 id · `/actuator/info` instance = Pod 이름 두 개 · api 두 Pod 가 다른 워커 · 게이트웨이 readiness UP · Service 전부 ClusterIP · 세 앱 비root).
- Envoy Service 는 `NodePort 80 → 30080` — `EnvoyProxy.envoyService.patch`(StrategicMerge, 공식 문서 예시 없음)가 그대로 동작해 폴백이 필요 없었다. Envoy 프록시 · Envoy Gateway 컨트롤러 · CoreDNS 는 제어 노드.
- **Flyway**: 첫 배포(14:46)에 api 두 Pod 가 동시에 기동했는데 `flyway_schema_history` 는 6행 · 버전 6개 · 실패 0 — 버전마다 한 번만 적용됐다 (DB 잠금으로 한쪽이 기다림). 이후 교체된 Pod 는 모두 "Schema `bank_transfer` is up to date. No migration necessary."
- **ShedLock**: `shedlock` 표 0행 — 일마감 대사 cron 이 01 ~ 06시라 측정 시각(15시대)에는 실행이 없었다. **확인 못 함 (측정 시각)**. 두 Pod 중 하나만 도는지는 P8-2 E5 에서 Compose 로 확인한 것과 같은 코드다.

## 단계 2 — 프로브 · 종료 · 롤링 업데이트
### R1 롤링 업데이트 (preStop 10s vs 0s)
부하 150초 (롤링 기본값 — 롤링이 부하 안에 끝나도록), 20초에 `kubectl set image` 로 api 이미지 태그를 바꾼다 (같은 이미지 · 새 태그 `-r<시각>`). `maxUnavailable 0 · maxSurge 1`.

| 회차 | preStop | 롤링 시간 | 첫 시도 202 | 첫 시도 5xx | 연결 실패 | 같은 키 재시도 | 포기 | api ready 최소 | 이중 · 누락 | p95 |
|---|---|---|---|---|---|---|---|---|---|---|
| steady-1 | 10 | — | 1201 | 0 | 0 | 0 | 0 | 2 | 0 · 0 | 26.9ms |
| steady-2 | 10 | — | 1201 | 0 | 0 | 0 | 0 | 2 | 0 · 0 | 19.9ms |
| rolling-1 | **10** | 21.2s | 3001 | **0** | 0 | 0 | 0 | 2 | 0 · 0 | 24.7ms |
| rolling-2 | **10** | 21.2s | 3001 | **0** | 0 | 0 | 0 | 2 | 0 · 0 | 22.6ms |
| rolling-3 | **0** | 21.2s | 2996 | **4** | 0 | 4 | 0 | 2 | 0 · 0 | 23.8ms |
| rolling-4 | **0** | 21.2s | 2992 | **9** | 0 | 9 | 0 | 2 | 0 · 0 | 21.1ms |

("롤링 시간" = `set image` 부터 `rollout status` 완료까지. 모든 회차 `action_within_load: true` — 롤링 전체가 부하 안.)

**preStop 0s 의 5xx 는 무엇이었나** (Envoy 접근 로그 `response_code_details`, 원자료 `build/load-p83a/rolling-*/envoy.log` → 요약만 여기):

| 회차 | 503 `UF` · Connection refused | 503 `UF` · connection_timeout (10s) |
|---|---|---|
| rolling-3 | 2 | 2 |
| rolling-4 | 9 | 0 |

- `UF` = upstream connection failure — Envoy 가 **아직 대상 목록에 있는 옛 Pod** 에 새 연결을 열다 실패했다. preStop 이 없으면 Pod 삭제와 동시에 SIGTERM → Spring graceful 이 리스너를 닫는데, Envoy 가 EndpointSlice 변경을 받아 그 Pod 를 빼기까지 잠깐 틈이 있다. preStop 10초가 이 틈을 덮는다 (스펙 §6-2 의 (A)·(B) 경쟁을 숫자로 확인).
- 전부 **연결 단계 실패**라 요청이 api 에 닿지 않았다 → 같은 키로 다시 보내도 이중 접수가 아니다 (P8-1 분류 "안 보냄"과 같은 성격). Envoy 는 재시도를 하지 않도록 설정했으므로(스펙 결정 3) 클라이언트에 503 이 그대로 나갔고, k6(클라이언트)가 같은 키로 다시 보내 전부 접수됐다 (포기 0).
- `connection_timeout` 2건은 10초를 기다린 뒤 503 — 사용자 체감이 가장 나쁜 경우. (Envoy 연결 시간 초과 기본 10초, 추정 — 이 값을 줄이는 것은 다음 과제)
- 해석 한 줄: **"preStop 은 앱이 아니라 입구가 늦게 아는 문제를 덮는 시간"**. 0 → 10초로 5xx 4 · 9 → 0 · 0.

### R2 Pod 강제 삭제 (`kubectl delete pod --grace-period=0 --force`, api 하나)
부하 60초, 20초에 삭제.

| 회차 | 첫 시도 5xx | 연결 실패 | api ready 최소 | 대체 Pod Ready 까지 | 옛 Pod 의 파티션 반납 | 남은 Pod 가 받음 | 이중 · 누락 |
|---|---|---|---|---|---|---|---|
| podkill-1 | 0 | 0 | 1 | 11.2s | +1.3s | +1.5s | 0 · 0 |
| podkill-2 | 0 | 0 | 1 | 10.8s | +1.2s | +3.6s | 0 · 0 |
| podkill-3 | 0 | 0 | 1 | 10.8s | +1.3s | +3.0s | 0 · 0 |

(시각은 삭제 명령 기준. 반납 · 받음은 `.rebalance.txt` 의 Kafka 리밸런스 로그 첫 줄)

- **P8-2 E4(Compose `docker kill`, 넘김 45.6초)와 같은 실험이 아니다.** `--force --grace-period=0` 은 API 에서 Pod 를 즉시 지우지만 kubelet 은 컨테이너에 **SIGTERM 을 먼저 보낸다** — 옛 Pod 가 1.3초 만에 스스로 파티션을 반납(정상 탈퇴)한 로그가 그 증거. E4 는 SIGKILL 이라 Kafka 가 `session.timeout.ms`(45초) 를 기다려야 했다. 진짜 즉사(프로세스 · 노드가 사라짐)는 단계 5 의 D2 가 본다.
- 5xx 0: Pod 가 API 에서 사라지는 순간 EndpointSlice 에서도 빠지고, 남은 1대가 다 받았다. 그동안 api ready 1대 (`api_ready_min` 1).
- 자동 재생성: Deployment 가 replicas 2 를 맞추려 새 Pod 를 만들어 약 11초 뒤 Ready — Compose `restart: unless-stopped` 가 `docker kill` 한 컨테이너를 다시 띄우지 않던 것(E4)과의 차이.

### R3 같은 태그로 다시 적용
`apply_overlay local dev-9094f48` (지금 배포된 태그 그대로) → `deployment.apps/api unchanged`, generation 8 → 8, ReplicaSet 목록 그대로. **이미지 내용이 바뀌었어도 태그가 같으면 Pod 스펙이 같아 K8s 는 아무것도 하지 않는다** → 이미지 태그를 커밋마다 바꾸는 규칙(`dev-<짧은 커밋>`, 수정 중이면 `-<시각>`)의 근거.

## 단계 3 — 은행 장애 (게이트웨이 readiness · liveness)
부하 60초 (출금 20건/s), 20초에 `kubectl scale deploy/mock-bank --replicas=0`, `BANK_DOWN_S` 뒤 `--replicas=1`. 모의 은행은 PVC 라 거래 기록이 남는다 (이중 송신 판정 근거). 게이트웨이 readiness = `readinessState,bankConnection` (Task 8 · `ProbeGroupsTest`), liveness 는 기본(B1) 또는 `livenessState,bankConnection`(B2, `experiment-liveness-bank`).

| 회차 | 실험 | 장애 | 게이트웨이 재시작 | Ready=False 감지 | 은행 복구 → Ready | 장애 중 1원인증 | 출금 (접수 = 완료) | DLT 경유 | 완료까지 | 이중 · 누락 |
|---|---|---|---|---|---|---|---|---|---|---|
| bank-1 | B1 | 60s | **0** | +9.3s | 8.0s | 503 | 1200 = 1200 | 0 | 57s | 0 · 0 |
| bank-2 | B1 | 60s | **0** | +10.3s | 34.0s | 503 | 1201 = 1201 | 0 | 79s | 0 · 0 |
| bank-5 | B1 | 180s | **0** | +7.7s | 6.7s | 503 | 1201 = 1201 | 있음 | 259s | 0 · 0 |
| bank-3 | B2 | 60s | **1** (+35.3s) | +8.2s | 6.9s | 503 | 1201 = 1201 | 0 | 57s | 0 · 0 |
| bank-4 | B2 | 180s | **2** (+34.8 · +169.4s) | +7.8s | 7.5s | 503 | 1200 = 1200 | 있음 | 259s | 0 · 0 |

(시각은 은행을 내린 순간 기준, kubectl 표본 ±1.5초. "완료까지" = 부하가 끝난 뒤 모든 출금이 `COMPLETED` 가 될 때까지. 180초 회차는 `action_within_load: false` — 장애가 부하보다 길게 의도한 것. 원자료 `bank-*.json` · `.timeline.txt`)

**확인한 것**
- **readiness 만(B1)이면 은행 장애는 재시작 사유가 아니다**: 60초 · 180초 모두 재시작 0. 게이트웨이는 Ready=False 로 Service 대상에서 빠지고(약 8 ~ 10초 안 — readiness 5초 × 2회), 은행이 돌아오면 다시 붙는다.
- **대상 0 인 Service 로의 연결은 즉시 거부**: 게이트웨이가 not ready 인 동안 api Pod 안에서 `wget http://bank-gateway:8082/…` → `Connection refused` (컨테이너 안 측정 약 1.0초, 이름 풀이 포함 — 추정). 스펙 §7 의 추정 항목 확인. api 쪽 로그는 "bank transfer not sent" (P8-1 분류: 연결 단계 실패 = 안 보냄) → 재시도 토픽.
- **돈은 맞았다**: 다섯 회차 모두 접수 = 출금 행 = 거래번호, 전부 `COMPLETED`, 환불 0, 은행에 두 번 간 거래번호 0. 180초 장애는 재시도 예산(5 · 30 · 60초 ≈ 95초)을 넘겨 DLT 로 갔다가("moved to DLT: bank not connected") 제한 재발행(ADR-0007)으로 끝까지 완료됐다.
- **장애 중 1원인증은 503**: 동기 경로라 은행이 없으면 바로 실패를 알린다 (출금 접수는 DB 만 쓰므로 영향 없음 — 첫 시도 5xx 0).

**B2(liveness 에 bankConnection)는 무엇이 나빴나**
- 재시작이 실제로 일어났다: 60초 장애에 1번, 180초 장애에 2번 — liveness 10초 × 3회 실패로 약 35초 뒤 첫 재시작, 새 프로세스는 startupProbe(120초)가 끝나고 liveness 가 다시 30초 실패하자 또 재시작.
- **그런데 복구 시간은 오히려 짧거나 같았다** (B2 6.9 · 7.5초 vs B1 6.7 ~ 34.0초). 원인은 게이트웨이의 은행 재접속 백오프 — 1초에서 2배씩 늘어 **30초 상한** (`BankGatewayClient.kt:50-51 · 205-206`, 코드 확인됨). 은행이 돌아온 순간 백오프가 큰 단계에 있으면 최대 30초 + readiness 주기 5초를 더 기다린다(bank-2 의 34초). B2 는 재시작이 백오프를 1초로 되돌려 우연히 빨랐다.
- 그래서 판단 기준은 복구 시간이 아니라 **재시작의 부작용**이다 (추정 — 일반 원리, 이 실험에서 따로 재지 않음): 처리 중 요청이 끊기고, JVM 냉시동이 매번 들고, 재시작이 쌓이면 kubelet 의 재시작 대기(back-off, 최대 5분)가 길어져 은행이 돌아온 뒤에도 늦게 살아날 수 있다. 은행이 1시간 점검이면 B2 는 재시작 루프다.
- 다음 과제(이번 범위 밖): 재접속 백오프 상한을 줄이거나 readiness 실패 동안은 짧은 주기로 재접속 — B1 의 복구 편차(6.7 ~ 34초)를 줄인다.

## 측정 중 생긴 일
- 실험 도구 리뷰(opus)가 실행 전에 결함 4가지를 찾아 고친 뒤 측정했다: 결과 파일을 판정 전에 쓰던 것(거짓 통과 파일 가능), api 두 대 분산 사전 확인 없음, 롤링 기본 부하 60초가 롤링보다 짧을 수 있음, 장애 중 1원인증 응답 코드 `0000` 이 JSON 을 깨던 것 (커밋 b9a8f2e).
- Docker Desktop 엔진이 처음에 뜨지 않았다 (`initializing Secrets Engine … engine.sock … The file cannot be accessed by the system`). 깨진 소켓 파일이 든 `%LOCALAPPDATA%\docker-secrets-engine` 폴더 이름을 바꾸고 재시작해 해결 — 측정과 무관.
- 리밸런스 줄 수(회차당 100 ~ 600)는 출금 토픽뿐 아니라 재시도 토픽 3개 · DLT 컨테이너 줄까지 세서 크다. 지표로는 쓰지 않고 시각만 쓴다.
