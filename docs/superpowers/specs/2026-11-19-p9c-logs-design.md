# P9c 로그 모으기 — Grafana Alloy · Loki — 설계

> 날짜: 2026-11-19 · 브랜치 `feat/p9c-logs` · 기준 main `c6ff1bf` (P9b #37 · 정리 #38 머지)
> P9 전체: P9a 지표 · 대시보드 · 알림 (완료) → P9b 분산 추적 (완료) → **P9c 로그 모으기 (이 문서)**.
> 출발점: 로그는 각 Pod 의 표준 출력에만 있다. 출금 한 건의 로그를 보려면 `kubectl logs -l app=api --prefix | grep <traceId>` 를 Pod 마다 돌려야 하고, Pod 가 바뀌면 이전 로그는 사라진다. P9b 로 api · 게이트웨이 로그 줄에 `[traceId-spanId]` 가 붙었지만 (코드 확인 — Spring Boot 기본 텍스트 형식), 로그 ↔ trace 를 오가는 화면은 없다.
> 버전 (2026-11-19 Docker Hub 태그 확인, 2주 이상 지난 안정판): **Loki 3.7.9** (2026-10-20, 4.0 은 rc), **Grafana Alloy v1.21.3** (2026-10-28 — v1.22.0 은 11-13 로 너무 새것). Promtail 은 2026-03-02 지원 종료라 쓰지 않는다.

## 1. 목표와 성공 기준
`bank-transfer` namespace 의 모든 Pod 로그(api · 게이트웨이 · 모의 은행 · Kafka · MySQL · Redis)를 Loki 에 모으고, Grafana 에서 **지표 → trace → 로그** 를 클릭으로 오가게 한다. 민감 정보는 앱과 수집기 두 겹으로 막는다. 실행 환경은 kind(K8s) 만 + 테스트.

성공 기준:
1. 돈 정확성 — 이전 단계와 같은 판정이 모든 실험에서 통과.
2. 출금 traceId 하나로 api · 게이트웨이 로그를 찾고 (모의 은행 줄은 trace 의 `bank.tran_no` 거래번호로), Grafana 에서 로그 → trace · trace → 로그가 양방향으로 이동한다.
3. Loki 에 금지 패턴(§5-3) **0건** — 스모크와 실험 전 기간.
4. 수집 비용: 구조화 로그(JSON)를 켜도 접수 p95 증가 ≤ +10%, 처리량 감소 ≤ −5% (P9b 와 같은 기준).
5. api Pod 강제 종료(L3) · Alloy 재시작(L5) 에서 잃은 줄 0.
6. `ErrorLogSurge` 가 계산값 근처에 도착하고, 은행 끊김 · 리더 없음 중에는 억제된다.
7. CI `k8s-smoke` 20분 안, 스모크 [12] 통과.

## 2. 결정 (브레인스토밍 2026-11-19 승인)
| # | 결정 | 이유 |
|---|---|---|
| F1 | 범위 = **`bank-transfer` namespace 전체 Pod** + 로그 ↔ trace 잇기. monitoring · kube-system · Envoy 로그는 모으지 않는다 | 은행 끊김 · Kafka 리밸런스 같은 장애 때 인프라 로그도 같은 화면에서 본다. 감시 도구 자신의 로그는 잡음이 크다 |
| F2 | 수집 = **Grafana Alloy DaemonSet** (노드마다 1개, 제어 노드 포함 3개)이 `/var/log/pods` 를 읽는다 | K8s 로그 수집의 표준 구조. 표준 출력이면 무엇이든 (Kafka · MySQL · Redis 포함), 앱이 죽기 직전 줄도 파일에 남아 수집된다. 앱이 직접 보내기(OTLP)는 JVM 만 되고 버퍼의 줄을 잃는다. K8s API 로 끌어오기는 API 서버 부하 · Pod 삭제 때 유실 |
| F3 | 저장 = **Loki 3.7.9 단일 바이너리** (monitoring namespace, 제어 노드 고정, emptyDir, 보관 2일), 접속은 port-forward 만 (3100) | Tempo 와 같은 모양. 운영이면 객체 저장소 · 읽기/쓰기 분리 |
| F4 | 형식 = **kind 에서만 api · 게이트웨이 구조화 로그(JSON, ECS)**. 로컬 · IntelliJ · Compose 는 텍스트 그대로 | 필드(traceId · level · logger) 를 정규식 없이 꺼내고, 예외 스택이 한 줄(한 항목)이 된다. P9b 의 "OTLP 주소는 kind 에만" 과 같은 자리. 사람이 읽는 콘솔은 그대로 |
| F5 | 라벨 = `namespace` · `app` · `container` 만. `pod` · `node` · `level` · `trace_id` 는 **구조화 메타데이터** | 라벨은 색인이라 값 종류가 적어야 한다. traceId 를 라벨로 하면 출금마다 새 스트림이 생긴다 |
| F6 | 민감 정보 = **이중 방어**: ① 앱이 남기지 않는다 (사용자 id 를 메시지에서 뺀다) ② Alloy 가 보내기 직전 모든 줄에서 계좌번호 · 1원인증 코드 · 금액 패턴을 가린다 ③ Loki 에서 금지 패턴 0건을 스모크 · 실험으로 증명 | P9b E9 와 같은 목록. 한 겹은 형식이 바뀌면 샌다. 모의 은행 콘솔의 통장 줄은 그대로 (개발 중 인증 코드를 읽는 통로) — Loki 사본만 가린다 |
| F7 | 잇기 = Loki 데이터 소스 **파생 필드**(`trace_id` → Tempo) + Tempo 데이터 소스 **`tracesToLogsV2`**(traceId 로 Loki 질의). 서비스 이름이 아니라 traceId 하나로 찾는다 | trace 의 서비스 이름(`bank-transfer`)과 `app` 라벨(`api`)이 달라 이름 맞추기는 깨지기 쉽다 |
| F8 | 대시보드 ① 출금 흐름에 4행 "로그": 서비스 · 레벨별 로그 양 + api · 게이트웨이 ERROR · WARN 로그 | 지표가 꺾인 시각의 로그를 같은 화면에서 |
| F9 | 로그 알림은 **`ErrorLogSurge` 하나** — Loki ruler → 기존 Alertmanager. 임계값은 실험(L6)의 정상 상태 ERROR 수로 정한다. `BankDisconnected` · `GatewayNoLeader` 가 울리는 동안 억제. 이름표 `source=logs` | 원칙: **업무 신호는 지표로, 로그 알림은 지표로 못 잡는 예상 밖 오류만**. 같은 장애 두 번 울리지 않기 (P9a) |
| F10 | 수집이 실패해도 앱은 그대로 — 앱은 표준 출력에만 쓰고 Loki 를 모른다 | 감시 장치가 감시 대상을 멈추면 안 된다 (P9b E10 과 같은 원칙, 이번엔 구조로 보장) |

## 3. 앱 — 설정 · 코드
| 무엇 | 어디 | 내용 |
|---|---|---|
| 구조화 로그 | kind `app-config` (api · 게이트웨이) | `logging.structured.format.console=ecs` (Spring Boot 3.4+ 기능, 4.1 에서 이름 확인). 서비스 이름 · 환경 필드는 기본값 확인 뒤 필요하면 지정 |
| traceId 필드 | 자동 (Micrometer Tracing 이 MDC 에 `traceId` · `spanId`) | ECS 출력에 MDC 가 어떤 키로 실리는지 계획 단계에서 실제 출력으로 확인 (Alloy 가 그 키를 `trace_id` 메타데이터로 꺼낸다) |
| 사용자 id 빼기 | `RedisWalletDebit` (4곳) · `OptimisticWalletDebit` · `TransferPersistence` 의 지갑 로그 | 메시지에서 사용자 id 를 뺀다. 같은 줄에 traceId 가 있으니 찾기는 그대로 된다 |
| 다시 훑기 | 모든 `log.*` 호출 | 예금주 · Idempotency-Key · 금액 · 계좌번호 전체가 찍히는 곳이 없는지 (계획 단계에서 목록으로) |
| 모의 은행 | 그대로 | 순수 logback 기본 형식 (Spring 아님). 통장 줄 `[통장] <계좌> 입금 <금액>원 적요='<BT+4자리>' 거래번호=...` 은 콘솔에 그대로 둔다 |

## 4. 인프라 (`infra/k8s/monitoring`)
- `loki.yaml` + `loki/loki.yml`: 단일 바이너리(`-target=all`), 파일시스템 저장 · emptyDir, `retention_period: 48h` (compactor), 제어 노드 고정 (Tempo 와 같은 nodeSelector · toleration), ruler 켬 (규칙은 ConfigMap, `alertmanager_url` = 기존 Alertmanager 서비스). 리소스 요청 · 상한은 Tempo 와 비슷하게 두고 L2 로 확인.
- `alloy.yaml` + `alloy/config.alloy`: DaemonSet, ServiceAccount + ClusterRole(노드 · Pod 읽기 — `discovery.kubernetes` 용), hostPath `/var/log/pods` 읽기 전용, 위치 기록은 hostPath(`/var/lib/alloy`), 제어 노드 taint 허용. 파이프라인: `discovery.kubernetes`(bank-transfer 만) → `discovery.relabel`(라벨 3개 + 메타데이터) → `loki.source.file` → `loki.process`(JSON 이면 필드 꺼내기 · 텍스트면 정규식으로 level · traceId, **가리기는 trace_id 를 꺼낸 다음**) → `loki.write`.
- Grafana: Loki 데이터 소스(파생 필드) 추가, Tempo 데이터 소스에 `tracesToLogsV2`. 대시보드 ① 에 4행.
- Alertmanager: 억제 규칙 하나 추가 (`BankDisconnected` · `GatewayNoLeader` → `ErrorLogSurge`).
- `versions.env`: `LOKI_VERSION=3.7.9` · `ALLOY_VERSION=v1.21.3`. `up.sh` · `down.sh` 는 지금 구조 그대로 (kustomization 에 추가).

## 5. 로그 형식 · 라벨 · 가리기 (요지)
### 5-1. 라벨과 메타데이터
| 이름 | 종류 | 값 |
|---|---|---|
| `namespace` | 라벨 | `bank-transfer` |
| `app` | 라벨 | Pod 의 `app` 라벨 — api · bank-gateway · mock-bank · kafka · mysql · redis |
| `container` | 라벨 | 컨테이너 이름 |
| `pod` · `node` | 메타데이터 | Pod · 노드 이름 |
| `level` | 메타데이터 | JSON 의 level, 텍스트는 정규식 (없으면 비움) |
| `trace_id` | 메타데이터 | JSON 의 traceId, 텍스트는 `[<32 hex>-<16 hex>]` |

### 5-2. 줄 모양
- api · 게이트웨이 (kind): JSON 한 줄 (ECS). 예외 스택은 필드 하나.
- 모의 은행 · Kafka · MySQL · Redis: 원래 텍스트. 여러 줄 스택은 줄마다 따로 들어간다 (한계).

### 5-3. 가리기 (Alloy, 모든 줄, 보내기 직전)
| 패턴 | 바꿈 | 노리는 것 |
|---|---|---|
| 10 ~ 14자리 연속 숫자 | 끝 4자리만 남기고 `*` | 계좌번호 |
| `BT` + 숫자 4자리 | `BT****` | 1원인증 코드 (적요) |
| 숫자 + `원` | `***원` | 금액 |

- 더 가리는 쪽을 받아들인다: 13자리 epoch 밀리초 · Kafka 오프셋 같은 긴 숫자도 가려진다. traceId 는 가리기 전에 메타데이터로 꺼내 둔다.
- "금지 패턴 0건" 검사는 위 세 패턴의 **가리지 않은 모양**을 Loki 에서 LogQL 정규식으로 센다.

## 6. 잇기 · 대시보드 · 알림
- 로그 → trace: Loki 파생 필드 — 메타데이터 `trace_id` 를 Tempo 내부 링크로.
- trace → 로그: Tempo `tracesToLogsV2` — 질의 `{namespace="bank-transfer"} | trace_id="${__span.traceId}"`, 구간 앞 1분 · 뒤 1분.
- 지표 → trace: P9b exemplar 그대로.
- 대시보드 ① 4행: (A) `sum by (app, level)` 로그 양 막대, (B) api · 게이트웨이 ERROR · WARN 로그 패널.
- `ErrorLogSurge`: `sum by (app) (count_over_time({namespace="bank-transfer", app=~"api|bank-gateway"} | level="ERROR" [5m])) > N`, `for: 2m`, `severity=warning`, `source=logs`. N 은 L6 ① 에서 정한다 (정상 상태 최대의 몇 배 — 계획 · 실험에서 확정, 스펙 §14 에 기록).

## 7. 실험 (`load/k8s/run.sh` 에 `PHASE=p9c`, 출금 20건/s)
| # | 무엇을 | 보는 것 |
|---|---|---|
| L1 | 수집 지연 | 줄의 시각 → Loki 검색 가능까지 p50 · p95 |
| L2 | 비용 | 서비스별 로그 양(줄 · 바이트/s), Loki · Alloy CPU · 메모리, JSON 켬 vs 끔의 접수 p95 · 처리량 |
| L3 | api Pod 강제 종료 (`--force --grace-period=0`) | 그 Pod 의 노드 로그 파일 줄 수 vs Loki 줄 수 (잃음 0) |
| L4 | Loki 0대 60초 | 돈 · p95 그대로, 그동안 쓴 줄 중 복구 뒤 들어온 수 (기준 없이 기록) |
| L5 | Alloy Pod 재시작 | 잃음 0, 중복 줄 수 |
| L6 | ErrorLogSurge | ① 정상 상태 ERROR 수 → N ② ERROR 를 내는 장애로 도착 시각 (어떤 장애인지는 계획에서) ③ 은행 2분 끊김 동안 억제 |
| L7 | 가리기 | 전 기간 금지 패턴 0건 (1원인증 포함) |

모든 회차에 돈 정확성 판정. 원자료 `docs/perf/raw/p9c/`, 결과 `docs/perf/p9c-logs.md`.

## 8. 테스트
- 앱: 구조화 로그 설정에서 구간 안 로그 한 줄이 JSON 이고 traceId 필드를 가진다 (OutputCapture). 지갑 로그에 사용자 id 가 없다.
- 설정 검사 (`infra/k8s/monitoring/check.sh`, CI `build`): Loki 설정 검증, 규칙 파일 문법, Alloy 설정 문법, 대시보드 LogQL. 가능하면 Alloy 가리기를 표본 줄로 실제 돌려 본다 (안 되면 스모크가 맡는다).

## 9. CI · 스모크 [12]
Loki Ready · Alloy 3/3 Ready → 스모크 출금의 traceId 로 Loki 검색 (api · 게이트웨이 줄, 수집 지연만큼 다시 묻기) → 그 출금의 모의 은행 통장 줄이 가려진 채로 있음 → 금지 패턴 0건 → `ErrorLogSurge` 규칙 적재 · Grafana Loki 데이터 소스 정상. CI `k8s-smoke` 20분 제한 그대로.

## 10. 위험
- **R1 hostPath**: Alloy 가 노드의 로그 디렉터리를 읽는다 — 읽기 전용 마운트, 다른 namespace 는 발견 단계에서 버린다. kind 노드의 로그 경로가 `/var/log/pods` 가 맞는지 첫 작업에서 확인.
- **R2 과한 가리기**: 긴 숫자 규칙이 진단에 필요한 숫자(오프셋 · 시각)를 지운다 — 받아들이되 문서에 적는다. 가려서 trace 링크가 깨지지 않게 순서를 지킨다.
- **R3 수집 지연**: Loki 도 Tempo 처럼 몇 초 늦다 — 스모크 · Postman 은 다시 묻는다.
- **R4 emptyDir**: Loki Pod 가 바뀌면 로그가 사라진다 (L4 에서 확인). 운영이면 객체 저장소.
- **R5 JSON 비용**: 구조화 로그 직렬화가 요청 스레드에서 돈다 — L2 로 잰다.
- **R6 CI 시간**: 이미지 두 개(Loki · Alloy)와 DaemonSet Ready 대기가 늘어난다 — 20분 제한 안인지 PR CI 로.
- **R7 ruler 평가 비용**: 5분 창을 1분마다 훑는다 — 규칙 하나라 작다 (L2 로 Loki CPU 확인).

## 11. 범위 밖 ("실제 운영이라면")
monitoring · kube-system 로그, Loki 객체 저장소 · 분산 모드 · 멀티 테넌트, 로그 보관 정책(법정 보관 기간 · 별도 감사 로그), Kafka 여러 줄 스택 묶기, 로그에서 지표 만들기(recording rule), Compose 환경의 로그 모으기, monitoring namespace NetworkPolicy (P9a 에서 넘긴 일 — 그대로 남김), Grafana 로그인 · 권한.

## 12. 변경 목록 (요지)
- 앱: 지갑 로그 메시지, kind `app-config` 구조화 로그, 테스트.
- 인프라: `monitoring/loki*` · `alloy*` · Grafana 데이터 소스 · 대시보드 ① · Alertmanager 억제 · `kustomization` · `versions.env` · `check.sh` · `smoke.sh` [12].
- 실험: `load/k8s/run.sh` `PHASE=p9c` · 수집 도구.
- Postman: `10. 로그 (P9c, kind)`.
- 문서: ADR-0022, OPERATION §19, ARCHITECTURE 로그 절, 구성도 두 장(`runtime-k8s.svg` 에 Loki · Alloy, `app-architecture.svg` 에 로그 흐름), CHANGELOG · HANDOVER · README.

## 13. 작업 순서
인프라(Loki · Alloy · 라벨 · 가리기) → 앱(구조화 로그 · 사용자 id) → 잇기(데이터 소스) → 대시보드 · 알림 → 검사 · 스모크 [12] → 실험 도구 · 실험 → Postman → 문서 · 구성도 → PR.

## 14. 계획 단계에서 고친 점
(계획을 쓰면서 채운다.)
