# P9b 분산 추적 — OpenTelemetry (Micrometer Tracing) · Grafana Tempo — 설계

> 날짜: 2026-11-18 · 브랜치 `feat/p9b-tracing` · 기준 main `19ff390` (P9a #35 · 정리 #36 머지)
> P9 전체: P9a 지표 · 대시보드 · 알림 (완료) → **P9b 분산 추적 (이 문서)** → P9c 로그 모으기.
> 출발점: P9a 로 "어느 단계가 막혔나" 는 보이지만, **출금 한 건이 어디서 얼마나 걸렸나** 는 여전히 로그를 손으로 맞춰야 한다. 출금은 HTTP 접수 → DB 아웃박스 → 0.5초 뒤 발행기 → Kafka → 컨슈머(다른 api 일 수 있음) → 게이트웨이(HTTP) → 은행(Netty TCP 전문) 으로 **스레드 · 프로세스가 여러 번 바뀐다**. 앱에 추적 계측은 없다 (코드 확인 — 의존성에 tracing · opentelemetry 없음).
> Spring Boot 4.1.1 (BOM: Micrometer Tracing 1.7.1 · OpenTelemetry 1.62.0, `spring-boot-starter-opentelemetry` 있음 — 로컬 BOM 확인). 설정 이름은 Boot 4.1 참고 문서로 확인 (아래 §3 표의 "확인" 열).

## 1. 목표와 성공 기준
출금 한 건이 **접수부터 은행 전문까지 한 줄의 이야기(trace 하나)** 로 보이게 하고, 1원인증 동기 경로도 추적한다. 로그 줄에 traceId 를 찍어 trace ↔ 로그를 오갈 수 있게 한다. 실행 환경은 kind(K8s) 만 + 통합 테스트.

성공 기준:
1. 돈 정확성 — 이전 단계와 같은 판정이 모든 실험에서 통과.
2. 출금 1건 = trace 1개가 **프로세스 2종(api · bank-gateway) · 스레드 전환 3번 이상**(접수 → 발행 → 컨슈머 → 게이트웨이)을 끊김 없이 잇는다 — 통합 테스트(메모리 내보내기)와 kind 스모크 [11] 둘 다로 확인.
3. 재발행 · 결과조회(정리 작업)는 `transfer.id` 한 번 검색으로 찾는다.
4. 추적 비용 (T1, 한 PC 상대 비교): 접수 p95 증가 **≤ 10%**, 처리량 감소 **≤ 5%**.
5. CI `k8s-smoke` 가 20분 제한 안에서 스모크 [11] 포함 통과.

## 2. 결정 (브레인스토밍 2026-11-18 승인)
| # | 결정 | 이유 |
|---|---|---|
| E1 | 범위 = **출금 전 구간 + 1원인증 + 로그 traceId** | "접수한 그 건이 은행까지 어떻게 갔나" 가 목적. 동기 HTTP 만으로는 Kafka 에서 끊긴다 |
| E2 | 백엔드 = **Grafana Tempo** (Jaeger 아님) | 이미 있는 Grafana 에서 본다. 지표 → trace(exemplar), P9c 로그(Loki)까지 한 화면 |
| E3 | kind 에만 + 테스트 (Compose · IntelliJ 는 보내기 끔) | P9a 와 같은 자리. 설정 한 벌. 문맥 전달은 통합 테스트로 증명 |
| E4 | 계측 = **Micrometer Tracing + OpenTelemetry 브리지** (`spring-boot-starter-opentelemetry`), Java 에이전트 아님 | P9a 지표와 같은 Observation 한 벌. 테스트로 "같은 traceId" 를 증명할 수 있다. 에이전트도 아웃박스는 못 잇는다(어차피 손코드) |
| E5 | Collector 없이 앱 → Tempo OTLP(HTTP) 직접 | 지금은 Collector 가 할 일이 없다 (꼬리 샘플링은 범위 밖). 나중에 주소 한 줄로 끼운다 |
| E6 | 아웃박스 행에 **`trace_parent`** (W3C) 를 저장하고, 발행기가 행마다 그것을 부모로 복원 | 접수 → 발행 사이 "DB 에 쓰고 나중에 다른 스레드가 꺼냄" 틈을 프레임워크는 모른다 |
| E7 | 정리 작업(재발행 · 결과조회)은 **실행 1회 = 새 trace**, 처음 trace 와는 `transfer.id` 속성으로 묶는다 (같은 trace 로 늘리지 않고, 이체 표도 안 바꿈) | 수 분짜리 trace 는 보기 · 샘플링이 어렵다. 스키마 변경은 아웃박스 컬럼 하나 |
| E8 | 은행 전문에는 추적을 싣지 않고, 게이트웨이의 은행 호출 한 점(`withClient`)에서 `bank.request` 구간을 직접 연다 | 전문 규격에 자리가 없다. 모의 은행 = 남의 시스템 (계측 안 함) |
| E9 | 구간 속성에 **금액 · 계좌번호 · 예금주 이름 · 1원인증 코드 · Idempotency-Key 를 넣지 않는다** | 민감정보 (P9a D7 과 같은 원칙). 넣는 것: id · 전문번호 · 결과 · 사유 · 종류 |
| E10 | 추적 실패는 돈 흐름을 바꾸지 않는다 — 보내기는 비동기 묶음(대기열 상한, 넘치면 버림), 틀린 `trace_parent` 는 새 trace | 감시 장치가 감시 대상을 멈추면 안 된다 |

## 3. 앱 — 의존성 · 설정
- `apps/api` · `apps/bank-gateway` 에 `implementation("org.springframework.boot:spring-boot-starter-opentelemetry")` (버전은 Boot BOM). `apps/api-reactive` · `apps/mock-bank` 는 대상 아님 (api-reactive 는 kind 에 배포되지 않는 P6-2 비교 앱).
- 설정 (기본 = 꺼짐, kind 의 `app-config` 에서 켬):

| 목적 | 속성 | 기본(application.yml) | kind | 확인 |
|---|---|---|---|---|
| 보내기 주소 (HTTP) | `management.opentelemetry.tracing.export.otlp.endpoint` | (비움) | `http://tempo.monitoring.svc.cluster.local:4318/v1/traces` | Boot 4.1 문서 |
| 보내기 켜기 | `management.tracing.export.enabled` | `false` | `true` | Boot 4.1 문서 |
| 샘플링 | `management.tracing.sampling.probability` | `1.0` (테스트가 모든 구간을 본다) | `1.0` | Boot 4.1 문서 (기본값 0.1) |
| 로그 상관 | `logging.pattern.correlation` | Boot 기본 (`[traceId-spanId]`) | 같음 | Boot 4.1 문서 |
| Kafka 관측 | `spring.kafka.template.observation-enabled` · `spring.kafka.listener.observation-enabled` | `true` | `true` | **계획 단계에서 확인** (Boot 3 이름 — 4.1 Integration Properties 에서 확인, 없으면 컨테이너 팩토리 · 템플릿에 코드로 `setObservationEnabled(true)`) |
| 리액터 문맥 | `spring.reactor.context-propagation` | `auto` (게이트웨이 WebFlux 판 · api WebClient) | 같음 | Boot 4.1 문서 |

- 서비스 이름은 `spring.application.name` — 지금 api = `bank-transfer`, 게이트웨이 = `bank-gateway` (코드 확인). 그대로 쓴다 (P9a 지표의 `application` 태그와 같은 값).
- 테스트: `micrometer-tracing-test` 류의 메모리 내보내기(또는 OpenTelemetry SDK `InMemorySpanExporter`)로 구간을 모아 단언 — 정확한 의존성은 계획에서 BOM 으로 확인.

## 4. 자동으로 안 이어지는 곳 (손으로 잇는 4곳)
### 4-1. 아웃박스 (E6)
- `V7__outbox_trace_parent.sql`: `ALTER TABLE outbox_event ADD COLUMN trace_parent VARCHAR(55) NULL` (W3C `00-<32hex>-<16hex>-<2hex>` = 55자).
- 쓰기: 저장소가 아웃박스 행을 만드는 곳(접수 `saveWithEvent` · 재발행 `republish` 둘 다 `outboxRow(...)` 한 곳)에서 **현재 구간의 traceparent** 를 넣는다. 현재 구간이 없으면 NULL.
- 읽기: 발행기 `SELECT_PENDING_SQL` 이 `trace_parent` 도 읽고, 행마다 그것을 부모로 하는 범위 안에서 `kafka.send` → Kafka producer 관측이 자식 구간 + 헤더 `traceparent` 를 만든다. 발행기 주기 실행 자체는 관측하지 않는다 (0.5초마다 빈 trace 가 쌓이지 않게).
- 복원 실패(NULL · 형식 틀림)는 부모 없이 보낸다 → 컨슈머 쪽이 새 trace 로 시작. 예외 없음.

### 4-2. 정리 작업 (E7)
- `TransferReconciler` 의 한 번 실행(스케줄)이 trace 하나. 재발행으로 쓴 아웃박스 행에는 그 실행의 문맥이 저장된다 → 재발행 뒤 Kafka → 은행 구간은 정리 작업 trace 에 이어진다.
- 결과조회(inquiry) 호출도 그 실행 trace 의 자식. 건마다 `transfer.id` 속성.

### 4-3. 은행 호출 (E8, 게이트웨이)
- `BankConnectionOwner.withClient(type)` 에서 `bank.request` 구간(Observation)을 열고, future 가 끝날 때 닫는다 (P9a 지표와 같은 자리 · 같은 결과 분류). 이벤트 루프 스레드에서 끝나므로 구간 객체를 직접 들고 닫는다 (스레드 지역 문맥에 기대지 않음).
- 속성: `bank.request.type` (transfer · inquiry · name), `bank.tran_no` (전문번호), `bank.outcome` (ok · rejected · not_sent · unknown), `bank.not_sent.reason` (안 보냄일 때), `transfer.reference` (이용기관 거래번호 — 결과조회 키, 개인정보 아님. 게이트웨이는 이체 id 를 모르므로 이 값으로 api 쪽 구간과 맞춘다).

### 4-4. 기본이 아닌 판
- 코루틴 컨슈머 (`transfer.consumer.mode=coroutine`): suspend 지점에서 스레드가 바뀌어도 문맥이 따라가게 Spring 의 코루틴 문맥 전달(관측 문맥을 코루틴 문맥 요소로) — 정확한 API 는 계획에서 확인.
- JDK HttpClient 게이트웨이 클라이언트 (`bank.gateway.http.client=jdk`): 자동 계측이 없으므로 클라이언트 구간을 직접 열고 전파기로 `traceparent` 헤더를 넣는다.
- WebClient 판(기본)은 **자동 구성된 `WebClient.Builder` 로 만들어야** 전파된다 (Boot 문서) — 지금 만드는 방식을 계획에서 확인.

## 5. 구간 이름 · 속성 (요지)
| 구간 | 어디서 | 종류 | 주요 속성 |
|---|---|---|---|
| `http post /api/v1/transfers` | api (Spring MVC 자동) | server | `transfer.id` (접수 뒤 추가) |
| `transfer.requested send` | api 발행기 (Kafka 자동, 부모 = 저장된 traceparent) | producer | `transfer.id`, `outbox.event_id` |
| `transfer.requested receive/process` (+ retry-N · dlt) | api 컨슈머 (Kafka 자동) | consumer | `transfer.id`, `transfer.reference`, 시도 토픽 |
| `http post /internal/v1/bank/transfers` | api → 게이트웨이 (WebClient 자동 / JDK 손코드) | client → server | — |
| `bank.request` | 게이트웨이 `withClient` (손코드) | internal | 위 §4-3 |
| `transfer.reconcile` | api 정리 작업 1회 | internal (root) | 처리 건수 |
| `http post /api/v1/account-verifications` … | api · 게이트웨이 | server · client · internal | `verification.id` |
금지 속성 (E9): 금액 · 계좌번호 · 예금주 이름 · 1원인증 코드 · Idempotency-Key · 사용자 id 는 넣지 않는다 (사용자 id 는 헤더값 그대로라 넣지 않음).

## 6. 인프라 (`infra/k8s/monitoring`)
- **Tempo** Deployment 1 (제어 노드, `nodeSelector bank-transfer/role: infra` · control-plane toleration — P9a 와 같은 자리), 단일 바이너리 · 로컬 저장 emptyDir · 보관 2일, 포트 OTLP HTTP 4318 · 조회 3200. 이미지 태그는 `versions.env` `TEMPO_VERSION` (계획에서 `docker manifest inspect` 로 존재 확인, 다운로드는 사용자 허락).
- Grafana 데이터 소스 `Tempo` 추가 (프로비저닝 YAML), Explore 에서 TraceQL. 서비스 맵 · 구간 지표 생성(metrics-generator)은 범위 밖.
- **(덤) exemplar**: Prometheus `--enable-feature=exemplar-storage` + OpenMetrics 수집, Grafana Prometheus 데이터 소스에 exemplar → Tempo 연결, 대시보드 "접수 → 확정 시간" 패널에 exemplar 표시. Micrometer 가 tracing 있을 때 exemplar 를 내는지 계획에서 확인 (`management.tracing.exemplars.include` 기본 `sampled-traces`). 막히면 뺀다 (성공 기준 아님).
- NetworkPolicy: 앱 → Tempo 는 `bank-transfer` namespace 의 egress 제한이 없어 그대로 된다 (ingress 정책만 있음 — 확인). monitoring namespace 정책은 P9a 에서 넘긴 일(범위 밖).
- `up.sh` · `smoke.sh` · `lib.sh wait_monitoring` 에 tempo 추가.

## 7. 실험 (`load/k8s/run.sh` 에 `PHASE=p9b`, 출금 20건/s)
| # | 동작 | 볼 것 |
|---|---|---|
| T1 | 추적 켬 vs 끔 (`management.tracing.export.enabled` / 샘플링 0) 각 2회, 동작 없음(steady) | 접수 p95 · 처리량 · api CPU — **추적 비용** (성공 기준 4) |
| T2 | 정상 출금 trace 모양 | 구간 수, 접수 → 발행 틈(발행 주기 0.5초 근처), 컨슈머 → 은행 시간 — Grafana 캡처 · Tempo API 원자료 |
| T3 | 모의 은행 2분 끊기 | 재시도 체인(retry-0 · 1 · 2 · DLT)이 같은 trace 에 붙는지, 재발행 trace 를 `transfer.id` 로 찾는지 (성공 기준 3) |
| T4 | 게이트웨이 리더 Pod 삭제 | 교대 중 요청의 `bank.request` 에 `not_leader` · `stepping_down` 이 보이는지 |
| T5 | (덤) exemplar | 대시보드 점 → trace 이동 |
원자료 `docs/perf/raw/p9b/`, 문서 `docs/perf/p9b-tracing.md`. Tempo 조회는 Prometheus 처럼 `kubectl exec` + `wget` (PC 포트를 열지 않음).

## 8. 테스트
- 단위: traceparent 쓰기 · 복원(NULL · 틀린 형식 → 새 trace, 예외 없음), `bank.request` 구간이 결과 4종 · 사유를 속성으로 갖고 future 완료 때 닫힘.
- 통합 (Testcontainers, 메모리 내보내기):
  1. 출금 1건 — api server → producer → consumer → client → 게이트웨이 server → `bank.request` 가 같은 traceId, 부모-자식 순서.
  2. 은행 끊김 — 재시도 토픽 처리 구간이 같은 trace.
  3. 재발행 — 새 trace, 같은 `transfer.id`.
  4. 1원인증 — 동기 경로 한 trace.
  5. 비교 판 — 코루틴 컨슈머 · JDK 클라이언트도 traceId 가 이어짐 (각 1개).
  6. 추적 끔(샘플링 0)에서 기존 흐름 테스트가 그대로 통과.
  7. 금지 속성(E9)이 어떤 구간에도 없음.
- 게이트웨이 별도 JVM 테스트(P8-1 방식)로 api ↔ 게이트웨이 HTTP 전파 확인 — 계획에서 기존 흐름 테스트 구조에 맞춤.

## 9. CI · 스모크 [11]
`smoke.sh` [11]: tempo Ready → 스모크 출금 1건의 `transfer.id` 로 Tempo 검색(`/api/search?q={ span.transfer.id = "…" }` 또는 tag 검색) → trace 의 서비스가 api · bank-gateway 둘 다, 구간 수 ≥ 5 → api 로그에 그 traceId 가 있음. `k8s-smoke` 20분 제한 (이미지 1개 추가).

## 10. 위험
| # | 위험 | 대응 |
|---|---|---|
| C1 | 아웃박스 발행기가 행마다 문맥을 바꾸며 보내는 비용 · 실수(문맥이 다음 행으로 샘) | 행마다 범위를 열고 닫는다(try-with-resources 류), 테스트 1 이 부모를 단언 |
| C2 | 재시도 토픽 · DLT 로 헤더가 넘어갈 때 traceparent 가 덮이거나 빠짐 | 테스트 2. Spring Kafka 재시도 발행도 관측 템플릿을 쓰는지 계획에서 확인 |
| C3 | 추적 비용 (구간 생성 · 내보내기) | T1 측정, 샘플링 손잡이 |
| C4 | Netty 이벤트 루프에서 구간을 닫을 때 스레드 지역 문맥 누수 | 구간 객체를 직접 들고 닫음 (§4-3) |
| C5 | 코루틴 · 리액터에서 문맥 유실 → trace 가 끊김 | 테스트 5, `spring.reactor.context-propagation=auto` |
| C6 | 민감정보가 속성 · 로그로 샘 | E9 + 테스트 7 |
| C7 | Tempo 장애가 앱을 느리게 함 | 비동기 묶음 · 대기열 상한 (E10), 실험 중 Tempo 0대 확인은 T1 보조 1회 |
| C8 | 로그 패턴 변경이 기존 로그 파싱(실험 도구 grep)을 깸 | 실험 도구의 로그 grep 문자열 점검 (계획) |

## 11. 범위 밖 ("실제 운영이라면")
OpenTelemetry Collector · 꼬리 샘플링, JDBC · SQL 구간, Redis 구간, 모의 은행 계측, 서비스 맵 · 구간 지표(metrics-generator), 영구 저장 · 원격 저장, 로그 모으기(P9c), Compose 추적.

## 12. 변경 목록 (요지)
- 앱: `apps/api` (의존성 · 설정 · 아웃박스 traceparent 쓰기 · 발행기 복원 · 정리 작업 구간 · JDK 클라이언트 전파 · 코루틴 문맥), `apps/bank-gateway` (의존성 · 설정 · `bank.request` 구간), `modules/bank-gateway-http` (JDK 판 전파 — 위치는 계획), DB `V7__outbox_trace_parent.sql`.
- 인프라: `infra/k8s/monitoring` (Tempo · Grafana 데이터 소스 · (덤) Prometheus exemplar), `base/config.yaml` app-config 추적 켜기, `versions.env` `TEMPO_VERSION`, `up.sh` · `smoke.sh` [11] · `lib.sh`.
- 실험: `load/k8s` `PHASE=p9b`.
- 문서: ADR-0021, OPERATION §18, ARCHITECTURE, README 로드맵 P9b, HANDOVER, CHANGELOG, 구성도 두 장(`runtime-k8s.svg` 에 Tempo, `app-architecture.svg` 에 trace 경로), `docs/database.md` (V7).

## 13. 작업 순서
1. 의존성 · 설정 (끔 기본) + 메모리 내보내기 테스트 토대 → 2. 자동 구간 확인(HTTP · Kafka) → 3. 아웃박스 traceparent (V7 · 쓰기 · 복원) → 4. 정리 작업 구간 → 5. 게이트웨이 `bank.request` → 6. 비교 판(코루틴 · JDK) → 7. 금지 속성 · 끔 테스트 → 8. 인프라(Tempo · Grafana · exemplar) → 9. 스모크 [11] · CI → 10. 실험 도구 `PHASE=p9b` → 11. 실험 T1 ~ T5 · 측정 문서 → 12. 문서 · 구성도.

## 14. 계획 단계에서 고친 점 (2026-11-18)
구현 계획(`docs/superpowers/plans/2026-11-18-p9b-tracing.md`)을 쓰며 실제 jar(메타데이터 · 바이트코드) · 코드로 확인한 결과다. 이 절이 앞 절과 다르면 이 절이 맞다. 번호는 계획의 "계획에서 정한 것" 과 같다.

1. **Kafka 관측 속성 확인** (§3 표 "계획 단계에서 확인") — `spring.kafka.template.observation-enabled` · `spring.kafka.listener.observation-enabled` 가 Boot 4.1.1 에 있고 기본 false. yml 두 줄로 켠다 (코드 불필요). 재시도 · DLT 발행은 자동 구성 템플릿을 쓰고 Spring Kafka 4.1.1 은 오류 처리기를 소비 구간 범위 안에서 부른다 → 재시도 체인은 같은 trace (위험 C2 확인).
2. **§3 표의 "보내기 켜기 `management.tracing.export.enabled` 기본 false" 를 버린다** — Boot 4.1 에서 이 값은 전파기까지 끈다. 보내기는 **주소(`management.opentelemetry.tracing.export.otlp.endpoint`)가 있을 때만** 생기므로 앱 기본 = 주소 없음, kind 의 app-config 만 주소를 준다. 구간 · 전파 · 로그 상관은 어디서나 켜져 있다. T1 의 "끔" = 샘플링 0.
3. **테스트 의존성 확정** (§3 끝) — `spring-boot-micrometer-tracing-test` 는 넣지 않는다 (`@AutoConfigureTracing` 없는 테스트마다 export.enabled=false → 2 때문에 전파가 꺼진다). `io.opentelemetry:opentelemetry-sdk-testing` 의 `InMemorySpanExporter` 를 `SimpleSpanProcessor` 빈으로 단다.
4. **OTLP 지표 끔** (새로) — 스타터가 `micrometer-registry-otlp` 도 끌어와 1분마다 `localhost:4318` 로 지표를 민다 → `management.otlp.metrics.export.enabled=false` (지표는 P9a Prometheus).
5. **@Scheduled 관측 끔** (§4-1 "발행기 주기 실행은 관측하지 않는다" 의 방법) — Boot 가 @Scheduled 실행마다 관측한다 → `management.observations.enable.[tasks.scheduled.execution]=false` (이 지표는 쓰지 않는다). 정리 작업 root 는 코드가 연다.
6. **actuator 구간은 내보내지 않는다** (새로) — 프로브 · Prometheus 수집 요청. 지표는 그대로.
7. **내보내기 앞 지우개 + 공용 모듈 `modules/tracing`** (새로, E9) — 예외를 구간에 남기면 메시지 · 스택 · 상태 설명이 같이 간다 (예: 잔고 부족 메시지의 사용자 id · 금액, 검증 메시지의 거절된 계좌번호). 보내기 전에 `exception.type` 만 남긴다. 두 앱이 같이 쓴다.
8. **아웃박스 복원 = 행마다 `outbox.publish` 구간** (§4-1 · §5 표) — 저장한 traceparent 를 부모로 짧은 구간을 열고 그 안에서 send. Kafka 자동 producer 구간은 사용자 속성을 받지 못해 `transfer.id` · `outbox.event_id` 를 이 구간에 둔다. 구간 표에 한 줄 는다.
9. **WebClient** (§4-4) — 게이트웨이 클라이언트는 `modules/bank-gateway-http` 에서 `WebClient.builder()` 로 직접 만든다 (Boot Builder 아님) → `HttpBankGateway.create(kind, settings, observations)` 로 관측 레지스트리를 넘긴다. JDK 판은 보내는 쪽 관측(`banktransfer.gateway.http.client`)을 손으로 — 모듈은 micrometer-observation 에만 의존.
10. **코루틴** (§4-4 "정확한 API") — Spring 7.0.9 가 suspend 리스너를 부를 때 자동 문맥 전파가 켜져 있으면 `PropagationContextElement` 를 붙인다 → `spring.reactor.context-propagation=auto` 한 줄 (코드 없음). 비동기 실패 경로의 재시도 발행은 소비 구간 밖이라 새 trace 일 수 있다 (시험 범위 밖). **→ 설정 한 줄로는 부족했다, 23.**
11. **1원인증 실행기 넘김** (새로) — 성명조회 응답 뒤 1원 이체가 앱 실행기 스레드에서 불려 trace 가 끊긴다 → 요청 스레드에서 ContextSnapshot 을 잡아 실행기를 감싼다.
12. **`bank.request` 는 Observation 이 아니라 Tracer** (§4-3 "구간(Observation)") — Observation 이면 P9a Timer 와 같은 이름의 지표가 다른 태그로 생긴다. 속성에 `error.type`(클래스 이름) 추가.
13. **정리 작업 건별 구간 = 도메인 포트 `ReconcileItemScope`** (§4-2) — 결과조회 · 재발행 한 건에 transfer.id 를 붙일 자리. 기본 no-op, apps/api 가 구현.
14. **소비 구간의 `transfer.reference` 는 뺀다** (§5 표) — 컨슈머는 이체 id 만 안다. 거래번호는 같은 trace 의 `bank.request` 에.
15. **T1 처리량 정의** (§1 기준 4 · §7) — 고정 도착률이라 초당 접수는 같다 → 처리량 = 출금 수 ÷ (부하 + 마무리 대기), CPU 는 부하 동안 평균. 포화 근처 40건/s 쌍 추가, 회차마다 재시작.
16. **Tempo `2.10.10`** (§6) — 2.x 마지막 줄 (3.x 는 구성이 크게 바뀌어 이번에 쓰지 않음). 2.7 부터 OTLP 수신 기본이 localhost → `0.0.0.0:4318` 명시.
17. **스모크 [11] 검색** (§9) — `{ span.transfer.id = … } && { resource.service.name = "bank-gateway" }` 로 멱등 재응답 trace 를 거르고, Tempo 가 뗀 traceID 앞 0 을 채워 로그를 grep, actuator 구간 0 도 본다.
18. **별도 JVM 게이트웨이 확인** (§8) — 테스트가 OTLP 수신기(JDK HttpServer + opentelemetry-proto)를 띄워 게이트웨이 JVM 의 구간을 받는다. 기존 `HttpGatewayFlowTest` 에 더한다 (새 컨텍스트 · 컨테이너를 늘리지 않음).
19. **exemplar 는 덤 Task 로 유지** (§6) — Boot 4.1.1 에 Prometheus exemplar 자동 구성이 있다. Prometheus `exemplar-storage` · Grafana 연결 · 패널 13, 안 보이면 되돌린다.
20. **로그 줄 모양** (위험 C8) — 실험 도구 · 스모크는 kubectl 시각 열과 부분 문자열만 읽는다 (점검함). Boot 기본 상관 패턴은 통합 테스트로 확인, 비어 있으면 `logging.pattern.correlation` 명시.
21. **`reconcile.*` 건수는 숫자(long) 속성** (§5 표) — "우리가 붙이는 속성 값은 문자열" 규칙의 의도된 예외. TraceQL 이 `span.reconcile.republished > 0` 처럼 비교하게 하려는 것이다.
22. **스펙 §8 통합 6(추적 끔에서 기존 흐름 그대로)은 대표 흐름 — 출금 · 1원인증 — 으로 갈음 (전체 스위트를 샘플링 0 으로 다시 돌리지 않음: 전파 코드는 샘플링과 무관하게 같은 길).**
23. **코루틴 컨슈머는 설정 한 줄로 부족했다 (10 을 고침)** — `spring.reactor.context-propagation=auto` 만으로는 trace 가 이어지지 않았다. Spring 7.0.9 의 `PropagationContextElement` 는 Reactor 문맥을 먼저 쓰는데 거기에 소비 구간이 없고, `bank.transfer` 는 `withContext(db)` 밖에서 돈다. 리스너 `on` 본문 전체를 Micrometer `ObservationRegistry.asContextElement()` 로 감싸 해결했다 (Task 7).
24. **JDK HttpClient 판은 CLIENT 구간이어야 한다** — 계획의 코드는 INTERNAL 구간을 냈다. 추가한 `JdkClientPropagationTest` 가 잡았고 `RequestReplySenderContext(setter, Kind.CLIENT)` 로 고쳤다.
25. **E10 을 글자 그대로 지킨다** — 돈 흐름 위의 모든 추적 호출(`Tracer.inSpan` · JDK 관측 · `bank.request` · `TraceTags` · 아웃박스 구간 열기)을 `runCatching` 으로 감싼다 (첫 실패만 WARN, 이후 DEBUG). 업무 블록은 정확히 한 번만 실행된다.
26. **traceparent 끝 플래그는 비교하지 않는다** — OpenTelemetry 1.62 는 W3C 2단계의 "random trace id" 비트를 켜서 `-03` (샘플링 0 이면 `-02`) 이다. `-01` 문자열 비교 코드는 깨진다.
27. **Grafana 익명 보기 사용자가 Explore 를 못 열었다** — `GF_USERS_VIEWERS_CAN_EDIT=true` (kind 만, 저장은 여전히 못 한다).
28. **스모크 [11] 의 검색 · 로그 확인** — Tempo 검색은 시작 · 끝 시각이 필요하고 요청 뒤 약 8초 지나야 보인다(제한된 재시도). `kubectl logs | grep -q` 는 pipefail 아래 SIGPIPE 로 거짓 실패하므로 `grep -c`.
29. **실험 도구 안전장치** — `run.sh` 는 인자를 검사한 **뒤에** 이전 출력을 지운다 (잘못된 인자가 추적되는 원자료를 지웠었다). `TRACING=off` 는 steady 에만. T1d (Tempo 0대) 는 Tempo 의 emptyDir 을 비운다.
30. **실제 422 · 400 요청은 구간에 예외를 싣지 않는다** — `ApiExceptionHandler` 가 처리한다. 지우개는 프로브 구간 테스트(지우개를 빼면 실패함)로 증명한다.
