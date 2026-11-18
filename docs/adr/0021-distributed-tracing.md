# ADR-0021: 분산 추적 — Micrometer Tracing · OpenTelemetry · Tempo (kind 에만)

- 상태: 제안 (2026-11-18) — 머지 뒤 정리 PR 에서 `채택`
- 관련: [ADR-0005](0005-kafka-delivery-and-consumer-idempotency.md) (아웃박스 · 적어도 한 번 전달) · [ADR-0012](0012-consumer-coroutines.md) (코루틴 비교 판) · [ADR-0015](0015-bank-gateway-service.md) (게이트웨이 분리) · [ADR-0020](0020-observability-metrics-alerts.md) (지표 · Grafana) · [스펙](../superpowers/specs/2026-11-18-p9b-tracing-design.md) (§14 계획 단계에서 고친 점 포함) · [측정 결과](../perf/p9b-tracing.md) · [OPERATION §18](../../OPERATION.md#18-분산-추적--tempo-p9b)

## 맥락
- P9a 로 "어느 단계가 막혔나" 는 지표로 보이지만, **한 건의 경로는 여전히 로그를 손으로 맞춰** 읽었다.
- 출금 한 건은 스레드 · 프로세스가 여러 번 바뀐다: HTTP 접수 → DB 저장 → 0.5초 뒤 발행기 → Kafka → 다른 api Pod 의 컨슈머 → HTTP → 게이트웨이 → TCP(은행). 자동 계측만으로는 아웃박스(DB 를 거치는 구간)와 정리 작업(몇 분 뒤 다시 보냄)에서 끊긴다.

## 결정
스펙 §2 의 E1 ~ E10 에 구현 계획과 실행 중에 정한 것(스펙 §14 · 계획 "계획에서 정한 것" 1 ~ 30)을 더했다.
1. **범위 · 백엔드** (E1 · E2): 출금 전 구간 + 1원인증 + 로그 traceId. 백엔드는 Grafana Tempo — 이미 있는 Grafana 에 데이터 소스로 붙인다. 로그 모으기(P9c) 코드 · 설정은 넣지 않는다.
2. **kind 에만 보낸다** (E3): Compose · IntelliJ 실행은 보내지 않는다. 계측은 Micrometer Tracing + OpenTelemetry 브리지(`spring-boot-starter-opentelemetry`), Java 에이전트 없음 (E4). Collector 없이 앱 → Tempo OTLP HTTP 직접 (E5).
3. **아웃박스 행에 `trace_parent`**(`VARCHAR(55)`, V7)를 저장하고 발행기가 행마다 부모로 되살린다 (E6). 되살림은 행마다 짧은 `outbox.publish` 구간을 열고 그 안에서 send — 접수 → 발행 틈이 trace 에 보이고, 행마다 열고 닫아 문맥이 다음 행으로 새지 않는다. 틀린 값은 새 trace.
4. **정리 작업 1회 = 새 trace** (`transfer.reconcile` root), 처음 trace 와는 `transfer.id` 속성으로 묶는다 (E7). 건별 `transfer.inquiry` · `transfer.republish` 는 도메인 포트 `ReconcileItemScope`(기본 no-op, apps/api 가 구현)로 연다.
5. **은행 전문에는 추적을 싣지 않는다** (E8): 게이트웨이 `withClient` 에서 `bank.request` 구간 (종류 · 결과 · 전문번호 · 사유 · 거래번호). Observation 이 아니라 **Tracer** — Observation 이면 P9a 의 같은 이름 Timer 와 겹친다. 모의 은행은 계측하지 않는다.
6. **금지 속성** (E9): 구간 속성 · 사건 · 상태 설명에 금액 · 계좌번호 · 예금주 · 1원인증 코드 · Idempotency-Key · 사용자 id 를 넣지 않는다. 예외를 구간에 남기면 메시지 · 스택이 같이 가므로 내보내기 앞 **지우개**(`ScrubbingSpanExporter`, 공용 모듈 `modules/tracing`)가 `exception.type` 만 남긴다. 지우개는 프로브 구간 테스트(지우개를 빼면 실패)로 증명한다. 실제 422 · 400 요청은 `ApiExceptionHandler` 가 처리해 구간에 예외가 실리지 않는다.
7. **추적 실패는 돈 흐름을 바꾸지 않는다** (E10): 보내기는 비동기 묶음(5초 · 대기열 2048, 넘치면 버림). 돈 흐름 위의 모든 추적 호출(`Tracer.inSpan` · JDK 관측 · `bank.request` · `TraceTags` · 아웃박스 구간 열기)을 `runCatching` 으로 감싸 첫 실패만 WARN, 이후 DEBUG — 업무 블록은 정확히 한 번 실행된다.
8. **보내기 켬 · 끔 = 주소의 유무** — `management.tracing.export.enabled=false` 는 Boot 4.1 에서 전파기까지 끄므로 쓰지 않는다. 앱 기본 = 주소 없음(구간 · 전파 · 로그 상관은 어디서나 켜짐), kind 의 app-config 만 `…otlp.endpoint` 를 준다. 비용 측정의 "끔" = 샘플링 0.
9. **자동 구성 정리**: 테스트용 자동 구성 모듈(`…tracing-test`)은 전파를 꺼서 넣지 않고 OTel `InMemorySpanExporter` 를 `SimpleSpanProcessor` 빈으로 단다. OTLP **지표** 내보내기 끔(P9a 는 Prometheus). `@Scheduled` 관측 끔(발행기 0.5초 · 정리 10초마다 root trace 가 생기므로 — 정리 작업 root 는 코드가 연다). `/actuator` SERVER 구간은 내보내지 않는다(프로브 · 수집이 trace 를 덮지 않게, 지표는 그대로).
10. **비교 판 · 실행기**: WebClient 는 `HttpBankGateway.create(kind, settings, observations)` 로 관측 레지스트리를 직접 받는다 (Boot Builder 아님). JDK HttpClient 판은 손코드로 **CLIENT** 구간 (`RequestReplySenderContext(setter, Kind.CLIENT)` — 처음 계획은 INTERNAL 을 냈고 `JdkClientPropagationTest` 가 잡았다). 1원인증은 성명조회 응답 뒤 1원 이체가 앱 실행기 스레드에서 불려 끊겨서, 요청 스레드의 `ContextSnapshot` 으로 실행기를 감쌌다 (`carryContext`).
11. **코루틴 컨슈머는 설정 한 줄로 부족했다**: `spring.reactor.context-propagation=auto` 만으로는 trace 가 이어지지 않았다 — Spring 7.0.9 의 `PropagationContextElement` 는 Reactor 문맥을 먼저 쓰는데 거기에 소비 구간이 없고, `bank.transfer` 는 `withContext(db)` 밖에서 돈다. 리스너 `on` 본문 전체를 Micrometer `ObservationRegistry.asContextElement()` 로 감싸 해결했다.
12. **Kafka 관측은 yml 두 줄** (`spring.kafka.template/listener.observation-enabled`). 재시도 · DLT 발행도 같은 템플릿이고 오류 처리기가 소비 구간 안에서 불려 **재시도 체인이 같은 trace** 다. 샘플링은 앱 기본 `1.0`(Boot 기본 0.1 을 덮음, 실험용), traceparent 끝 플래그(`-03` / 샘플링 0 이면 `-02`)는 비교하지 않는다.
13. **Tempo 2.10.10** — 단일 바이너리 · emptyDir · 보관 2일 · 제어 노드 고정 (3.x 는 구성이 크게 바뀐 판이라 이번에 쓰지 않음). 접속은 `kubectl port-forward` 뿐(Grafana 3000 · Tempo 3200). Grafana 익명 보기 사용자가 Explore 를 열도록 `GF_USERS_VIEWERS_CAN_EDIT=true`.

## 결과
측정 (2026-11-18, 한 PC · kind 3노드 — 운영 수치가 아니다, [측정 결과](../perf/p9b-tracing.md)). 숫자는 그 문서와 `docs/perf/raw/p9b/` 에서 그대로 옮겼다. 판정(돈 정확성)은 **11회 중 10회 통과, 1회(T3 첫 회차 `bank-1`)는 판정 규칙상 실패지만 돈은 정확**하다.
- **T1 추적 비용**: 20건/s 에서 접수 p95 켬 **25.25ms** / 끔 25.39ms (**−0.6%**, 회차 사이 흔들림 ±1.3ms 안 — 차이 없음), 완료 /s 같음, api CPU **+1.7%**, 게이트웨이 CPU +5.2%. 40건/s 한 쌍은 접수 p95 **+5.8%** (한 쌍뿐이라 관찰). **Tempo 0대(T1d)**: 돈 · 접수 p95 · 처리량이 켬 회차와 같고, 보내기 실패는 앱 로그 12줄로만 남았다 (요청은 기다리지 않음 — E10 확인). 그 회차에 Tempo 가 새 Pod(emptyDir)로 떠서 이전 trace 는 사라졌다.
- **T2 정상 출금의 모양**: 표본 20개 × 7회차 = **140 trace 모두 7구간 완전**, 두 서비스. 끝까지 p50 230 ~ 360ms 중 130 ~ 210ms 가 "접수 → 발행" 틈 (발행기 0.5초 주기, 아웃박스의 값), 컨슈머 → 은행 15ms 안팎, 모의 은행 호출 1ms 대. P9a 지표로는 나눠 볼 수 없던 것이다.
- **T3 은행 2분 끊기**: 재시도 trace **1000/1000** 이 처음 접수 trace 에 이어 붙었다 (두 회차, 위험 C2). 재발행 출금은 `transfer.id` 한 번 검색으로 접수 trace + 정리 작업 trace(2개 이상)와 `transfer.reconcile` root 가 모두 찾혔다 (검색 상한으로 표본 286 · 304건). 정리 작업 trace 는 최대 701구간으로 크다. `bank-1` 판정 실패는 은행을 끄는 순간 이미 보낸 3건이 "모름 → 결과조회 → 원거래 없음 → 환불" 로 끝나 "전부 COMPLETED" 규칙에 걸린 것 (돈은 정확).
- **T4 리더 교대**: `bank.request` 의 안 보냄 사유가 trace 에 남는다 — 모두 `stepping_down` (`gwkill-1` 3 trace · `gwroll-1` 14 trace), `not_leader` 0.
- **T5 (덤) exemplar**: 켬 회차마다 Prometheus 가 exemplar 26 ~ 119개를 저장했고 그 `trace_id` 를 Tempo 에서 찾았다.
- **실험 · 구현 중에 찾은 것**: 코루틴 설정 한 줄 부족(결정 11), JDK 판 INTERNAL(결정 10), OTel 1.62 의 traceparent 플래그 `-03`, Grafana 익명 사용자 Explore 불가, 스모크 [11] 의 Tempo 검색 지연(약 8초)과 `grep -q` SIGPIPE 거짓 실패(→ `grep -c`), 실험 도구가 잘못된 인자로 추적되는 원자료를 지우던 것(인자 검사를 삭제 앞으로).
- CI `k8s-smoke` 시간 · PR 번호는 PR CI 에서 기록.

## 위험 · 다음
- **코루틴 비동기 실패 경로**: 재시도 발행이 소비 구간 밖에서 일어나면 새 trace 일 수 있다 — 시험하지 않았다.
- **Tempo emptyDir**: Tempo Pod 가 바뀌면 trace 가 모두 사라진다 (T1d 에서 확인). 운영이면 객체 저장소.
- **샘플링 1.0 은 실험용**: 운영이라면 부모 기반 비율 샘플링 + 꼬리 샘플링(Collector 필요).
- **정리 작업 trace 가 크다** (최대 701구간, "실행 1회 = 새 trace" 의 대가) — 운영이면 건별 trace + 링크가 낫다 (추정).
- 운영이라면 (스펙 §11): OpenTelemetry Collector · 꼬리 샘플링, JDBC · SQL · Redis 구간, 모의 은행 계측, 서비스 맵 · 구간 지표(metrics-generator), 영구 · 원격 저장, Compose 추적.
- 다음: **P9c 로그 모으기** (Loki — 로그의 traceId 로 trace 이동). 지연을 줄이려면 발행 주기(0.5초)나 커밋 뒤 즉시 발행이 첫 후보 (추정, 이번 범위 밖).

## 검토한 대안
- **Java 에이전트 (자동 계측)**: 아웃박스 · 정리 작업은 어차피 손으로 이어야 하고, "같은 traceId" 를 테스트로 못 박기 어렵다 → 라이브러리(Micrometer Tracing + OTel 브리지) 로.
- **Jaeger**: Grafana 한 화면에서 지표 · 추적(· P9c 로그)을 보는 쪽이 낫다 → Tempo.
- **OpenTelemetry Collector**: 지금은 할 일이 없다(꼬리 샘플링 때) → 앱 → Tempo 직접.
- **정리 작업을 처음 trace 에 이어 붙이기**: 접수 뒤 수 분 · 수 시간 뒤의 일이라 trace 가 끝나지 않는다 → 새 trace + `transfer.id` 로 묶기.
- **은행 전문에 추적 싣기**: 규격에 자리가 없다 → 게이트웨이의 `bank.request` 구간까지만.
