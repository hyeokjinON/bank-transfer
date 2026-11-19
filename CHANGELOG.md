# CHANGELOG

형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/) 를 따르고, 릴리스 단위는 로드맵 단계(P0~P9)다.
항목은 "무엇이 바뀌었나"를 쓰고, 이유는 PR · ADR 로 연결한다.

## [Unreleased]
### Added
- 프로젝트 안내 문서: `CLAUDE.md`, `ARCHITECTURE.md`, `SETUP.md`, `OPERATION.md`, `HANDOVER.md`, `CHANGELOG.md`.
- P3 설계 문서 (`docs/superpowers/specs/2026-10-19-p3-transfer-design.md`).
- DB 문서 `docs/database.md`: ERD, 테이블 정의, 인덱스 용도, 잠금 방식, 시간대 정책(KST 저장), 마이그레이션 규칙, 운영 쿼리.

## [P9c] 로그 모으기 — Grafana Alloy · Loki — 2026-11-19 ([#39](https://github.com/hyeokjinON/bank-transfer/pull/39), 머지 후 정리 PR #40)
### Added
- kind: Loki 3.7.9 (`monitoring/loki.yaml` · `loki/loki.yml` · `loki/rules.yml`, 제어 노드 · emptyDir · 보관 2일, 라벨 `namespace` · `app` · `container` 셋 · 구조화 메타데이터 `pod` · `node` · `level` · `trace_id`) · Grafana Alloy v1.21.3 DaemonSet 3개 (`monitoring/alloy.yaml` · `alloy/{config,pipeline,check}.alloy` · `base/rbac-alloy.yaml` — `bank-transfer` namespace Pod 읽기 Role) · `versions.env` `LOKI_VERSION` · `ALLOY_VERSION`.
- 처리 모듈 `pipeline.alloy` (꺼내기 level · trace_id → 가리기): 수집 설정과 CI 표본 설정이 같이 import. 가리기 = 계좌(10 ~ 16자리) · 1원인증 코드 `BT****` · 금액 `***원`, 같은 규칙 두 번 · 영숫자 경계 · JSON 이스케이프 경계. 금지 패턴(가리지 않은 모양) 0건을 Loki 질의로 증명.
- 로그 알림 `ErrorLogSurge` 하나 (Loki ruler → Alertmanager, `source=logs`, 5분 ERROR > 10 · `for: 2m`, Spring Kafka DLT 발행 줄 제외) · Alertmanager 억제 (`BankDisconnected` · `GatewayNoLeader` 동안).
- Grafana: Loki 데이터 소스 (`trace_id` 파생 필드 → Tempo) · Tempo → 로그(`tracesToLogsV2`) · 대시보드 ① 4행 "로그" (서비스 · 레벨별 양, api · 게이트웨이 ERROR · WARN).
- 게이트웨이 은행 호출 한 줄 `bank call type= outcome= reason= tranNo= reference= ms=` (`bank.request` 문맥의 traceId) · kind 구조화 로그(ECS JSON) 계약 테스트 (별도 JVM).
- 검사 · 스모크: `check.sh` [5/6] Loki 설정 · LogQL · [6/6] Alloy 가리기 표본 실제 실행, 스모크 [12] 로그 (traceId 로 api · 게이트웨이 줄, 모의 은행 줄 가림, 금지 패턴 0, ErrorLogSurge 규칙 · Grafana Loki).
- 실험 도구 `PHASE=p9c` (`load/k8s/p9c.sh` — 수집 지연 표지 · `JSONLOG=on|off` · `podkill` · `lokizero` · `alloykill` · `errfault`(1원인증 저장 실패 트리거) · 억제 시험 · 가리기 확인) · [측정 결과](docs/perf/p9c-logs.md) · 원자료 `docs/perf/raw/p9c/`. Postman `10. 로그 (P9c, kind)` 폴더 (10-1 ~ 10-6).
- [ADR-0022](docs/adr/0022-log-collection.md) (정리 PR #40 에서 채택), OPERATION §19, ARCHITECTURE 로그 절, 구성도 두 장 갱신 (`runtime-k8s.svg` 에 Loki · Alloy · 갈색 점선, `app-architecture.svg` 에 ● L), `infra/k8s/README.md`.
### Changed
- kind 의 api · 게이트웨이 로그를 ECS JSON 으로 (`app-config` `LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs`, 게이트웨이 env 는 optional). Compose · IntelliJ 는 텍스트 그대로.
- 로그에서 사용자 id · 멱등키 · 금액 빼기: 지갑 로그 · `Transfer.toString` · `StoreConflict` · 지갑 낙관적 차감 충돌 예외 메시지, 앱 알림의 로그 사본(`ALERT` 로거)도 금액 · 잔고를 가린다 (`AlertLogMask` — 텔레그램 본문은 그대로), Redisson 시간 초과 예외는 클래스 이름만, Hibernate 제약 위반 로그(`org.hibernate.orm.jdbc.error`)는 ERROR 로 (api `application.yml`, 모든 환경).
- `check-dashboards` 를 Python 으로 나눠 PromQL 은 promtool, LogQL 은 `check.sh` [5/6] 의 Loki 가 검사.
### 측정 요약 (이 PC · kind 3노드 기준 — 운영 수치가 아니다, 돈은 9회 전부 통과)
- 수집 지연 (L1): 표지 줄이 Loki 에서 보이기까지 p50 1.0 ~ 2.3초, p95 **2.4 ~ 2.6초**, 빠진 표본 0 (해상도 약 1초).
- JSON 비용 (L2): 접수 p95 켬 평균 27.19ms / 끔 26.86ms → **+1.2%** (기준 ≤ +10%), 완료 /s 19.68 / 19.69 → **−0.05%** (기준 ≥ −5%). 바이트는 약 두 배 (api 11.1 대 5.7 KB/s, 게이트웨이 9.6 대 5.1 KB/s). Loki CPU 0.015 ~ 0.027 코어 · RSS 192 ~ 242MB, Alloy CPU 0.026 ~ 0.066 코어 · RSS 231 ~ 239MB.
- 잃는 줄 (L3 · L5): api Pod 강제 종료 **940/940**, Alloy 재시작 **506/506**, 중복 0. Loki 0대 60초 (L4): 회차 중 기록은 813줄 중 270줄 도착(543 미도착)이었지만 약 19분 뒤 재계수 **830 ≥ 813** — 최종 잃음 0, 도착 지연만 (Alloy 가 나중에 다시 넣음 — 지수 백오프 재시도인지는 추정, 코드로 확인하지 않음. `loki.write` 가 막히면 읽기가 멈추는 역압일 수도 있다. 도구의 기다림이 더 짧았음 — 543 은 도구 한계). 꺼지기 전 데이터는 emptyDir 이라 사라짐.
- ErrorLogSurge (L6): 정상 상태 5분 ERROR 최대 0 → N = 10 그대로. 1원인증 저장 실패 주입 → 500 응답 58건 = api ERROR 58줄, `ApiHigh5xx` 안 울림. **도착은 계산 구간 [186, 256]초보다 18초 늦은 273.9초 — 성공 기준 6 은 "부분"** (원인 후보: 묶음 `group_interval` 대기 · ruler 의 다음 평가, 둘 다 추정). 은행 2분 끊김: 억제 확인 (`inhibit_ok` true), 새 ErrorLogSurge 0, DLT 를 포함하면 5분 ERROR 가 773줄 — 식에서 뺀 결정이 실측으로 맞음.
- 가리기 (L7): 모든 회차 금지 패턴 **0** (실험은 계좌 10 ~ 14자리 식 · 최종 리뷰 수정 전 이미지 `dev-6cafa40` — 16자리로 넓힌 식은 CI 표본 · 스모크 [12] 로 확인), 가린 줄은 실제로 있음 (계좌 703 ~ 11468줄, 1원인증 코드 58줄 = 주입 58건).
- 발견: 측정 도구의 L4 기다림이 Alloy 가 다시 넣기까지보다 짧아 유실처럼 보인 것(→ 재계수), 로그 알림은 넘은 뒤 약 3 ~ 4.5분(지표 알림 50초보다 느림), Loki 3.7 자동 라벨 · `detected_level` 끄기, 계좌 가리기 경계에 JSON 이스케이프 추가.
### CI · 사람 확인
- CI (#39): 첫 실행 `9adea98` — `build` 10m27s · `infra-smoke` 48s · `app-smoke` 3m57s · `k8s-smoke` 11m26s **실패** (제어 노드 Alloy 가 `inotify: too many open files` 로 CrashLoopBackOff — GitHub 러너의 `max_user_instances=128` 을 kind 노드 3개가 같이 씀 → Alloy `import.file` 감시를 폴링으로 + CI 에 inotify 한도 올리는 단계, `c57b0b1`). `c57b0b1` — `k8s-smoke` 7m51s **실패** (CI `mysql:8.4` 의 소켓이 `/var/lib/mysql/mysql.sock` 이라 스모크 [12] 의 `sql()` 이 못 붙음 → kind 의 `sql()` · `sql_root()` 를 TCP `127.0.0.1` 로, `18b133c`). 머지 직전 `18b133c` — `build` 10m29s · `infra-smoke` 51s · `app-smoke` 3m51s · **`k8s-smoke` 5m32s** (20분 제한, 성공 기준 7 통과).
- Postman 직접 확인 (2026-11-20): 새 폴더 `10. 로그 (P9c, kind)` Run folder 10-1 ~ 10-6 **16/16** 통과 — 출금 202 → 출금 id 로 api 줄 · trace_id → 같은 trace_id 로 api · 게이트웨이 줄 (`bank call … outcome=ok`) → 거래번호로 모의 은행 줄 가려짐 → 금지 패턴 0 → `ErrorLogSurge` 규칙. Grafana Explore 로그 → Tempo → "Logs for this span". (10-3 만 따로 보내면 앞 Run 의 trace_id 가 15분 밖이라 빈 결과 — 폴더를 처음부터.)
- 정리 PR #40: ADR-0022 채택, README P9c · P9 `[x]` (하위 단계가 모두 끝났는데 비어 있던 P8 상위 칸도 `[x]`), ARCHITECTURE 기준 줄 · "(제안)" 떼기, 측정 결과에 CI 시간 · Postman 확인 · 알림 로그 사본의 10자리 이상 금액 한계, OPERATION §19-4 inotify · §19-6 그 한계, HANDOVER P9 완료. 구성도 두 장은 #39 에서 이미 갱신.

## [P9b] 분산 추적 — Micrometer Tracing · OpenTelemetry · Tempo — 2026-11-18 ([#37](https://github.com/hyeokjinON/bank-transfer/pull/37), 머지 후 정리 PR #38)
### Added
- 추적 스타터(`spring-boot-starter-opentelemetry`) · 공용 모듈 `modules/tracing`(내보내기 앞 지우개 `ScrubbingSpanExporter` — 예외 메시지 · 스택 · 상태 설명 제거, `/actuator` 구간 거름). 보내기는 주소가 있을 때만 (kind 의 app-config).
- V7 `outbox_event.trace_parent`(`VARCHAR(55)`) · 발행기 행마다 `outbox.publish` 구간 (저장한 traceparent 를 부모로, 틀린 값은 새 trace) · 정리 작업 1회 = 새 trace `transfer.reconcile` (건별 `transfer.inquiry` · `transfer.republish`, 도메인 포트 `ReconcileItemScope`) · 게이트웨이 `bank.request` 구간 (Tracer, `bank.outcome` · `bank.not_sent.reason` · `bank.tran_no` …).
- 비교 판: JDK HttpClient 전파(CLIENT 구간, `JdkClientPropagationTest`) · 코루틴 컨슈머(`ObservationRegistry.asContextElement()`) · 1원인증 실행기 넘김(`ContextSnapshot`).
- kind: Tempo 2.10.10 (`monitoring/tempo.yaml` · `tempo/tempo.yml`, 제어 노드 · emptyDir · 보관 2일) · Grafana Tempo 데이터 소스 · `versions.env` `TEMPO_VERSION` · 스모크 [11] (Tempo 검색으로 api · 게이트웨이 구간 확인).
- 실험 도구 `PHASE=p9b` (`load/k8s/p9b.sh` · `traceshape.py`, `tempozero` 동작, `TRACING=on|off`) · [측정 결과](docs/perf/p9b-tracing.md) · 원자료 `docs/perf/raw/p9b/`.
- (덤) Prometheus exemplar → Grafana "접수 → 확정 시간" 패널 점에서 Tempo 로.
- [ADR-0021](docs/adr/0021-distributed-tracing.md) (정리 PR 에서 채택), OPERATION §18, ARCHITECTURE 추적 절, 구성도 두 장 갱신 (`runtime-k8s.svg` 에 Tempo · 보내기 선, `app-architecture.svg` 에 ● T), `docs/database.md` V7.
### Changed
- Kafka 관측(template · listener) 켬 · 샘플링 1.0 · `spring.reactor.context-propagation=auto` · `@Scheduled` 관측 끔 · OTLP 지표 끔 · Grafana 익명 보기 사용자도 Explore (`GF_USERS_VIEWERS_CAN_EDIT=true`, kind 만).
- `HttpBankGateway.create(kind, settings, observations)` (관측 레지스트리 인자) · `BankConnectionOwner` 인자 · `TransferReconciler` 건별 범위.
- 돈 흐름 위의 모든 추적 호출은 `runCatching` 으로 감쌌다 (E10 — 추적 실패는 업무를 바꾸지 않는다).
- "V7 인덱스" 문구 → "다음 마이그레이션 인덱스" (새 V7 = `trace_parent` 와 혼동 방지: `docs/perf/p9a-metrics.md` B1 · OPERATION §17-6 · `load/k8s/p9a.sh` · api `application.yml` 주석).
### 측정 요약 (이 PC · kind 3노드 기준 — 운영 수치가 아니다, 돈은 11회 중 10회 통과 · 1회는 판정 규칙상 실패지만 정확)
- 추적 비용 (T1, 20건/s): 접수 p95 켬 **25.25ms** / 끔 25.39ms (**−0.6%**, 잡음 안), 완료 /s 같음, api CPU **+1.7%**. 40건/s 한 쌍은 **+5.8%** (한 쌍뿐 — 관찰). Tempo 0대: 돈 · 접수 p95 · 처리량 그대로, 보내기 실패 로그 12줄.
- trace 모양 (T2): steady · gwkill · gwroll 표본 100개는 모두 정확히 7구간 · 출금 1건 · 두 서비스 (bank 표본 40개 중 6개는 여러 출금이 든 정리 작업 root trace — 설계, 누수 아님). 끝까지 p50 230 ~ 360ms 중 130 ~ 210ms 가 접수 → 발행 틈 (발행기 0.5초 주기).
- 은행 2분 끊김 (T3): 재시도 trace 1000/1000 이 접수 trace 에 이어 붙음, 재발행은 `transfer.id` 한 번 검색으로 접수 + 정리 작업 trace. `bank-1` 은 은행을 끄는 순간 보낸 3건이 환불로 끝나 판정 규칙상 실패 (돈은 정확).
- 리더 교대 (T4): 안 보냄 사유는 모두 `stepping_down`. exemplar (T5): 회차마다 26 ~ 119개, Tempo 에서 trace 확인.
- 발견: 코루틴은 설정 한 줄로 부족(→ `asContextElement()`), JDK 판 INTERNAL → CLIENT, traceparent 플래그 `-03`, 스모크 `grep -q` SIGPIPE 거짓 실패(→ `grep -c`), 실험 도구가 잘못된 인자로 원자료를 지우던 것(인자 검사를 삭제 앞으로).
### CI · 사람 확인
- CI (#37): 첫 실행 `48b130f` — `build` 9m56s · `infra-smoke` 48s · `app-smoke` 3m50s · `k8s-smoke` 6m37s **실패** (스모크 [10] 출금 카운터 `increase()` 0 — 새 Pod 의 첫 수집이 스모크 출금보다 늦으면 첫 증가를 못 셈 → 원래 합계로 한 번 더 보게 고침 `b1247a5`). `b1247a5` — `k8s-smoke` 4m13s 통과, `build` 는 가상 스레드 실험 테스트의 pinning 2ms(Kafka 컨슈머 내부 잠금)로 실패. 머지 직전 `3a9ed42` — `infra-smoke` 55s · `app-smoke` 2m09s · **`k8s-smoke` 4m49s** 통과, `build` 는 게이트웨이 리더 반납 테스트가 CI 의 API 서버에서 2초를 넘겨 실패 → 실패 잡만 다시 돌려 9m53s 통과. `build` 두 실패는 P9b 코드와 무관한 시간 흔들림 (로컬 5/5 · 3/3 통과) — 기준을 느슨하게 하지 않았다.
- PR 직전 수정 (최종 리뷰 m4): 빈 정리 작업 root 거르기가 OTel 의 root parentId `0000000000000000` 을 root 로 못 봐 아무것도 거르지 않았다 → 모두 0 도 root 로 보고, 건별 구간 수 `reconcile.items` 를 함께 봐서 은행 끊김 때 root 를 지워 자식이 고아가 되는 것을 막음 (`553a2d7`). 실제 브리지 구간 테스트 · kind `dev-553a2d7` 새 Pod 의 빈 실행 Tempo 0건.
- Postman 직접 확인 (2026-11-19): 새 폴더 `9. 추적 (P9b, kind)` 9-1 ~ 9-4 통과 — 출금 202 → Tempo 검색 trace 1개 → 구간 7개 이름 · 두 서비스 · 계좌번호 없음, Grafana Explore 로 같은 trace, api 로그 줄의 traceId 로 같은 출금 찾음, 은행 끊기 중 재시도 구간이 처음 trace 에 붙음, `{ name = "transfer.reconcile" && span.reconcile.items = 0 }` 결과 없음. (Tempo 수집이 8 ~ 20초 늦어 9-2 는 5초 간격 6번까지 다시 묻는다.)
- 정리 PR #38: ADR-0021 채택, README P9b `[x]`, ARCHITECTURE 기준 줄 · "(제안)" 떼기, 측정 결과에 CI 시간 · Postman 확인, HANDOVER 다음 = P9c. 구성도 두 장은 #37 에서 이미 갱신.

## [P9a] 관측성 — 지표 · 대시보드 · 알림 — 2026-11-17 ([#35](https://github.com/hyeokjinON/bank-transfer/pull/35), 머지 후 정리 PR #36)
### Added
- 업무 지표 (`banktransfer.*`): api — 접수 · 은행 결과(`ok|rejected|not_sent|unknown|maintenance`) · 완료 시간(접수 → 확정) · 포기 · DLT(`kind`) · 재발행 · 결과조회 · 아웃박스 발행, 게이트웨이 — 은행 호출 시간 · 안 보냄 사유 · 리더 · 은행 연결 · 번호 구간. 두 앱 `/actuator/prometheus` 노출. 지표 이름 표는 OPERATION §17-4.
- 상태 Gauge (api, 10초마다 DB 조회): 미완료 건 수 · 가장 오래된 나이(SQL `NOW(6)`) · 결과조회 소진 UNKNOWN · 미발행 아웃박스. 조회 실패는 NaN.
- `monitoring` namespace (제어 노드): Prometheus(Pod 직접 5초 수집, Role) · Alertmanager · alert-sink(busybox `httpd` CGI) · Grafana, 규칙 11개 · 대시보드 2장(① 출금 흐름 · ② 게이트웨이 · 은행 · JVM). `infra/k8s/monitoring/` · `base/rbac-prometheus.yaml` · Secret `grafana-admin`. 규칙 · 설정 검사 `bash infra/k8s/monitoring/check.sh` (CI `build` 잡도).
- 스모크 [10] (관측 4개 Ready · 수집 대상 `up` · 리더 1 · 은행 연결 1 · 지표 증가 · 입구 `/actuator/prometheus` 404 · 규칙 11 · 시험 알림 도착). NetworkPolicy `metrics-from-prometheus-api` · `-gateway`.
- 실험 도구: `PHASE=p9a`(감시 원자료), 동작 `gwzero` · `maint`, 요약 JSON `bank_gap_diff_s`(5초 격자) · `bank_gap_diff_raw_s`(원 표본), 카운터는 `increase()`. [측정 결과](docs/perf/p9a-metrics.md) · 원자료 `docs/perf/raw/p9a/`.
- [ADR-0020](docs/adr/0020-observability-metrics-alerts.md) (정리 PR 에서 채택), OPERATION §17, ARCHITECTURE 관측 절, 구성도 두 장 갱신 (`runtime-k8s.svg` 에 monitoring, `app-architecture.svg` 에 지표 지점 ● M).
### Changed
- `TransferPipelineStalled` RECEIVED 임계값 30분 → **15분** (최종 리뷰 I1): 지표 나이는 마지막 상태 변화 뒤(`updated_at`)라 정상 최대가 514초 (M4). 대시보드 임계선 · 경계 테스트 · `TransferStateReaderTest`(updated_at 기준 고정) · 스펙 §16 · ADR-0020 · OPERATION 같이.
- Envoy HTTPRoute 를 허용 목록으로: `/api/v1` · `/admin` · `/actuator/health` · `/actuator/info` 만 넘기고 나머지는 404 (이전에는 모든 경로를 넘겼다).
- `BankNotSentException` 에 `reason: NotSentReason`(기본 `OTHER`, 점검 보류 `MAINTENANCE` 포함), 도메인 포트 `TransferObserver`, `finish(transferId, …)` → `finish(transfer, …)`, `DltKind` public, 스케줄 풀 4 → 5.
- 앱 Pod 템플릿에 수집 어노테이션 (롤링 1회).
### 측정 요약 (이 PC · kind 3노드 기준 — 운영 수치가 아니다, 돈은 8회 전부 정확)
- 은행 공백 지표 vs 모의 은행 로그: 차이 원 표본 **2.4 ~ 4.5초** (기준 5초 이내), 5초 격자로 읽으면 5.0 ~ 8.2초 — 대시보드는 복구를 최대 약 8초 늦게 보인다. 5초보다 짧은 공백(1.9 · 2.0초)은 지표에 안 보인다.
- 알림 도착: BankDisconnected **49 · 50초**, GatewayNoLeader **47초** (계산값 50초). 억제 동작 (M3 의 BankDisconnected 는 Alertmanager 에서 `suppressed`). 점검 창 M5 거짓 알림 **0건**.
- M4 (은행 10분 끊김, 약 1만 2천 건 밀림): 상태 조회 최대 **43.8ms** (≤ 50 — V7 인덱스 불필요), 시계열 수 최대 55,395, 카운터 접수 = DB (12801). Pod 재시작이 있으면 단순 차이는 −420 → `increase()` 로 재며 약 1% 어긋난다 — 카운터는 추세용, 돈의 정답은 DB.
- 발견: alert-sink 를 `nc` 로 만들었더니 본문을 읽기 전에 200 을 답하고 끝나 알림 절반이 조용히 사라졌다 (설계의 위험 B7 보다 나빴다) → busybox `httpd` + CGI 로 고치고 M2 를 다시 쟀다.
### CI · 사람 확인
- CI (#35): 첫 실행 `c936b08` — `build` 9m13s · `infra-smoke` 49s · `app-smoke` 3m56s · **`k8s-smoke` 8m07s**, 머지 직전 `ba2f5b0` — `build` 9m19s · `infra-smoke` 47s · `app-smoke` 4m30s · **`k8s-smoke` 5m59s** (20분 제한 그대로, 성공 기준 5 통과).
- Postman 직접 확인 (2026-11-18): 새 폴더 `8. 관측 (P9a, kind)` 8-1 ~ 8-9 통과 — 입구 `/actuator/prometheus` 404, 출금 `COMPLETED`, 리더 1 · 은행 연결 1, 은행을 끊으면 BankDisconnected pending → firing, 되살리면 해제. (첫 시도에서 새 컬렉션 변수가 열린 탭에서 빈 값으로 풀려 `ENOTFOUND api` → 폴더 8 은 주소를 요청에 직접 적음.)
- 정리 PR #36: ADR-0020 채택, README P9a `[x]`, ARCHITECTURE 기준 줄 · "(제안)" 떼기, OPERATION §17-6 M4 명령을 실제 실험 값으로, 측정 결과에 CI 시간 · Postman 확인, HANDOVER 다음 = P9b.

## [P8-3c] 게이트웨이 필수 분산 — 2026-11-17 ([#33](https://github.com/hyeokjinON/bank-transfer/pull/33), 머지 후 정리 PR)
### Changed
- 게이트웨이 `topologySpreadConstraints`: `ScheduleAnyway` → **`DoNotSchedule`** + `nodeTaintsPolicy: Ignore` · `nodeAffinityPolicy: Honor` 명시 (`matchLabelKeys [pod-template-hash]` 유지). 두 게이트웨이는 한 워커에 놓일 수 없다 — 워커가 빠지면 대체 Pod 는 Pending, 돌아오면 그 워커에 뜬다 ([ADR-0019](docs/adr/0019-gateway-required-spread.md), 채택).
- 스모크 [9]: "두 게이트웨이가 서로 다른 워커" 확인 추가.
- 실험 도구: `PHASE=p8-3c`, 요약 JSON `gw_pending_s` · `gw_colocated_s` · `gw_nodes_end`, drain · 노드 동작 뒤 게이트웨이 Ready 2/2 까지 표본을 남김.
- OPERATION §16-3 (필수 분산 · Pending 이 정상), §16-4 표 한 줄.
### Added
- 구성도 두 장 `docs/diagrams/app-architecture.svg` (애플리케이션 구조 · 출금 흐름) · `runtime-k8s.svg` (K8s 실행 구성) — README · ARCHITECTURE 에 걸고 단계마다 갱신. ARCHITECTURE 의 Kubernetes 그림을 P8-3b 구성(리더 · 대기)으로 고침.
- [측정 결과](docs/perf/p8-3c-gateway-spread.md) (원자료 `docs/perf/raw/p8-3c/`).
### 측정 요약 (이 PC · kind 3노드 기준 — 운영 수치가 아니다, 돈은 전 회차 정확)
- S1 롤링: 멈추지 않음, 끝난 뒤 두 워커 · 몰림 0. S2 리더 워커 drain: 대체 Pod **Pending 42초 → uncordon 뒤 원래 워커**. S3 대기자 워커 SIGKILL: **Pending 70.5초 → 복구 뒤 원래 워커**, 리더 교대 없음. S4 이어서 리더 워커 SIGKILL: 새 리더 **14.4초** (P8-3b 의 같은 순서 353.4초), 대체 Pod 71.8초 Pending → 원래 워커. 전 회차 몰림 0초 · 세션 거부 · 번호 중복 · 결과 모름 0.
### CI · 사람 확인
- CI (#33): `build` 8m50s · `infra-smoke` 47s · `app-smoke` 1m44s · `k8s-smoke` 3m53s 통과 (스모크 [9] "두 게이트웨이가 서로 다른 워커" 포함).
- Postman 직접 확인 (머지 전, kind · 이미지 `dev-aba0c2f`, 2026-11-17, 사용자) — 6항목 통과: 모드 확인(두 워커 · 리더 1) · 1원인증 `VERIFIED` · 출금 202 → `COMPLETED` · 같은 키 200 같은 id · 리더 워커(worker2) drain → 리더 교대 3초 · 은행 공백 3.4초 · 대체 Pod **Pending** (워커 `Ready,SchedulingDisabled`) · 그동안 출금 `COMPLETED` · 사유 `1 node(s) didn't match pod topology spread constraints, 1 node(s) had untolerated taint(s), 1 node(s) were unschedulable` · uncordon 즉시 **같은 worker2** 에 배치(Pending 10분 21초 = 사람 확인 시간), 5초 뒤 Ready. `session rejected` 0 · `duplicate tran no` 0.
### 정리 PR
- ADR-0019 채택, README P8-3c `[x]`, ARCHITECTURE 기준 줄 · "(제안)" 뗌, HANDOVER 완료.
- OPERATION §16-3 · §16-4: Pending 사유 확인을 `describe | tail` 에서 `get events --field-selector involvedObject.name=<Pod>,reason=FailedScheduling` 로 (사람 확인에서 `tail` 로 사유가 안 보였다), drain 과 노드 장애의 사유 문구 차이.
- 테스트 흔들림 수정 (이 PR 의 CI `build` 에서 발견): `GatewayFailoverTest` 가 구간 10개를 다 쓴 순간 다음 구간 미리 예약(백그라운드 API 서버 왕복)이 아직이면 게이트웨이는 설계대로 "안 보냄" 으로 답하는데, 테스트가 이를 실패로 봤다. 운영의 api 재시도처럼 그 경우만 다시 보내게 고침 (안 보냄 = 은행에 안 감이라 입금 건수 판정은 그대로). 코드 변경 없음.

## [P8-3b] 게이트웨이 이중화 — 2026-11-16 ([#31](https://github.com/hyeokjinON/bank-transfer/pull/31), 머지 후 정리 #32)
### Added
- 게이트웨이 2대 active-standby: K8s Lease `bank-gateway-leader` 리더 선출(fabric8 `kubernetes-client` 8.0.0, JDK HttpClient · 재시도 없음 · 연결 2s · 요청 5s) + 로컬 워치독(갱신 기한 + 0.25초 안에 자기 차단 · 선출기 반납 후 재후보) + 리더 라벨 `bank-transfer/gateway-role=leader`(Service selector, 대기자도 Ready — readiness `role: standby`) + 임기마다 새 `BankGatewayClient` (`BankConnectionOwner`) + SIGTERM 종료 훅 교대(새 요청 거절 → 처리 중 전문 비우기 → 연결 끊기 → 라벨 떼기 → Lease 반납). 선출은 `bank.gateway.leader-election.enabled` 기본 꺼짐.
- 전문번호 ConfigMap 구간 예약 `bank-gateway-tran-no` (1,000개씩, `resourceVersion` 조건부 update, 경쟁 테스트) — `TranNoSource` 인터페이스(기본 메모리 카운터). 저장된 날짜가 더 새로우면 예약하지 않고, 깨진 기록은 덮어쓰지 않으며, 둘 다 송신 안 함(fail-closed).
- 규격 [v0.4](docs/protocol.md): 전문번호 기관 · 일자(KST)별 유일, 응답코드 `3001`(처리 안 함) → 게이트웨이 `503 BANK_NOT_SENT`.
- 모의 은행 옵션(기본 꺼짐, K8s 에서만): `--single-session` · `--session-idle-timeout=60s` · `--unique-tran-no`, 세션 수 로그. readiness 프로브는 `/proc/net/tcp` LISTEN exec (tcpSocket 은 세션을 먹는다).
- 매니페스트: `base/rbac-gateway.yaml`(ServiceAccount · Role · RoleBinding), 게이트웨이 PDB, `overlays/experiment-gateway-single`(H0 이전 구성 대조). 스모크 [9](Ready 2/2 · 리더 라벨 1개 = Lease 보유자 · 엔드포인트 = 그 Pod · 모의 은행 동시 세션 1).
- 실험 도구 `load/k8s/run.sh`: `PHASE=p8-3b`, 동작 `gwroll` · `gwkill` · `gwnode` · `gwcut`, `NODE_KILL=1`, 게이트웨이 분산 사전 확인, 요약 JSON 확장(`bank_sessions_*` · `bank_dup_tran_no` · `unknown_results` · `gw_acquired_s` · 은행 공백).
- ADR-0018 (채택), OPERATION §16, ARCHITECTURE "게이트웨이 이중화", [측정 결과](docs/perf/p8-3b-gateway-ha.md) (원자료 `docs/perf/raw/p8-3b/`). 테스트 41개 추가 — 전체 664개 (P8-3a 623 + 41, 건너뜀 2).
### Changed
- 게이트웨이 `Recreate` · preStop 10s → `RollingUpdate`(0/1) · preStop 없음 · replicas 2 · `matchLabelKeys` 분산 · PDB `minAvailable 1`. **이전 구성 → 이중화 첫 적용은 `scale --replicas=0` 뒤 적용** (롤링 교착, OPERATION §16-5).
- Compose · IntelliJ (선출 꺼짐) 종료 순서가 바뀜: SIGTERM 에 웹 graceful 전에 새 요청 503 BANK_NOT_SENT → 처리 중 전문 최대 5초 비우기 → 끊기 (예산 5 + 20 < 30초 `stop_grace_period`).
- 문서: README 로드맵(P8-3b `[x]`) · 실행 안내, HANDOVER, `infra/k8s/README.md`, OPERATION §15 머리말.
### 측정 요약 ([결과](docs/perf/p8-3b-gateway-ha.md), 이 PC · kind 3노드 기준 — 운영 수치가 아니다)
- **13회 전부 돈 정확** (접수 = 출금 행 = 거래번호, 전부 `COMPLETED`, 은행 기록 빠짐 0 · 이중 0, 환불 0, 잔고 = 원장 합). **동시 세션 최대 1.**
- **H0 ↔ H1**: 이전 구성 drain 세션 거부 **7** · 번호 중복 **591** → 이중화 **0 · 0** (2회). preStop 없는 H0 변형은 중복 387 — 번호 중복은 재시작만으로 생긴다.
- **정상 교대 · 롤링 · 강제 삭제** 6회 은행 공백 **1.1 ~ 3.7초**, 결과 모름 0. **노드 SIGKILL(H4′)** 새 리더 **14.6초** · 공백 14.3초(Lease 만료). `docker stop` 은 SIGTERM 이라 정상 교대와 같다(4.3초).
- 발견: 첫 전환 롤링 교착 · 두 복제본이 한 노드면 새 리더 **353.4초**(`ScheduleAnyway` 는 재분산 안 함) · 모의 은행 tcp 프로브가 세션을 먹음(거부 13). H5(API 서버 단절)는 kind(kindnet)가 API 서버 egress 를 막지 않아 재현 못 함 → `GatewayLeaderElectionTest` 로 대체.
- 구현 리뷰가 찾은 안전 결함(측정 전 수정): 응답 없는 API 서버에서 자기 차단이 15 ~ 20초 늦음(워치독), 워치독 뒤 리더 없음 고착, 전문번호 날짜 되돌림.
### CI · 사람 확인 (머지 전)
- CI (#31, `dcfb203`): `build` 9분 12초 · `infra-smoke` 48초 · `app-smoke` 4분 10초 · **`k8s-smoke` 5분 52초** (P8-3a 5분 45초 — 스모크 [9] 를 더하고도 스모크 단계 21초 그대로, 20분 제한 그대로).
- Postman 직접 확인 (2026-11-16, kind · 이미지 `dev-dcfb203`): 1원인증 `VERIFIED` · 충전 → 출금 `COMPLETED` · 같은 키 재요청 200 같은 id · **리더 Pod 삭제 2번** 직후 출금 모두 `COMPLETED` (리더 · Lease 보유자가 다른 Pod 로 옮겨 감) · 대기자 `bankConnection` = `role: standby` UP · 모의 은행 세션 `closed sessions=0 → opened sessions=1` (은행 공백 4.0 · 3.0초), `session rejected` 0 · `duplicate tran no` 0 · 번호 장부 `reservedTo` 28000 → 30000 (교대마다 +1,000) — 7항목 모두 기대대로.

## [P8-3a] Kubernetes(kind) 로 옮기기 — 2026-11-16 ([#29](https://github.com/hyeokjinON/bank-transfer/pull/29), 머지 후 정리 #30)
### Added
- `infra/k8s/`: kind 클러스터(제어 1 + 워커 2, `kind.yaml` · 앱 입구는 `127.0.0.1:8000` 하나) · `versions.env`(kind v0.33.0 · 노드 `kindest/node:v1.36.4`(digest) · Envoy Gateway v1.9.3 · kubectl v1.36.4 · curl 8.23.0 · k6 2.3.0 한 곳) · 스크립트 `up.sh` · `down.sh` · `smoke.sh`([0] ~ [8]) · `lib.sh` · Kustomize `base`(MySQL · Kafka · Redis · 모의 은행(PVC) · 게이트웨이 · api 2대 · `EnvoyProxy` + `Gateway` + `HTTPRoute`(재시도 없음) · NetworkPolicy 7개 · api PDB) + `overlays/{local,ci,experiment-prestop0,experiment-liveness-bank,experiment-fast-evict}`, `cluster/`(namespace · GatewayClass), `README.md`.
- CI 잡 `k8s-smoke` (20분 제한: kind · kubectl 체크섬 설치 → 모든 overlay `kubectl kustomize` → 이미지 3개 빌드(gha 캐시) → `up.sh ci` → `smoke.sh` → 실패 시 상태 · 로그).
- 실험 도구 `load/k8s/` (`run.sh <steady|rolling|podkill|tagtrap|bank|drain|node> <회차>` · `cleanup`, `timeline.sh` · `timeline.awk`)와 실험 R1 ~ R3 · B1 · B2 · N0 · N1 · D1 · D2 · D2' ([결과](docs/perf/p8-3a-k8s.md), 원자료 `docs/perf/raw/p8-3a/`).
- 테스트 1개: `ProbeGroupsTest` (게이트웨이 readiness 그룹에 `bankConnection` 포함, liveness 에는 없음). 전체 623개 (P8-2 622 + 1, 건너뜀 2).
- ADR-0017 (채택), OPERATION §15 "Kubernetes 실행", SETUP §2-9 · §1 표 kind 줄, ARCHITECTURE "Kubernetes 실행" 그림, `infra/k8s/README.md`.
### Changed
- 게이트웨이 `application.yml`: readiness 그룹 = `readinessState,bankConnection`, 프로브 명시 (liveness 는 기본 — 은행 장애는 재시작 사유가 아니다).
- `load/compose/withdraw.js`: 결과 폴더를 `OUT_SUBDIR`(기본 `p8-2`)로 — K8s 실험은 `p8-3a` 로 같은 부하 스크립트를 쓴다 (한 줄).
- `.gitattributes`: `*.yaml` · `*.env` · `*.awk` 를 LF 로 (Windows 체크아웃에서 `source` 하는 `versions.env` 가 CRLF 로 깨지지 않게).
- 문서: README 로드맵 · 실행 안내 · 테스트 수(623), `infra/README.md` 에 k8s 안내 한 줄, HANDOVER P8-3a 줄.
### Fixed (구현 중)
- 롤링 뒤 api 두 Pod 가 한 워커로 몰리던 것: `topologySpreadConstraints` 가 롤링 중 옛 ReplicaSet Pod 까지 세었다 → `matchLabelKeys: [pod-template-hash]` (8aadd27). 몰린 상태에서 노드를 정지하면 api 0대 구간이 생긴다 (node-1: 5xx 883 · 포기 883).
- 실험 도구: 결과 파일을 판정 전에 쓰던 것 · api 분산 사전 확인 없음 · 롤링 기본 부하가 롤링보다 짧을 수 있음 · 장애 중 1원인증 응답 코드 `0000` 이 JSON 을 깨던 것 (b9a8f2e), 종료 중(Terminating) Pod 를 세던 사전 확인 · 복구 (8aadd27, node-2 는 무효 회차).
- 스모크 N0 가 오류에 통과할 수 있던 것: 양성 대조(`kubernetes.default:443`) · 숫자 확인으로 실패 쪽으로 닫음, 스모크 [0] 은 종료 중인 api Pod 가 없어질 때까지 기다림 (0633e3c).
### Fixed (최종 리뷰, 머지 전)
- 문서가 측정보다 세게 쓴 곳 정정 (D1 의 PDB 효과 · `up.sh` 시간 조건 · 겹침 4 ~ 7초 · 노드 표 숫자), CI `k8s-smoke` 진단을 시간 초과(cancelled)에도 출력, 스모크 N0 를 curl 종료 코드(28 만 막힘)와 FQDN 대상으로, mysql 준비 확인 명령에서 비밀번호를 뺌 (40a268b · 5562007).
### CI · 사람 확인 (머지 전)
- 첫 CI 실행: `build` 7분 51초 · `infra-smoke` 47초 · `app-smoke` 4분 38초 · **`k8s-smoke` 5분 45초** (20분 제한 그대로).
- Postman 직접 확인 (2026-11-16): 1원인증 `VERIFIED` · 출금 9건 `COMPLETED` · 같은 키 재요청 이중 0 · api Pod 삭제 직후 출금 4건 `COMPLETED` · 새 Pod 로 다시 2대 — 잔고 = 원장 합, 은행 기록 거래번호마다 1번.
### 측정 요약 ([결과](docs/perf/p8-3a-k8s.md), 이 PC 기준 — 운영 수치가 아니다, 돈은 전 회차 정확)
- **이식**: `up.sh` 재실행(클러스터 있음, 이미지 빌드 포함) 2분 7초 뒤 스모크 통과 (처음 생성 시간은 재지 않음), api 두 Pod 동시 기동에도 Flyway 는 버전마다 1번.
- **R1 롤링**: 롤링 중 첫 시도 5xx preStop 10s **0 · 0**, 0s **4 · 9** (전부 Envoy `503 UF`). **R2 Pod 강제 삭제** 3회 5xx 0, 대체 Pod Ready 10.8 ~ 11.2s. **R3** 같은 태그는 롤링이 일어나지 않는다.
- **B1 · B2 은행 장애**: readiness 에만(B1) 60 · 180s 장애에도 게이트웨이 재시작 **0**, 출금 전부 `COMPLETED` · 이중 0. liveness 에도 넣으면(B2) 재시작 1 · 2. 복구 편차(B1 6.7 ~ 34.0s)는 게이트웨이 재접속 백오프(상한 30s) 때문.
- **N0 · N1 NetworkPolicy**: 정책 전 연결됨(0.0011 · 0.0010s) → 후 막힘, kindnet 이 집행, Pod 전부 Ready 유지.
- **D1 · D2 · D2' 노드**: drain 5xx 0 (api 는 요청 받는 대상 기준 최소 1대 · Pod Ready 기준 2대, PDB 는 이번 배치에서 개입 안 함), 단 `Recreate` 인데도 퇴거 때 게이트웨이 두 대가 4 ~ 7초 겹침 (→ P8-3b). 노드 정지: 대체 api 363.1s, 퇴거 30s 로 88.2s, 감지 전 약 50초에 5xx 512 · 439.

## [P8-2] 앱 이미지 · Docker Compose 전체 실행 · api 2대 — 2026-11-11 ([#27](https://github.com/hyeokjinON/bank-transfer/pull/27), 머지 후 정리 #28)
### Added
- 루트 `Dockerfile` (타깃 `api` · `bank-gateway` · `mock-bank`, 멀티 스테이지 · Boot jar 레이어 추출 · JRE Alpine · root 아닌 사용자) · `.dockerignore` (`infra/.env` 제외), `.gitattributes` 에 `Dockerfile` · `.dockerignore` · `*.conf` LF.
- Compose `app` 프로필: nginx(PC `127.0.0.1:8000` 하나, 읽기 15s, 라운드 로빈, 연결 단계 실패만 재시도, `zone api`) · api-1 · api-2 · bank-gateway · mock-bank, 공통 블록(`x-java-app` · `x-bank-maintenance` · `x-api-env` · `x-api`), 대사 파일 공유 볼륨 `bank-files`.
- ShedLock: 하루 배치 둘(`daily-recon` · `data-retention`)을 api 여러 대 중 한 대만 (JDBC · `usingDbTime`), Flyway `V6__shedlock.sql` (`DATETIME(3)`, 값은 UTC).
- `/actuator/info` 의 `instance` (`INFO_INSTANCE`), liveness · readiness 헬스 그룹 명시.
- `infra/app-smoke.sh` + CI 잡 `app-smoke` (15분 제한, 이미지 빌드 → 기동 → nginx 로 1원인증 · 출금 · 멱등 · 분배 · 포트 · 이미지 비밀값 · 대사 파일 바인딩).
- 실험 도구 `load/compose/` (`run.sh <steady|restart|kill> <회차>` · `cleanup`)와 실험 E1 ~ E5 ([결과](docs/perf/p8-2-compose.md), 원자료 `docs/perf/raw/p8-2/`).
- ADR-0016 (채택), OPERATION §14 "컨테이너 실행", SETUP §2-8, `infra/README.md` `app` 프로필 절.
### Changed
- `bank.gateway.mode` 기본을 **`http`** 로 (api · api-reactive). 테스트 공통 기반 · 측정 스크립트(`load/lib/gateway.sh`) · 실행 구성 `5. API (부하 측정)` 은 `direct` 를 명시한다.
- api 에 `server.shutdown=graceful` · `spring.lifecycle.timeout-per-shutdown-phase: 20s` (Compose `stop_grace_period` 30s).
- 하루 배치 스케줄러 두 개와 `run()` 을 `open` 으로 (ShedLock `PROXY_METHOD` 프록시).
- 실행 구성: `0. Mock Bank + API` 에 게이트웨이(`11.`) 추가, `12. API (http 게이트웨이)` → `12. API (direct 직결)`. 실행 구성 `4` · `6` · `7` · `10` 은 이제 `http` 로 뜨므로 게이트웨이가 함께 필요하다.
- `docs/database.md`: §7 명명 규칙에 `shedlock` 예외 한 줄 · §4-10 · 시간대 예외 (UTC 값).
- 테스트: 전체 622개 (P8-1 608 + P8-2 14, 건너뜀 2).
### Fixed (머지 전 CI)
- `app-smoke` [5/7] 포트 확인이 CI 에서만 실패: CI 의 docker compose 는 공개 안 된 포트를 `invalid IP:0` 으로 찍는다 (로컬은 빈 줄 · `:0`). "안 열림" 출력을 나열하지 않고 `주소:1 이상 포트` 로 끝날 때만 열림으로 본다 (adfb6ba).

## [P8-1] 은행 게이트웨이 서비스 분리 — 2026-11-10 ([#25](https://github.com/hyeokjinON/bank-transfer/pull/25), 머지 후 정리 [#26](https://github.com/hyeokjinON/bank-transfer/pull/26))
### Added
- 신규 앱 `apps/bank-gateway` (포트 8082): 은행 FEP 연결 하나를 내부 HTTP API 3개(`/internal/v1/bank/{name-inquiries,transfers,inquiries}`)로 노출. 오류는 503 `BANK_NOT_SENT` · 504 `BANK_TIMEOUT` · 502 `BANK_UNAVAILABLE` · 400 · 500, 점검 시간 송신 보류, `/actuator/health/bankConnection`. 서버 두 판: WebFlux(기본) · MVC 비동기 (`spring.main.web-application-type`). 요청이 끊겨도 은행 송신은 취소하지 않는다 (클라이언트 응답 시간 초과로 연결을 닫는 계약 ⑨ 에서 "은행 1회 · 대기 표 비움" 을 네 조합 모두 확인).
- 신규 모듈 `modules/bank-gateway-http`: 계약 DTO · 실패 분류(`BankNotSentException` 만 "안 보냄", 503 은 코드가 있을 때만) · 클라이언트 두 판(WebClient 기본 · JDK `HttpClient`, `bank.gateway.http.client`), 자동 재시도 끔, 테스트 픽스처(`GatewayProcess` · `TcpCutProxy`).
- `apps/api` · `apps/api-reactive` 설정 `bank.gateway.mode` (`direct` 기본 | `http`)와 `bank.gateway.http.*` (base-url · max-connections 500 · connect-timeout 1s · pending-acquire-timeout 1s · response-timeout 5s).
- Toxiproxy (Compose 프로필 `chaos`): 망 지연 주입(측정), 지연 · 응답 막힘(latency · timeout toxic) 테스트(Testcontainers). 연결 끊김은 `TcpCutProxy`(계약 ⑦ · 흐름 테스트)로 다룬다. 계약 테스트 4조합(서버 2 × 클라이언트 2): 클라이언트가 연결을 닫아도 은행 1회(⑨), 호출자 future 취소는 HTTP 교환을 취소하지 않음(⑩, 의존 future), 받고 끊긴 뒤 은행 1회(⑦ · ⑪ — 재시도 설정 단독이 아니라 Reactor Netty 의 헤더 송신 뒤 재시도 안 함 동작과 함께 고정).
- 측정 도구 확장: `GATEWAY` · `SERVER` · `CLIENT` · `GW_POOL` · `NET_DELAY`, 게이트웨이 CPU · 스레드 표본 · 스레드 덤프. 측정 42회 + 재측정 2회 (원자료 `docs/perf/raw/p8-1/`).
- 테스트: 전체 608개 (P7 524 + P8-1 84, 건너뜀 2).
- ADR-0015 (채택), OPERATION "게이트웨이 장애 때", [측정 결과](docs/perf/p8-1-gateway-split.md), SETUP 2-3-1, 실행 구성 `11. Bank Gateway (8082)` · `12. API (http 게이트웨이)`.
### Changed
- 처리 코드(`TransferProcessor` · 어댑터)가 구현(Netty 직결)이 아니라 `BankGateway` 인터페이스를 본다 (동작 변화 없음, 기본 `direct`).
- `BankTimeoutException` 에 `timeout` 이 들어갔다 (HTTP 504 해석에 필요).
- 최종 리뷰 반영: 게이트웨이의 전문 거래일시 시계를 KST(`Asia/Seoul`)로, WebClient 연결 풀에 유휴 상한(`maxIdleTime` 15s · `evictInBackground` 30s), 테스트 픽스처 `GatewayProcess` 에 JVM 종료 훅.
- 측정 요약 스크립트(`load/consumer/summarize.sh` · `load/verification/summarize.sh`)가 원자료 종류별로 자기 JSON 만 읽는다 (컨슈머 `"tps"` · 웹 `"rate"` 키) — 한 폴더에 섞여 있어도 돈다 (`70c8ead`).
### Fixed
- 연결 풀 대기열 상한 초과(`PoolAcquirePendingLimitException`, Reactor Netty 기본 = 최대 연결 × 2)가 "모름" 으로 분류되던 것을 "안 보냄" 으로 (`7afaf5b`). 측정 D 풀 32 첫 두 회차가 정확성 확인에서 실패해 드러났다 (이중 송신은 없었다). 실제 라이브러리 예외로 분류 테스트, 풀 1 · 동시 4건 재현 테스트 추가.
### Fixed (머지 후 정리)
- CI `build` 가 P7 가상 스레드 실험 컨텍스트를 닫다가 30분 멈추던 것 (#26 첫 실행에서 재발, 자동 스레드 덤프로 원인 확인): 가상 스레드 컨슈머 약 16개가 한꺼번에 close 하며 Kafka 의 synchronized 안(`AppInfoParser.unregisterAppInfo` 등)에서 INFO 로그를 찍고, JDK 21 에서 모니터를 쥔 채 logback 출력 잠금을 기다려 캐리어를 붙잡았다 (냉시동 교착과 같은 모양). 실험 컨텍스트에서만 `org.apache.kafka` · `org.springframework.kafka` 로그를 WARN 으로 낮춰 잠금 안 로그 경로를 없앴다. 캐리어 4개로 3회 · api 모듈 330개 통과.

## [P7] 가상 스레드 비교 — 2026-11-09 ([#23](https://github.com/hyeokjinON/bank-transfer/pull/23), 머지 후 정리 [#24](https://github.com/hyeokjinON/bank-transfer/pull/24))
### Added
- `apps/api` 설정 `spring.threads.virtual.enabled` (기본 `false` 명시)와 `/actuator/info` 의 `threads.virtual`. 켜면 Tomcat 요청 · 앱 실행기 · `@Scheduled` · Kafka 리스너가 가상 스레드 (업무 코드 변경 없음).
- 테스트 6개: 기본값 고정 2 (`VirtualThreadDefaultTest`), 진행 조건 실험 4 — V1 무엇이 가상 스레드가 되나(탐침), V2 실제 경로 pinning 0 (JFR `RecordingStream` 0ms) · V2 대조(`synchronized` 안 `sleep`), V3 `jvm.threads.live` 는 가상 스레드를 세지 않음. 전체 524개.
- 측정 도구: 두 `run.sh` 에 `VT=1`(앱 인자 + JFR 녹화 → 끝나기 전 `jcmd JFR.dump` → `jdk.VirtualThreadPinned` 집계, MVC 판 · 블로킹 컨슈머만) · `RAW_SUB`(원자료 폴더), 공통 `load/lib/vt.sh` · `load/lib/proc.sh`, 표 `load/p7/summarize.sh`.
- ADR-0014 (제안), OPERATION §13, [측정 결과](docs/perf/p7-virtual-threads.md) (웹 28회 · 컨슈머 6회 — 원자료 `docs/perf/raw/p7/`). 냉시동 교착 재현 기록 (VT 켬 4/4 멈춤).
### Changed
- 웹 측정 회차(`load/verification/run.sh`)는 k6 전에 1원인증 3건을 순차로 보낸다 (모든 판). 정확성은 워밍업 뒤 기준값과의 차이로. 결과 JSON 에 `warmup` · `virtual_threads` · `pinned_events`.
- 두 `run.sh` 의 포트 · PID 찾기를 netstat 에서 `/dev/tcp` 접속 시험 · `jcmd -l` 로 (이 PC 의 netstat 은 0.0.0.0 대기 포트를 보여 주지 않았다).
- k6 스크립트(`verification.js`)의 요약 파일 경로가 `OUT_SUB` 를 따른다 (기본 `p6-2` 그대로).
### Changed (머지 후 정리)
- ADR-0014 채택. 손 확인 결과(가상 스레드로 띄운 개발용 API 에서 1원인증 → 출금 `COMPLETED`, 스레드 덤프로 리스너 · 스케줄러가 가상 스레드임을 확인)를 ADR-0014 · HANDOVER 에. README · ARCHITECTURE · 결과 문서의 "제안" 표기 정리.
### Fixed (머지 후 정리)
- 진행 조건 실험 V2 가 CI(4코어)에서 깨지던 것: 녹화 안의 1회성 초기화 로그가 출력 잠금을 기다려 pinning — 첫 Kafka 프로듀서 생성(`AppInfoParser.registerAppInfo`, `static synchronized`)과 가상 스레드 리스너의 컨슈머 시작(그룹 합류 · 오프셋 초기화, 캐리어 4개로 로컬 재현). 녹화 전에 프로듀서를 파티션 조회로 만들고 모든 리스너가 첫 fetch 를 보낼 때까지 기다린다. 실패 메시지의 스택은 8줄 → 40줄. 가상 스레드 컨텍스트는 클래스가 끝나면 닫는다 (ADR-0014 · 결과 문서 §4-4).
- CI `build` 단계: 30분 시간 제한, 20분이 지나도 돌면 JVM 스레드 덤프를 남겨 실패 산출물로 올린다 — P7 PR 의 마지막 실행이 3시간 멈췄는데 덤프가 없어 원인을 확인하지 못했다.

## [P6-2] 웹 1원인증 WebFlux 비교 — 2026-11-06 ([#21](https://github.com/hyeokjinON/bank-transfer/pull/21), 머지 후 정리 [#22](https://github.com/hyeokjinON/bank-transfer/pull/22))
### Added
- 신규 앱 `apps/api-reactive` (WebFlux, 포트 8081): 1원인증 요청 `POST /api/v1/account-verifications` 하나만, 코루틴 판(`withContext(io)`)과 Reactor 판(`fromFuture` · `publishOn(boundedElastic())`). 클라이언트가 끊으면 취소, 1원 이체 대기 중 끊김은 마스킹한 경고. 설정 `account.verification.web-mode`(`coroutine` 기본 · `reactor`).
- 신규 모듈 `modules/account-infra`: 1원인증 JPA 저장소 · 은행 응답 해석 · 오류 계약 기대표(`VerificationErrorContract`, test fixtures).
- `apps/api` 1원인증 `web-mode`(`async` 기본 · `blocking` 측정용 대조군), `/actuator/info` 에 판 · Tomcat 최대 스레드.
- 판정 순수 함수 `VerificationRequestRules`(`modules/account`).
- 진행 조건 실험 E1 ~ E4 (스레드 · 끊김 취소 · 예외), 끊김 테스트, 변이 검사 4종 (모두 테스트가 잡음), 오류 계약 테스트.
- 측정 도구 `load/verification/` (k6 열린 모델 · 측정용 포트 18080 · 18081 · 9190 · 시간 초과를 포함한 정확성 규칙 · `summarize.sh` · `cleanup`).
- IntelliJ 실행 구성 `8. API 리액티브 (코루틴)` · `9. API 리액티브 (Reactor)` · `10. API (1원인증 블로킹)`, `http/account-verification.http` 와 Postman `1-1R` 요청 (`reactiveBaseUrl`).
- ADR-0013 (제안), OPERATION §12, [측정 결과](docs/perf/p6-2-web-reactive.md) (82회: 본 72 · 원인 확인 3 · 한계 탐색 6 · 실행기 확인 1 — 원자료 `docs/perf/raw/p6-2/`).
### Changed
- 1원인증 JPA 저장소 · 은행 응답 해석을 `apps/api` 에서 `modules/account-infra` 로 이동 (동작 변화 없음). 이동한 패키지의 엔티티 스캔은 `@AutoConfigurationPackage(basePackageClasses = [앱 클래스], basePackages = …)`.
- `BankMaintenanceGuard` · `BankMaintenanceException` 을 `apps/api` 에서 `modules/bank-gateway` 로 이동 (두 앱 공유).
- `apps/api` 1원인증 컨트롤러를 셋으로 나눔: 확인만(`AccountVerificationController`) · 비동기 요청 · 블로킹 요청 (경로 · 응답 그대로).
- 모의 은행 입금 기록을 `CopyOnWriteArrayList` 에서 `ConcurrentLinkedQueue` 로 (O(1) 추가, 순서 · `depositsOf()` 그대로).
- 용어 정리: "퍼밋" → "은행 호출 자리(세마포어 permit)" (ADR-0012 · P6-1 결과 문서 · ARCHITECTURE · HANDOVER · 코루틴 리스너 주석 · 테스트 이름).
- 예상 못 한 예외의 500 응답이 Boot 기본 모양에서 ProblemDetail + code `INTERNAL_ERROR` 로 (두 앱 · 네 판 같게). 처리기에 `Exception::class` 를 더했고, 리액티브 처리기는 취소(`CancellationException`)를 응답으로 바꾸지 않고 다시 던진다. 계약 테스트가 처리기 라우팅까지 확인.
- 전체 테스트 515개.
### Fixed (머지 후 정리)
- 리액티브 처리기가 취소 예외를 다시 던지는지 지키는 테스트 `ReactiveExceptionHandlerTest` (2개). P6-1 `ConsumerModeWiringTest` 의 끝이 열린 정규식을 `[,}]` 로 막음. 전체 테스트 518개.
- 진행 조건 실험 E1 (`WebFluxExperimentTest`) 이 CI 에서 가끔 깨지던 것: 은행 응답이 `await` · 구독보다 먼저 오면 기다린 뒤 코드가 게이트웨이 루프가 아니라 부른 HTTP 루프에서 이어진다. 은행 지연을 둔 결정적 E1 둘 + 지연 없이 "이벤트 루프 중 하나" 인지 보는 E1 하나로 나눔. 흐름 KDoc · ADR-0013 · 결과 문서 §3-2 에 두 경우를 적음 (동작 · 결정 변화 없음).
- ADR-0013 채택. 결과 문서 §5-3 표본 표기 · 천장 칸 표본 간격, ADR-0013 의 Reactor 스레드 조건, "자리를 먼저 받는다" → "잡는다" (ADR-0012 · HANDOVER).

## [P6-1] 컨슈머 코루틴 — 2026-11-05 ([#19](https://github.com/hyeokjinON/bank-transfer/pull/19), 머지 후 정리 [#20](https://github.com/hyeokjinON/bank-transfer/pull/20))
### Added
- 출금 컨슈머의 코루틴 판 `SuspendingTransferRequestedListener` (Spring Kafka suspend 리스너): 은행은 `await`, DB 는 IO 디스패처, 은행 동시 호출 상한 `Semaphore` (은행 호출 자리(세마포어 permit)를 잡은 뒤 `begin`).
- 설정 `transfer.consumer.mode`(`blocking` 기본 · `coroutine`, 소문자만) · `transfer.consumer.coroutine.max-in-flight-bank-calls`(64, 1 ~ 1024), 범위를 벗어나면 기동 실패. `/actuator/info` 에 모드 · 상한 · 풀 크기.
- `SuspendBankTransferPort` · `SuspendBankTransferAdapter`(취소는 분류하지 않고 다시 던짐), 모의 은행 `--response-delay=<0~2999>ms` · 동시 대기 최댓값.
- Spring Kafka suspend 리스너 실험 테스트(재시도 토픽 · poll 묶음 정지 · await 뒤 스레드), 코루틴 모드 흐름 테스트, 측정 도구 `load/consumer/`(개발 데이터가 섞여 있으면 시작하지 않는 사전 확인 포함), 실행 구성 `7. API (코루틴 컨슈머)`, ADR-0012, OPERATION §11. 테스트 411 → 465개.
- 의존성 kotlinx-coroutines-core · -reactor 1.10.2 (Boot BOM).
- 측정 결과 `docs/perf/p6-1-consumer-coroutines.md` · `docs/perf/raw/p6-1/` (비교 24 + 대조 3 + 용량 탐색 21 + drift 1 = 49회): 은행 지연별 두 모드 처리량, 대조 실험, 상한 · Hikari 용량 탐색, 현상과 읽는 법. ADR-0012 결과 절에 측정과 운영 기본 판단(blocking 유지 · 전환 조건).
### Changed
- `TransferProcessor.process` 를 `begin` · `finish` 로 나눴다 (동작 변화 없음). 리스너의 key · 결과 마무리 · DLT 를 `TransferRequestedHandling` 으로, 예외 분류를 `classifyBankFailure` 로 뽑아 두 판이 같이 쓴다. 블로킹 리스너 빈은 `ConsumerConfiguration` 으로 옮겼다.
### Fixed
- `OutboxRelayTest` 의 발행 확인이 가끔 깨지던 것: Awaitility `untilAsserted` 는 `AssertionError` 만 다시 시도하는데 `single{}` 이 `NoSuchElementException` 을 던져 사실상 poll 한 번이었다. 토픽 끝 오프셋까지 읽는 방식으로 고쳤다.
### Fixed (머지 후 정리)
- `RetryTopicFlowTest` 의 커밋 오프셋 대기 3곳에 `ignoreExceptions()` — 그룹 코디네이터가 아직 준비되지 않아 조회가 예외를 던지면 기다림이 바로 끝나던 것 (`OutboxRelayTest` 와 같은 종류).
- ADR-0012 채택. 코루틴 리스너 주석 · ADR 의 "`begin` 의 DB 예외는 `RECEIVED` 로 남는다" 에 전제(ack 와 재시도 발행 사이에 죽었을 때)를 붙이고, 결과 문서 §7-2 제목에 확인 · 추정 구분을 붙였다.

## [P5-2] 보존 배치 — 2026-11-04 ([#17](https://github.com/hyeokjinON/bank-transfer/pull/17), 머지 후 정리 [#18](https://github.com/hyeokjinON/bank-transfer/pull/18))
### Added
- Spring Batch 6 잡 `dataRetentionJob`(① `purgeOutbox` ② `purgeVerifications`), `DELETE … LIMIT n` 을 READ COMMITTED 트랜잭션마다 한 번. 발행된 `outbox_event`(7일) · `VERIFIED` 가 아닌 만료 `account_verification`(30일)만 지운다.
- Flyway V5 `idx_verification_purge (status, expires_at)`, 설정 `data-retention.*`(범위를 벗어나면 기동 실패).
- 스케줄 매일 06:30 KST · `POST /admin/retention-runs`(200 / 409 `RETENTION_ALREADY_COMPLETED` · `RETENTION_RUNNING` / 400 `RETENTION_DATE_IN_FUTURE`), 실패 WARN 알림.
- `http/data-retention.http`, Postman 7 폴더, ADR-0011, database.md 보존 정책표(§4-9), OPERATION §10.
### Changed
- 시작 경합 처리를 `DatedJobLauncher` · `DatedJobSingleRunGuard` 로 뽑아 대사와 같이 쓴다 (대사 동작 변화 없음).
- 스케줄 풀 3 → 4. `Job` 빈이 둘이 되어 대사 쪽 주입에 `@Qualifier`.
- 부하 측정 실행 구성 · 명령줄에 `--data-retention.schedule.enabled=false`.
### Fixed
- `AlertConfigurationTest` 가 PC 의 `ALERT_TELEGRAM_*` 환경 변수에 좌우되던 P5-1 테스트 버그 (커밋 `3be03bb`).

## [P5-1] 일마감 대사 · 은행 점검 시간 · 알림 — 2026-10-30 ([#15](https://github.com/hyeokjinON/bank-transfer/pull/15), 머지 후 정리 [#16](https://github.com/hyeokjinON/bank-transfer/pull/16))
### Added
- 대사 파일 형식 v0.3 (`ReconFileFormat`, 헤더 · 상세 · 트레일러, 59바이트 레코드), 모의 은행 거래 기록 · 재기동 복원 · 00:05 일마감 · `--close-day` · `--bank-files`.
- `BankMaintenanceWindow` · `SendGate` (점검 시간에는 은행으로 보내지 않음), 웹 503 `BANK_MAINTENANCE` + `Retry-After`.
- Spring Batch 6 잡 `dailyReconciliationJob`(6단계) · `StatementMatcher` · `StatementSettler` · 응답코드 `E002`, Flyway V4(Batch 메타 · `bank_statement_line` · `recon_issue`).
- 스케줄 01 ~ 06시 매시 · `POST /admin/reconciliations`(200 / 409 `RECON_ALREADY_COMPLETED` · `RECON_RUNNING` / 400 `RECON_DATE_NOT_CLOSED`), 마지막 회차의 "대사 미완료" 알림(실패 · 실행 중 모두).
- `AlertNotifier` · `TelegramNotifier` · `LogNotifier`, 설정 `bank.maintenance.*` · `daily-recon.*` · `alert.telegram.*`.
- 실행 구성 `6. API (대사 손 확인)`, `http/daily-recon.http`, Postman 6 폴더, ADR-0010, 전문 규격 v0.3, 테스트 240 → 370개.
### Changed
- `Clock.systemUTC()` 배선을 `Clock` 빈 하나로.
- 기존 경고 3곳(`CONFLICT` · DLT · 결과조회 한도)에 WARN 알림.
- Gradle `run` · `bootRun` 작업 디렉터리 = 저장소 루트.
- 스케줄 풀 2 → 3.
- `spring-boot-starter-batch-jdbc` 추가.
- 부하 측정 실행 구성에 `--daily-recon.schedule.enabled=false`.
### Fixed (최종 리뷰)
- ③ 자동 확정(완료 전이 · `E002` 실패 + 환불)은 `sending_at` 이 아직 대사일 안일 때만 — 상태와 같은 조건부 UPDATE 의 조건(`TransferStore.transition` · `fail` 의 `sentWithin`). 읽은 뒤 다시 보내진 출금을 환불해 이중 지급이 되던 틈을 막았다. 그런 건은 새 판정 `RESENT_LATER`(확정 · 이슈 없음), 요약 `resentLater`.
- ④ · 줄 수에서 빼는 1원인증은 `V…` 이면서 1원인 줄만 — 1원이 아닌 `V…` 줄은 `UNKNOWN_REFERENCE` + 알림.
- 대사 파일 날짜 · 시각 엄격 해석(`20260230` · `240000` 거부).
- Postman 컬렉션 변수 `manualWalletBusy` 중복 키(lint 오류) 제거, 6-4 도 `reconDate` 기본값.
### Fixed (마감 후)
- 수동 대사는 마감된 날(어제 이전, KST)만 — 오늘 이후 400 `RECON_DATE_NOT_CLOSED`(`RECON_DATE_IN_FUTURE` 대체). 오늘을 대사하면 아직 은행 파일에 없는 방금 보낸 출금을 `E002` 환불 → 은행이 그 뒤 처리하면 이중 지급. 손 확인용 설정 `daily-recon.allow-today`(기본 `false`, 실행 구성 6 만 `true`).
- `StatementSettler.settle` 의 범위 인자 기본값 제거(③ 은 D일 범위, ④ 는 `null` 명시) + ③ 쓰기가 범위를 넘기는 배선을 잡 수준 테스트로 고정.

## [P4-2] 잔고 차감 락 전략 비교 — 2026-10-28 ([#13](https://github.com/hyeokjinON/bank-transfer/pull/13), 머지 후 정리 [#14](https://github.com/hyeokjinON/bank-transfer/pull/14))
### Added
- `WalletDebit` 이음새와 전략 4종(`conditional` · `pessimistic` · `optimistic` · `redis`), 설정 `wallet.*` · `GET /actuator/info` 의 전략.
- 오류 코드 `WALLET_BUSY`(503) · `CONCURRENT_UPDATE`(409) + `Retry-After: 1`.
- Redisson 4.8.0, 테스트용 Redis 컨테이너(`redis:7.4-alpine`).
- `load/` (k6 2.3.0 핫스팟 · 분산, 시드 · 정리 · 검증 · `run.sh` · `summarize.sh`), compose `k6` (`load` 프로필), 실행 구성 `5. API (부하 측정)`.
- ADR-0009, 측정 결과 `docs/perf/p4-2-lock-comparison.md` · 원본 `docs/perf/raw/` (24회 + drift 확인 1회).
### Changed
- Hikari 풀 20 명시 · 락 대기 5초(`innodb_lock_wait_timeout`, 모든 커넥션).
- MySQL 1205 · 1213 → 503 (전에는 500).
- `saveWithEvent` 를 바깥 트랜잭션 안에서 부르면 거부.
- 차감 SQL 이 `WalletLedger` 에서 전략으로.

## [P4-1] 원화 잔고 — 2026-10-26 ([#11](https://github.com/hyeokjinON/bank-transfer/pull/11), 머지 후 정리 [#12](https://github.com/hyeokjinON/bank-transfer/pull/12))
### Added
- `wallet` · `wallet_ledger`(V3), 잔고 SQL 한 곳 `WalletLedger`, 지갑 API(`POST /api/v1/wallet/deposits` 관리 · 테스트용 충전, `GET /api/v1/wallet` 잔고 · 보류).
- `TransferStore.fail()`: 실패 확정과 환불을 한 트랜잭션에. 오류 코드 `USER_ID_REQUIRED` · `INSUFFICIENT_BALANCE`.
- ADR-0008, 동시성 테스트(출금 100건 동시 → 정확히 10건 · 잔고 0, 같은 키 20건 → 차감 1번), 손 확인(`http/transfer.http` 9절 · Postman 5 폴더).
### Changed (**호환 깨짐**)
- 모든 출금 · 1원인증 API 에 `X-User-Id` 헤더 필수.
- 출금은 접수할 때 잔고에서 차감 (충전이 필요하고, 모자라면 422).
- 멱등키 UNIQUE 를 `(user_id, idempotency_key)` 로 (사용자마다 같은 키를 쓸 수 있다).
- 받는 계좌는 내가 인증한 계좌만. 남의 출금 조회 · 남의 인증 확인은 404.
- `transition(…, FAILED)` 금지: 실패 확정 4경로 모두 `fail()`.
- `CONFLICT` 경고에 "잔고 그대로".

## [P3-3b] 재시도 토픽 · 제한 재발행 — 2026-10-26 ([#9](https://github.com/hyeokjinON/bank-transfer/pull/9), 머지 후 정리 [#10](https://github.com/hyeokjinON/bank-transfer/pull/10))
### Added
- 재시도 토픽 `transfer.requested-retry-0/1/2` · `transfer.requested-dlt` (5s · 30s · 60s, 설정 `transfer.retry.*`), DLT 핸들러(이유별 로그: 은행 연결 없음 · 잘못된 key 는 WARN, 그 밖의 예외는 자동 재처리가 없어 ERROR + ack), key 가 UUID 가 아니면 재시도 없이 바로 DLT (`InvalidTransferKeyException`).
- 정리 작업 ⓪ 재발행 · 포기: 5분(`transfer.reconcile.republish-after`) 넘게 `RECEIVED` 이고 마지막 재발행 이후 은행에 보내려 한 흔적(`sending_at > republished_at`)이 있으면 아웃박스로 다시 발행, 3번(`max-republish`)을 다 쓰면 `FAILED` `E001`.
- `TransferStore.findRepublishable` · `republish`(조건부 UPDATE 와 아웃박스 INSERT 를 한 트랜잭션에), `InquiryOutcome.Unavailable`, V2 마이그레이션(`republish_count` · `republished_at`).
- 모의 은행 `pause` / `resume` (은행 장애 재현: 붙은 연결도 끊고 같은 포트로 다시 연다).
- ADR-0007, 손 확인(`http/transfer.http` 8절 · Postman 4 폴더 · 실행 구성 `4. API (재발행 손 확인)`).
### Changed
- 블로킹 `kafkaErrorHandler` 삭제: 재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막지 않는다. 수동 ack 는 유지하고 넘긴 레코드의 오프셋은 프레임워크가 단계마다 커밋한다.
- 상태 전이 `RECEIVED → FAILED` 추가 (정리 작업 ⓪ 만 사용, `bank_response_code = E001` = 은행에 간 적 없음).
- 리스너 그룹 · 동시성을 yml 한 곳으로 (`spring.kafka.consumer.group-id`, `listener.concurrency: 3`). 재시도 · DLT 그룹은 접미사로 나뉜다.
- 결과조회 중 연결 없음은 그 주기의 남은 조회를 멈추고 `inquiry_attempts` 를 올리지 않는다.
- 재시도 토픽 전용 1스레드 스케줄러 (`RetryTopicSchedulerWrapper`).

### Fixed (머지 후 정리)
- DLT ERROR 로그의 `cause=` 가 spring-kafka 래퍼 문구 대신 원래 예외 메시지를 보여 준다 (실제 4.1 헤더 모양 "래퍼; 원인" 으로 가른다).

## [P3-3a] 결과조회와 정리 작업 — 2026-10-21 ([#7](https://github.com/hyeokjinON/bank-transfer/pull/7))
### Added
- 전문 규격 v0.2: 이체 개별부에 이용기관 거래번호 20 추가(52 → 72바이트), 신규 업무 `INQ` 결과조회(요청 거래번호 20, 응답 거래번호 20 + 처리결과 4: `0000` 처리됨 / `2001` 원거래 없음) ([protocol.md](docs/protocol.md)).
- `BankInquiryPort` · `BankInquiryAdapter`: 결과조회는 확실한 답(`0000` · `2001`)만 확정하고 나머지(조회 거절 · 시간 초과 · 연결 없음)는 모름으로 둔다. 예외를 던지지 않는다.
- 정리 작업 `TransferReconciler`(10초 고정 지연, 한 번에 100건): 30초 넘게 `SENDING` 인 건 → `UNKNOWN`, `UNKNOWN` → 결과조회로 `COMPLETED` / `FAILED`, 10번 모르면 멈추고 경고(일마감 대사 대상). 대상은 `status` 로 고른다. 설정은 `transfer.reconcile.*`.
- 결과조회는 `sending_at` 30초 유예 뒤에 한다(컨슈머가 3초 타임아웃으로 적은 `UNKNOWN` 도 포함): 느린 은행의 `2001` 로 처리 중인 건이 `FAILED` 가 되지 않게. 결과조회 응답의 거래번호가 물은 것과 다르면 모름으로 둔다.
- `TransferStore.findSendingBefore` · `findUnknownForInquiry` · `recordInquiryAttempt`(조건부, `UNKNOWN` 에서만 증가).
- 모의 은행: 이체의 거래번호를 기억해 결과조회에 답하고, `9990000000002` 는 처리하고 응답만 보내지 않는다(1원 이체는 정상 응답). 테스트에서 `9990000000001` 은 처리도 응답도 하지 않는다.
- ADR-0006, `http/transfer.http` 의 "손 확인: 처리 후 무응답" 절.
### Changed
- 이체 전문 72바이트: 출금이체는 저장된 `transfer.reference`, 1원인증은 API 어댑터가 만든 `V` 거래번호를 싣는다(저장 안 함).
- 컨슈머 경합: 정리 작업이 먼저 `UNKNOWN` 으로 바꿨어도 은행의 확정 결과(완료 · 거절)를 받으면 `UNKNOWN → COMPLETED/FAILED` 로 기록한다. 이미 반대 결과로 확정돼 있으면 `CONFLICT` 경고를 남기고 넘어간다(던지지 않는다).
- 스케줄러 스레드 2개(`spring.task.scheduling.pool.size`): 발행기와 정리 작업이 서로 막지 않는다.
- 스키마 변경 없음 (`inquiry_attempts` 는 V1 에 있었다).

## [P3-2] 출금이체 처리 — 2026-10-21 ([#5](https://github.com/hyeokjinON/bank-transfer/pull/5))
### Added
- 폴링 퍼블리셔 `OutboxRelay`: 미발행 `outbox_event` 를 `FOR UPDATE SKIP LOCKED` 로 잡아 `transfer.requested` 토픽에 발행하고, 브로커 확인 뒤 `published_at` 기록 (적어도 한 번). 간격 · 배치 크기는 `transfer.outbox.relay.*`.
- Kafka 설정: 프로듀서 `acks=all` + 멱등, 컨슈머 수동 ack, 토픽 `transfer.requested`(파티션 3, key = 이체 id), 로컬 Kafka Testcontainers.
- 컨슈머 `TransferRequestedListener` (그룹 `transfer-processor`, 컨슈머 3) 와 처리 규칙 `TransferProcessor`: 조건부 `RECEIVED → SENDING`, 은행 호출은 트랜잭션 밖, 결과 4분류(완료 · 거절 · 결과 모름 · 미전송). 결과 모름(포트 예외 포함)은 `UNKNOWN` 으로 두고 다시 보내지 않는다.
- `TransferStore.transition`: 현재 상태가 맞을 때만 바꾸는 조건부 UPDATE (`sending_at` KST 기록).
- `BankTransferAdapter`: 게이트웨이 예외를 완료 · 거절 · 결과 모름 · 미전송으로 분류.
- 컨슈머 에러 핸들러: 은행에 못 보내면 1초부터 두 배씩(최대 10초) 5번 재시도 후 로그를 남기고 넘어감 (블로킹 재시도, P3-3 에서 재시도 토픽 · DLT 로 교체).
- ADR-0005 (제안), `http/transfer.http` 2-1) · Postman `2-9` 처리 결과 확인 요청.
### Changed
- 게이트웨이: 전문을 쓰기 전 연결이 없으면 `BankNotSentException`(`BankUnavailableException` 하위)으로 구분 — 은행에 안 갔으므로 다시 보내도 안전.
- 요청 형식 오류(본문 JSON 깨짐 · 검증 실패 · 경로 변수 타입 오류 …)도 `code: INVALID_REQUEST`, 그 밖의 표준 Spring 오류는 HTTP 상태 이름을 `code` 로 응답. 잘못된 상태 전이는 409 `ILLEGAL_TRANSITION`.
- `Transfer.toString()` 은 계좌번호 뒤 4자리만 남기고 적요는 찍지 않는다.
- API 실행에 Kafka 가 필요하다.

## [P3-1] 출금이체 접수 — 2026-10-20 ([#3](https://github.com/hyeokjinON/bank-transfer/pull/3))
### Added
- `modules/transfer`: 상태 전이 규칙, 멱등 접수 서비스, 이용기관 거래번호 발급 (Spring·JPA 의존 없음).
- `POST /api/v1/transfers` (Idempotency-Key 필수, 202/200), `GET /api/v1/transfers/{id}`.
- MySQL + Flyway `V1__init.sql` (account_verification · transfer · outbox_event), 시각 KST 저장.
- 트랜잭셔널 아웃박스: 이체와 `TransferRequested` 이벤트를 한 트랜잭션에 저장.
- Testcontainers MySQL 통합 테스트 (공유 컨텍스트), ADR-0004, `http/transfer.http`.
- 멱등 키 · 거래번호는 `ascii_bin` 으로 바이트 비교 (키는 보이는 ASCII 1~64자). 금액이 소수 · 문자열이면 400.
### Changed
- 1원인증 저장소: 메모리 → MySQL (`SELECT … FOR UPDATE`). 재시작해도 인증 기록이 남는다.
- 은행 응답 뒤 처리를 Netty 이벤트 루프 대신 앱 실행기에서 실행 (`thenApplyAsync`).
- API 실행에 MySQL 이 필요하다.

## [P3-0] 로컬 인프라 — 2026-10-16 ([#1](https://github.com/hyeokjinON/bank-transfer/pull/1))
### Added
- `infra/docker-compose.yml`: MySQL 8.4, Kafka 4.1 (KRaft 단일 노드), Redis 7.4 (AOF), 선택 도구 kafka-ui(`--profile tools`).
- `infra/verify.sh`: MySQL 쓰기/읽기(utf8mb4), Kafka 토픽 생성→produce→consume, Redis SET/GET/TTL 확인.
- CI `infra-smoke` 잡: 같은 Compose 파일을 띄우고 `verify.sh` 실행, 실패 시 로그, 항상 `down -v`.
- ADR-0003 로컬 인프라 결정 기록.
### Fixed
- `.gitattributes`: 기존 `gradlew`·`*.bat`·`*.jar` 규칙을 덮어쓴 실수 복원, `*.sh`·`*.yml` LF 고정 추가.
- Windows Git Bash 에서 `verify.sh` 의 Kafka 단계 실패 (MSYS 경로 변환) → `MSYS_NO_PATHCONV=1`. PowerShell `bash`(WSL 실행기) 대신 Git Bash 안내.

## [chore] IntelliJ 실행 구성 — 2026-10-07
### Added
- `.run/` 실행 구성 4개 (Mock Bank + API, 각각 실행, 전체 테스트), `http/account-verification.http` 요청 모음.

## [P0–P2] 은행 전문 게이트웨이와 1원 계좌인증 — 2026-10-01
### Added
- P0: Gradle 9 멀티모듈(Kotlin DSL), Kotlin 2.3, Spring Boot 4.1, Java 21, GitHub Actions `build` 잡.
- P1: 1원 계좌인증 API (`POST /api/v1/account-verifications`, `POST /{id}/confirm`) — 성명조회 → 1원 이체 → 코드 확인, 해시 저장, 5분 유효, 5회 잠금, 서블릿 비동기.
- P2: 모의 은행 FEP(Netty 서버), 게이트웨이(폴링 `IdleStateHandler`, 지수 백오프 재접속, 전문번호 → `CompletableFuture` 응답 매칭, 3초 타임아웃).
- 전문 규격 v0.1 (`docs/protocol.md`), ADR-0001(모듈러 모놀리스 + 포트), ADR-0002(프레이밍 · 응답 매칭).
- 테스트 29개 (직렬화, 쪼개진 TCP 패킷, 동시 50건 매칭, 타임아웃, 재접속, API 통합).
### Changed
- CI 액션을 v6 으로 올려 Node 20 경고 제거.
