# P9a 관측성 — 지표 · 대시보드 · 알림 (Micrometer · Prometheus · Grafana · Alertmanager) — 설계

> 날짜: 2026-11-17 · 브랜치 `feat/p9a-metrics` · 기준 main `ffcad9e` (P8-3c #33 · 정리 #34 머지)
> P9 전체: **P9a 지표 · 대시보드 · 알림 (이 문서)** → P9b 분산 추적 (OpenTelemetry) → P9c 로그 모으기. P9b · P9c 는 별도 스펙.
> 출발점: P8 실험의 숫자(은행 공백 · 리더 교대 · Pending 시간)는 모두 **로그와 kubectl 표본을 손으로 맞춰** 냈다. 앱에 업무 지표가 0개이고 Prometheus 형식 노출도 없다 (코드 확인 — actuator 노출은 `health,info`, 게이트웨이만 `metrics`).

## 1. 목표와 성공 기준
"돈이 제대로 흐르고 있나" 와 "어느 부품이 문제인가" 를 **지표로 보이게** 하고, 사람이 지금 무언가를 해야 할 때만 **알림**이 가게 한다. 실행 환경은 kind(K8s) 만.

성공 기준:
1. 돈 정확성 — 이전 단계와 같은 판정(접수 키 = 출금 행 = 거래번호, 전부 확정, 은행 빠짐 0 · 이중 0, 잔고 = 원장 합)이 모든 실험에서 통과.
2. 노드 · 리더 장애 실험(M1)에서 **지표로 본 은행 공백과 모의 은행 로그로 잰 공백의 차이 ≤ 5초** (수집 간격).
3. 알림 도착 시간(M2 · M3)이 계산값(수집 5 + 평가 5 + `for` + 묶기 대기 10초) **±10초** 안. 억제(inhibit)가 동작.
4. 은행 점검 시간 모의(M5)에서 **거짓 알림 0건**.
5. `k8s-smoke` 가 20분 제한 안에서 스모크 [10] 포함 통과. Envoy 입구로 `/actuator/prometheus` 는 404.

## 2. 결정 (브레인스토밍 2026-11-17 승인)
| # | 결정 | 이유 |
|---|---|---|
| D1 | P9 를 a(지표 · 대시보드 · 알림) / b(추적) / c(로그) 로 나눈다 | 한 PR 크기. 지표가 장애 분석의 출발점 |
| D2 | kind 에만 붙인다 (Compose 제외) | 게이트웨이 이중화 · 노드 장애를 그래프로 볼 수 있는 곳. 설정 한 벌 |
| D3 | 업무 지표 = **사건 카운터 · 타이머 + 상태는 DB 를 주기적으로 읽는 Gauge** | "끝나지 않고 쌓이는 건" 은 카운터로는 못 본다 (Pod 재시작 때 카운터 0). Observation API 는 P9b 에서 필요한 곳만 |
| D4 | Prometheus 는 Operator 없이 Deployment + 설정 파일, `kubernetes_sd`(pod, `bank-transfer` namespace 만) | CRD · 컨트롤러가 늘지 않고 읽기 쉽다. 운영이면 Operator · 관리형 |
| D5 | 알림 = 규칙 + Alertmanager → 클러스터 안 웹훅 수신기(alert-sink, 로그 출력) | 외부 메신저 · 토큰 없이 "도착 · 묶기 · 억제 · 해제" 를 잰다 |
| D6 | Envoy HTTPRoute 를 **허용 목록**으로 (`/api/v1` · `/admin` · `/actuator/health` · `/actuator/info`) | 지금은 모든 경로를 넘겨 `/actuator/prometheus` 가 PC 입구로 노출된다. 관리 포트 분리는 범위가 커서 다음 (§12) |
| D7 | 라벨에 사용자 id · 계좌 · 참조번호 · 금액을 넣지 않는다 | 시계열 폭발 방지 · 개인정보 |
| D8 | 점검 시간은 앱이 `banktransfer.bank.maintenance` 로 내고, 규칙이 그것으로 제외한다 | 점검 시간 설정을 앱 한 곳에만 둔다 (Alertmanager 에 같은 시각을 다시 적지 않음) |
| D9 | Alertmanager 가 api 를 거쳐 기존 `AlertNotifier`(텔레그램 · 로그)로 보내는 안은 **고르지 않는다** | 감시 대상(api)이 죽으면 알림 경로도 죽는다. 기존 P5 알림은 업무 결과용으로 그대로 |

## 3. 앱 — 노출 · 기본 지표
- `api` · `bank-gateway` 에 `io.micrometer:micrometer-registry-prometheus` (Boot BOM 버전). `management.endpoints.web.exposure.include` 에 `prometheus` 추가 (게이트웨이는 기존 `metrics` 유지).
- 공통 태그 `application` = `spring.application.name`. Pod · 노드는 Prometheus 가 붙인다 (§6).
- 기본 지표: JVM(힙 · GC · 스레드), `http.server.requests`, Hikari, Tomcat(`server.tomcat.mbeanregistry.enabled=true` — api), Spring Kafka 컨슈머(lag 포함 — Boot 자동 구성의 Micrometer 리스너. 계획 단계에서 `/actuator/prometheus` 로 lag 시계열 존재 확인).
- `http.server.requests` 히스토그램: `percentiles-histogram` 을 켜되 `minimum-expected-value 5ms` · `maximum-expected-value 10s` 로 버킷 범위를 제한 (위험 B2).
- `api-reactive` 는 kind 에 없어 제외. 모의 은행은 지표 없음.

## 4. 앱 — 업무 지표 (이름 앞 `banktransfer.`)
### 4-1. api 사건 지표 (Counter · Timer)
| 지표 | 종류 · 태그 | 세는 곳 |
|---|---|---|
| `transfer.accepted` | Counter `result=created\|replayed` | 접수 API (멱등 재요청 = replayed) |
| `transfer.bank.result` | Counter `outcome=ok\|rejected\|not_sent\|unknown\|maintenance` | 컨슈머의 은행 호출 결과 (P8-1 실패 분류와 같은 기준). 점검 보류는 `BankMaintenanceException` 이라 따로 `maintenance` |
| `transfer.completed` | Timer `status=COMPLETED\|FAILED`, 히스토그램 — **접수(created_at) → 확정** 시간 | 최종 상태 전이 |
| `transfer.gave_up` | Counter | 재발행 한도 소진 FAILED(E001) |
| `transfer.dlt` | Counter | DLT 리스너 |
| `transfer.republished` | Counter | 정리 작업 재발행 |
| `transfer.reconciled` | Counter `result=completed\|failed\|still_unknown` | UNKNOWN 결과조회 |
| `outbox.published` | Counter `result=ok\|error` | OutboxRelay |
| `bank.maintenance` | Gauge 0/1 | 점검 창(`bank.maintenance.*`, KST) 판정 — api · 게이트웨이 둘 다 |

**세는 시점 (위험 A4)**: 상태 전이 지표는 **저장 메서드가 성공적으로 돌아온 뒤**(커밋 뒤) 한 곳(`TransferMetrics` 컴포넌트)에서 센다. 트랜잭션 안에서 세지 않는다 — 롤백 · 낙관 락 재시도 때 두 번 세지 않게. 정확한 호출 지점은 계획에서 코드로 고정한다.

**카운터는 근사치 (위험 B3)**: Kafka 최소 한 번 전달 · Pod 재시작으로 카운터는 정확한 건수가 아니다. **돈의 정답은 DB**, 지표는 추세 · 장애 신호용 (문서에 명시).

### 4-2. api 상태 지표 (Gauge — 10초마다 DB 조회)
| 지표 | 쿼리 요지 | 인덱스 |
|---|---|---|
| `transfer.open{status}` | `status IN ('RECEIVED','SENDING','UNKNOWN')` 별 건수 | `idx_transfer_status (status, sending_at)` 범위 — 쌓인 COMPLETED 를 읽지 않는다 |
| `transfer.open.oldest.seconds{status}` | RECEIVED: `MIN(updated_at)`, SENDING · UNKNOWN: `MIN(sending_at)` → 나이 | SENDING · UNKNOWN 은 인덱스 안. **RECEIVED 의 `updated_at` 은 인덱스 밖** (위험 B1) |
| `transfer.unknown.exhausted` | UNKNOWN 이고 `inquiry_attempts >= transfer.reconcile.max-inquiries` 인 건수 | 같은 인덱스 + 행 읽기 (UNKNOWN 은 적다) |
| `outbox.unpublished` · `outbox.unpublished.oldest.seconds` | `published_at IS NULL` | `idx_outbox_unpublished` |
| `metrics.db.query` | Timer `result=ok\|error` — 위 조회 자체의 시간 · 실패 | — |

- **나이는 SQL 에서** `TIMESTAMPDIFF(MICROSECOND, x, NOW(6))` 로 계산 (위험 A7 — 저장은 KST DATETIME, DB 세션 시간대 +09:00 하나만 믿는다). 앱 시계를 쓰지 않는다.
- **조회 실패 시 값은 NaN** (0 이 아니다 — 위험 A6), `metrics.db.query{result=error}` 증가. 이전 값을 들고 있지 않는다.
- api 2대가 같은 값을 낸다 → 대시보드 · 규칙은 `max()`.
- 조회 주기 10초는 설정(`banktransfer.metrics.db-poll-interval`), 끌 수 있다(`enabled`, 테스트용).

### 4-3. 게이트웨이 지표
| 지표 | 종류 · 태그 | 세는 곳 |
|---|---|---|
| `gateway.bank.request` | Timer `type=transfer\|inquiry\|name`, `outcome=ok\|rejected\|not_sent\|unknown` | `BankConnectionOwner.withClient` 한 곳에서 결과 분류 |
| `gateway.not_sent` | Counter `reason` (아래 enum) | 같은 곳 |
| `gateway.leader` | Gauge 0/1 | `BankConnectionOwner.isLeader` |
| `gateway.bank.connected` | Gauge 0/1 | 현재 클라이언트 `isConnected` (리더 아니면 0) |
| `gateway.leader.terms` | Counter | `startLeading` 성공 |
| `gateway.tran_no.remaining` | Gauge | 현재 구간 남은 번호 (ConfigMap 출처일 때. 메모리 출처는 NaN) |
| `gateway.tran_no.reserve` | Counter `result=ok\|conflict\|error` | `ConfigMapTranNoSource.reserve` |
| `bank.maintenance` | Gauge 0/1 | 점검 창 판정 |

**안 보냄 사유 (위험 A5)**: `BankNotSentException` 에 `reason: NotSentReason` 추가 (기본 `OTHER`). 값: `NOT_LEADER` · `STEPPING_DOWN` · `NO_TRAN_NO` · `DISCONNECTED` · `DUPLICATE_TRAN_NO` · `TRAN_NO_STORE` (깨진 기록 · 날짜 역전 · 소진 · 예약 충돌) · `GATEWAY_UNREACHABLE` (HTTP 클라이언트 쪽) · `OTHER`. 던지는 곳마다 사유를 넣는다. 문자열 매칭으로 가르지 않는다. 점검 보류는 이 예외가 아니라 `BankMaintenanceException` (코드 확인) — 사유 목록에 넣지 않고 결과 `maintenance` 로 따로 센다.

## 5. 대시보드 (Grafana, JSON 을 Git 에 · ConfigMap 으로 프로비저닝)
**① 출금 흐름 (업무)** — 위에서부터 읽으면 막힌 단계가 좁혀진다.
1. 한눈에: 접수/초 · 완료/초 · 실패/초, 미완료 건(상태별), **가장 오래된 미완료 나이**(상태별), 결과조회 소진 UNKNOWN 수, 점검 중 표시.
2. 단계별: 아웃박스 미발행 수 · 나이 → Kafka 컨슈머 lag → 은행 결과 분포 → 재시도 · DLT · 재발행 · 결과조회 · 포기.
3. 체감: 접수 → 확정 p50/p95/p99, HTTP p95 · 5xx 비율(경로별).

**② 게이트웨이 · 은행 · JVM**
1. 연결 주인: 리더 수 합(정상 = 1) · Pod 별 리더(계단), Pod 별 은행 연결(계단 — 은행 공백), 리더 임기 수.
2. 은행 호출: 지연 p50/p95(종류별), 결과 분포, **안 보냄 사유별**, 번호 구간 잔량 · 예약 결과.
3. JVM · 자원(api · 게이트웨이, Pod 별): 힙 · GC 멈춤 · 스레드, Hikari 활성/대기 · 대기 시간, Tomcat 바쁜 스레드, 상태 조회 시간(`metrics.db.query`).

공통: 패널 설명에 "답하는 질문", 범례에 Pod · 노드, 알림 임계값을 같은 그래프에 선으로, 기본 15분 · 새로고침 5초. 로그 · 추적 패널 없음 (P9b · c).

## 6. 수집 구성 (`infra/k8s`, namespace `monitoring`)
| 구성 | 설정 |
|---|---|
| Prometheus (Deployment 1) | `scrape_interval 5s` · `evaluation_interval 5s` · 보관 2일 · emptyDir. `kubernetes_sd_configs: role: pod, namespaces: [bank-transfer]`. 어노테이션 `prometheus.io/scrape: "true"` · `prometheus.io/port` · `prometheus.io/path` 인 Pod 만. relabel 로 `pod` · `node` · `app` 라벨. 규칙 파일 ConfigMap |
| Alertmanager (Deployment 1) | 받는 곳 alert-sink 하나 · `send_resolved: true` · `group_by [alertname]` · `group_wait 10s` · `group_interval 1m` · `repeat_interval 1h` · 억제 규칙 (§7) |
| alert-sink (Deployment 1) | busybox `nc -l` 반복 — 받은 요청을 로그로 찍고 200. 운영이면 Slack · 텔레그램 자리. 한 번에 하나만 받음 (위험 B7) |
| Grafana (Deployment 1) | 데이터소스 · 대시보드 ConfigMap 프로비저닝. 익명 Viewer, 관리자 비밀번호는 `up.sh` 가 임의 생성해 Secret (커밋 안 함) |

- 4개 모두 제어 노드(`nodeSelector: bank-transfer/role=infra` + control-plane toleration) — 워커를 죽이는 실험에서 관측 쪽은 살아 있다.
- 접속: `kubectl port-forward` (Grafana 3000 · Prometheus 9090 · Alertmanager 9093). kind 포트 매핑을 늘리지 않는다.
- 권한: Prometheus ServiceAccount 에 `bank-transfer` namespace Pod `get/list/watch` 만 (Role + RoleBinding, ClusterRole 아님).
- 이미지 4개(`prom/prometheus` · `prom/alertmanager` · `grafana/grafana` · `busybox`)는 `versions.env` 에 고정 — 2주 이상 지난 안정판을 계획 단계에서 확인.
- 앱 Pod 템플릿(api · bank-gateway)에 수집 어노테이션 추가 (롤링 1회 발생).
- NetworkPolicy (`bank-transfer` namespace): "`monitoring` namespace 의 `app=prometheus` → api:8080 · bank-gateway:8082" 추가. 기존 기본 거부 그대로. Prometheus → 게이트웨이 8082 허용은 송신 API 도 닿는다 — **받아들인 위험** (B5, ADR 에 기록).
- HTTPRoute 허용 목록 (D6). 나머지 경로는 Envoy 404.
- overlay: `local` · `ci` 모두 monitoring 포함. 실험 overlay 는 기존처럼 local 기반.

## 7. 알림 규칙
기다림(`for`)은 **시스템이 스스로 회복하는 시간보다 길게**. 점검 제외 = `unless on() (max_over_time(max(banktransfer_bank_maintenance)[30m:]) == 1)` (점검 중 + 끝난 뒤 30분).

| # | 알림 | 등급 | 조건 요지 | for | 근거 |
|---|---|---|---|---|---|
| 1 | GatewayNoLeader | critical | `sum(gateway_leader) == 0` | 30s | 정상 교대 3초 · 노드 장애 15초(Lease) |
| 2 | GatewayMultipleLeaders | critical | `sum(gateway_leader) > 1` | 15s | 있어선 안 됨 (교대 순간 겹침만 허용) |
| 3 | BankDisconnected | critical | `max(gateway_bank_connected) == 0` | 30s | 재접속 3초를 여러 번 넘김 — 은행 · 망 |
| 4 | TransferPipelineStalled | critical | RECEIVED 나이 > 30m **또는** SENDING 나이 > 2m, 점검 제외 | 1m | 재발행 3회(약 26분) 뒤엔 RECEIVED 가 남을 수 없다 · SENDING 은 30초 뒤 UNKNOWN 이어야 (A2) |
| 5 | TransferGaveUp | warning | `increase(transfer_gave_up[10m]) > 0` | 0 | 사용자가 다시 요청해야 하는 건 (A2) |
| 6 | TransferUnknownExhausted | warning | `max(transfer_unknown_exhausted) > 0` | 0 | 일마감 대사가 맞출 건 — 1시간마다 반복 (A3) |
| 7 | OutboxBacklog | warning | 미발행 나이 > 60s | 1m | 발행기 정지 · Kafka 장애 (발행 주기 0.5초) |
| 8 | TransferDeadLettered | warning | `increase(transfer_dlt[5m]) > 0` | 0 | DLT 한 건 = 사람 확인 한 건 |
| 9 | ApiHigh5xx | warning | 5xx 비율 > 5%, 점검 제외 | 2m | 노드 장애 직후 약 50초 5xx(P8-3a D2)는 넘지 않게 |
| 10 | ScrapeTargetDown | warning | 앱 Pod `up == 0` | 1m | 지표가 없으면 다른 규칙이 거짓 정상 |
| 11 | MetricsMissing | warning | 상태 Gauge 가 NaN · 없음 | 2m | DB 조회 실패 (A6) |

- 억제: `GatewayNoLeader` 가 울리는 동안 `BankDisconnected` 억제 (원인 하나 · 알림 하나).
- 정확한 PromQL 은 계획에서 `promtool test rules` 로 고정한다 (아래 §9).
- 기존 P5 `AlertNotifier`(일마감 대사 결과)는 그대로 — 업무 결과 알림 vs 시스템 상태 알림 (D9).

## 8. 실험 (`load/k8s/run.sh` 에 `PHASE=p9a`, 출금 20건/s)
| # | 동작 | 볼 것 |
|---|---|---|
| M1 | 리더 Pod 삭제 · 리더 노드 SIGKILL | 지표의 은행 공백(연결 Gauge) vs 모의 은행 로그 공백 (≤ 5초), 리더 합 0 구간 길이, 죽은 노드 Pod 의 시계열이 끊기는가 (B4) |
| M2 | 모의 은행 0대 2분 → 복구 | BankDisconnected 도착 시간 vs 계산값, 해제 알림 |
| M3 | 게이트웨이 0대 1분 → 복구 | GatewayNoLeader 도착, **BankDisconnected 억제** |
| M4 | 은행 10분 끊기 (약 1만 2천 건) | 상태 조회 시간 (**> 50ms 면 V7 `(status, updated_at)` 인덱스** — B1), 전체 시계열 수 (B2), 카운터 vs DB 차이 (B3) |
| M5 | 점검 창을 "지금" 으로 바꾼 overlay | 점검 중 · 뒤 30분 거짓 알림 0 (A1) |

결과는 `docs/perf/p9a-metrics.md` + 원자료 `docs/perf/raw/p9a/` (Prometheus 질의 결과 · alert-sink 로그 · 모의 은행 로그).

## 9. 테스트
- 단위: 결과 분류 · 사유 enum · `TransferMetrics` (커밋 뒤에만 셈 — 롤백 시 0) · 점검 Gauge — `SimpleMeterRegistry`.
- 통합 (Testcontainers MySQL): 상태 Gauge 쿼리 — 상태별 건수 · SQL 나이 · 결과조회 소진 · DB 중단 시 NaN.
- `/actuator/prometheus` 에 업무 지표 이름이 나온다 (api · 게이트웨이 각 1).
- 알림 규칙: `promtool test rules` 로 규칙별 발화 · 미발화 · 점검 제외 · 억제는 Alertmanager 설정 검사(`amtool check-config`). CI 에서 promtool · amtool 컨테이너로 실행.

## 10. CI · 스모크 [10]
`k8s-smoke` 의 `smoke.sh` 에 [10]:
1. monitoring 4개 Ready.
2. 수집 대상 api ×2 · bank-gateway ×2 `up == 1`.
3. `sum(banktransfer_gateway_leader) == 1`, `max(banktransfer_gateway_bank_connected) == 1`.
4. 스모크 출금 뒤 `transfer_accepted` · `transfer_completed_count` 증가.
5. 입구 `$BASE/actuator/prometheus` → 404.
6. 규칙 적재 수 = 11, 울리는 알림 0.
7. Alertmanager API 로 테스트 알림 → alert-sink 로그에 30초 안 도착.

`build` 잡에 규칙 테스트(§9) 추가. 시간 기록 — `k8s-smoke` 20분 제한 (B6).

## 11. 위험
| # | 위험 | 대응 |
|---|---|---|
| B1 | RECEIVED 나이 조회가 밀림이 클수록 무겁다 (`updated_at` 인덱스 밖) | M4 에서 측정, 50ms 초과면 V7 인덱스 |
| B2 | 히스토그램 시계열 증가 | 버킷 범위 제한, M4 에서 시계열 수 기록 |
| B3 | 카운터 근사치 | 문서 명시, M4 에서 DB 대비 차이 기록 |
| B4 | 죽은 노드 Pod 의 마지막 값이 남는가 | Prometheus staleness (추정) — M1 확인 |
| B5 | Prometheus → 게이트웨이 송신 API 도달 | Prometheus Pod 하나만 허용, ADR 에 받아들인 위험 · 관리 포트 분리는 다음 |
| B6 | 제어 노드에 감시 4개 추가 — CI 시간 · 자원 | 시간 기록, 20분 제한 |
| B7 | alert-sink 동시 수신 불가 | Alertmanager 재시도, 실험용 대역임을 명시 |
| B8 | 수집 어노테이션 추가로 앱 롤링 1회 | 정상 롤링 (P8-3c 분산 규칙 그대로) |

## 12. 범위 밖 ("실제 운영이라면")
관리 포트 분리(`management.server.port`) · Prometheus Operator · 원격 저장(Thanos 등) · 영구 볼륨 · Envoy · MySQL · Kafka 브로커 exporter · Watchdog(생존 신호 — 받을 외부 시스템 필요) · on-call · SLO 오류 예산 알림 · Compose 수집 · 사용자/계좌별 지표 · 추적(P9b) · 로그 모으기(P9c).

## 13. 변경 목록 (요지)
- 앱: `apps/api` (registry · `TransferMetrics` · 상태 Gauge · 점검 Gauge · 설정), `apps/bank-gateway` (registry · 게이트웨이 지표), `modules/bank-gateway` (`NotSentReason`), `modules/bank-gateway-http` (사유), `modules/transfer` (`TransferProcessor` · `TransferReconciler` 의 결과 — 세는 지점은 계획에서 확정).
- 매니페스트: `infra/k8s/monitoring/` (Prometheus · Alertmanager · alert-sink · Grafana · RBAC · ConfigMap: 설정 · 규칙 · 대시보드 2장), `base/gateway.yaml`(허용 목록), `base/networkpolicy.yaml`, `base/api.yaml` · `base/bank-gateway.yaml`(어노테이션), overlay, `versions.env`, `up.sh`(Grafana 비밀번호 · port-forward 안내), `smoke.sh` [10].
- CI: 규칙 테스트, [10]. 실험: `load/k8s/run.sh` `PHASE=p9a`.
- 문서: ADR-0020, OPERATION §17(대시보드 읽는 순서 · 알림별 할 일), ARCHITECTURE, README, HANDOVER, CHANGELOG, 측정 결과, **구성도 두 장 갱신** (runtime-k8s 에 monitoring, app-architecture 에 지표 지점).

## 14. 작업 순서
1. 사유 enum · 게이트웨이 지표 (테스트 먼저)
2. api 사건 지표 · `TransferMetrics`
3. api 상태 Gauge (Testcontainers)
4. 노출 설정 · 기본 지표 확인
5. 매니페스트 monitoring · HTTPRoute 허용 목록 · NetworkPolicy
6. 규칙 · Alertmanager · 규칙 테스트
7. 대시보드 2장
8. 스모크 [10] · CI
9. 실험 M1 ~ M5 · 결과 문서
10. ADR · OPERATION · 구성도 · 나머지 문서

## 15. 계획 단계에서 고친 점 (2026-11-17, 계획 `docs/superpowers/plans/2026-11-17-p9a-metrics.md` 끝 "계획에서 정한 것")
- **점검 보류의 예외** (§4-1 · §4-3 정정): 컨슈머 · 정리 작업의 송신 경로는 `SendGate` 가 `BankNotSentException("bank maintenance window")` 을 낸다 (코드 확인 — `BankMaintenanceException` 은 접수 · 1원인증 API 쪽). → 사유 enum 에 `MAINTENANCE` 를 둔다. api 의 `transfer.bank.result{outcome=maintenance}` 는 "안 보냄이고 지금이 점검 창" 으로 판정 (HTTP 로는 사유가 넘어오지 않는다).
- **`transfer.dlt` 에 `kind` 태그** (`bank_not_connected | invalid_key | other`). 은행 연결 없음 DLT 는 재발행이 이어받으므로 `TransferDeadLettered` 에서 뺀다 (안 빼면 은행 장애 실험마다 거짓 알림).
- **도메인 포트 `TransferObserver`** (modules/transfer, 의존 없음) — 저장 메서드가 성공을 돌려준 뒤에만 부른다. 일마감 대사의 확정은 청크 트랜잭션 안이라 세지 않는다. 은행 결과 자체는 DB 기록 성공과 무관하게 센다.
- **알림 PromQL**: `GatewayNoLeader` · `BankDisconnected` 에 `or vector(0)` (게이트웨이 0대면 시계열이 없어 안 울린다), `ScrapeTargetDown` 은 15분 안에 한 번이라도 읽힌 Pod 만, `ApiHigh5xx` 는 `/actuator` 경로 제외.
- **업무 Timer 는 SLO 버킷 고정**, 모든 태그 조합 Counter 를 기동 때 0 으로 등록 (`increase()` 첫 증가 손실 방지).
- **`tran_no.remaining` = 현재 구간 + 미리 예약 구간** (실제로 쓸 수 있는 번호).
- **스모크 [10]-4 이름**: Timer 라 `banktransfer_transfer_completed_seconds_count`.
- **M5** 는 정적 overlay 대신 `app-config` ConfigMap 을 고쳐 재시작하고 끝나면 되돌린다 ("지금" 을 정적 파일에 적을 수 없다).
- **Prometheus 자기 수집** 추가 (시계열 수 B2 측정용). 이미지: Prometheus v3.5.0 · Alertmanager v0.28.1 · Grafana 12.0.0 · busybox 1.37.0 (구현 때 `docker manifest inspect` 로 존재 확인).

## 16. 실험 뒤 정정 (2026-11-17)
실험(M1 ~ M5, [측정 결과](../../perf/p9a-metrics.md))에서 알게 된 것으로 위 내용을 바로잡는다. §6 표의 alert-sink 줄은 설계 당시 그대로 두었다.
- **alert-sink 는 `busybox nc -l` 이 아니다** (§6 표 · §11 B7 정정). nc 방식은 응답(200)을 먼저 쓰고 본문을 읽기 전에 끝나, 1초 넘게 늦게 온 알림의 본문을 조용히 잃었다 (첫 회차 알림의 절반쯤). B7 은 "동시에 오면 거부되고 재시도" 가 아니라 **성공 응답 뒤 유실이라 재시도도 없었다** — 설계보다 나빴다. 지금은 busybox `httpd` + CGI 스크립트(ConfigMap — `infra/k8s/monitoring/alert-sink.yaml` · `alert-sink/sink.sh`), 웹훅 주소 `/cgi-bin/sink` (ADR-0020).
- **성공 기준 2 (은행 공백 차이 ≤ 5초)는 Prometheus 원 표본으로 판정한다**: 원 표본 2.4 ~ 4.5초(통과), 5초 `query_range` 격자로 읽으면 5.0 ~ 8.2초. 기준값은 바꾸지 않고 두 값을 다 남긴다 (`bank_gap_diff_s` · `bank_gap_diff_raw_s`). 대시보드는 복구를 최대 약 8초 늦게 보일 수 있다 (OPERATION §17-2).
- **실험의 카운터 차이는 `increase()` 로 잰다**: Pod 가 재시작하면 카운터가 0 으로 돌아가 단순 차이가 음수가 된다 (M1b −420). §4-1 "카운터는 근사치" 그대로이고, `increase()` 도 창 끝 외삽으로 약 1% 어긋난다.
