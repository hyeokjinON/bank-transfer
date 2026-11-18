# ADR-0020: 관측 — 업무 지표 · 상태 Gauge · Prometheus · Alertmanager · Grafana (kind 에만)

- 상태: 채택 (2026-11-18, [#35](https://github.com/hyeokjinON/bank-transfer/pull/35) 머지 · 정리 PR #36 — 제안 2026-11-17)
- 관련: [ADR-0017](0017-kubernetes-kind.md) (kind · 제어 노드 배치) · [ADR-0018](0018-gateway-ha-leader-election.md) (리더 · 은행 연결 — 지표로 보는 대상) · [ADR-0019](0019-gateway-required-spread.md) · [스펙](../superpowers/specs/2026-11-17-p9a-metrics-design.md) (끝의 "§16 실험 뒤 정정" 포함) · [측정 결과](../perf/p9a-metrics.md) · [OPERATION §17](../../OPERATION.md)

## 맥락
- P8 실험의 숫자(은행 공백 · 리더 교대 · Pending 시간)는 모두 **로그와 kubectl 표본을 손으로 맞춰** 냈다. 앱에 업무 지표가 0 개였고 Prometheus 형식 노출도 없었다.
- "돈이 제대로 흐르나" 와 "어느 부품이 문제인가" 를 지표로 보이게 하고, 사람이 무언가 해야 할 때만 알림이 가게 한다.
- 입구(Envoy HTTPRoute)는 모든 경로를 api 로 넘겨, 지표 경로(`/actuator/prometheus`)를 켜면 PC 입구로 그대로 노출될 상태였다.

## 결정
스펙 §2 의 D1 ~ D9 에 구현 계획이 정한 것을 더했다.
1. **P9 를 a(지표 · 대시보드 · 알림) / b(추적) / c(로그)로 나눈다** (D1). 이 PR 은 a 만.
2. **kind 에만 붙인다** (D2) — Compose 제외. 게이트웨이 이중화 · 노드 장애를 그래프로 볼 수 있는 곳이다.
3. **업무 지표 = 사건 Counter · Timer + 상태는 DB 를 10초마다 읽는 Gauge** (D3). "끝나지 않고 쌓이는 건" 은 카운터로 못 본다 (Pod 재시작 때 카운터 0). 라벨에 사용자 id · 계좌 · 참조번호 · 금액을 넣지 않는다 (D7).
4. **Prometheus 는 Operator 없이 Deployment + 설정 파일**, `kubernetes_sd`(pod, `bank-transfer` namespace 만), Service 를 거치지 않고 **Pod 를 직접** 5초마다 읽는다 (D4). 권한은 Role + RoleBinding(Pod get · list · watch 만, ClusterRole 아님).
5. **알림 = 규칙 11개 + Alertmanager → alert-sink**(클러스터 안 웹훅 수신기, 받은 알림 = 로그) (D5). 외부 메신저 · 토큰 없이 도착 · 묶기 · 억제 · 해제를 잰다. `GatewayNoLeader` 가 울리는 동안 `BankDisconnected` 억제.
6. **입구 HTTPRoute 를 허용 목록으로** (`/api/v1` · `/admin` · `/actuator/health` · `/actuator/info`, 나머지 Envoy 404) (D6). NetworkPolicy 는 `monitoring` namespace 의 Prometheus 만 api:8080 · 게이트웨이:8082 로 허용 (`metrics-from-prometheus-*`).
7. **점검 시간은 앱 설정 한 곳**(`BANK_MAINTENANCE_*`)에서 `banktransfer.bank.maintenance` Gauge 로 내고, 규칙이 점검 중 + 끝난 뒤 30분 `TransferPipelineStalled` · `ApiHigh5xx` 를 뺀다 (D8).
8. **Alertmanager → api → 기존 `AlertNotifier`(텔레그램) 경로는 고르지 않는다** (D9). 감시 대상(api)이 죽으면 알림 경로도 죽는다. 기존 P5 알림은 업무 결과용으로 그대로.
9. **관측 4개(Prometheus · Alertmanager · alert-sink · Grafana)는 제어 노드**에 둔다 — 워커를 죽이는 실험에서 관측 쪽은 살아 있다.

구현 계획이 스펙의 빈 곳 · 코드와 다른 곳에서 정한 것:
- **점검 보류 = `SendGate`**: 컨슈머 · 정리 작업 경로는 `SendGate` 가 `BankNotSentException("bank maintenance window")` 를 낸다 (코드 확인 — `BankMaintenanceException` 은 접수 · 1원인증 API 쪽). 그래서 사유 enum 에 `NotSentReason.MAINTENANCE` 를 더하고, api 의 `transfer.bank.result{outcome=maintenance}` 는 "안 보냄이고 지금이 점검 창" 으로 판정한다 (HTTP 로는 사유가 넘어오지 않는다).
- **`transfer.dlt` 에 `kind` 태그** (`bank_not_connected | invalid_key | other`). 은행 연결 없음 DLT 는 재발행이 이어받으므로 `TransferDeadLettered` 에서 뺀다 (안 빼면 은행 장애 실험마다 거짓 알림). `DltKind` 는 public.
- **관찰 포트 `TransferObserver`** (modules/transfer, 의존 없음) — 저장 메서드가 성공을 돌려준 **뒤에만** 부른다 (롤백 · 낙관 락 재시도에서 두 번 세지 않게). 일마감 대사의 확정은 청크 트랜잭션 안이라 세지 않는다. 은행 결과 자체는 DB 기록 성공과 무관하게 센다.
- **상태 Gauge 의 나이는 SQL `NOW(6)` 로** 계산하고(앱 시계를 쓰지 않는다), **조회가 실패하면 값은 NaN**(0 이 아니다)이다. 대시보드 · 규칙은 api 2대의 같은 값을 `max()` 로.
- **알림 PromQL**: `GatewayNoLeader` · `BankDisconnected` 에 `or vector(0)` (게이트웨이가 0대면 시계열이 없어 안 울린다), `ScrapeTargetDown` 은 "15분 안에 한 번이라도 읽힌 Pod" 만 (롤링 · 기동 중 거짓 알림 방지), `MetricsMissing` = NaN 또는 없음, `ApiHigh5xx` 는 `/actuator` 경로 제외.
- 업무 Timer 는 SLO 버킷 고정, 모든 태그 조합 Counter 를 기동 때 0 으로 등록 (`increase()` 첫 증가 손실 방지), 스케줄 풀 4 → 5.
- **관측 접속은 `kubectl port-forward`** (Grafana 3000 · Prometheus 9090 · Alertmanager 9093) — kind 포트 매핑을 늘리지 않는다. Grafana 관리자 비밀번호는 `up.sh` 가 처음 한 번만 임의 생성해 Secret `grafana-admin` 에 둔다 (커밋 안 함).

## 결과
측정 (2026-11-17, 한 PC · kind 3노드 — 운영 수치가 아니다, [측정 결과](../perf/p9a-metrics.md)). 판정(돈 정확성)은 **8회 전부 통과**.
- **M1 은행 공백, 지표 vs 모의 은행 로그** (기준 ≤ 5초): 지표를 **Prometheus 원 표본**으로 읽으면 차이 2.4 ~ 4.5초 (M1b 2.9 · M2 3.0 / 4.5 · M3 2.4 · M4 3.6) — 통과. 대시보드처럼 **5초 격자**로 읽으면 5.0 ~ 8.2초 (6.5 · 6.7 · 5.0 · 8.2) — 복구가 최대 약 8초 늦게 보인다. 두 값을 다 남겼다 (`bank_gap_diff_s` · `bank_gap_diff_raw_s`). 5초보다 짧은 공백(M1a 1.9 · 2.0초)은 지표에 안 보이고 `for 30s` 알림도 울리지 않는다. 리더 노드 SIGKILL(M1b): 로그 공백 17.5초, 지표 공백 15.0초, 지표가 공백 시작을 +9초에 본다.
- **M2 · M3 알림 도착** (계산값 50초 = 수집 5 + 평가 5 + `for` 30 + 묶기 10): BankDisconnected **49초**(M2 `bank-2`) · **50초**(M4), GatewayNoLeader **47초**(M3) — ±10초 안. 해제는 은행 복구(로그 142.5초) 뒤 169초에 도착. **억제**: M3 에서 Prometheus 는 BankDisconnected 도 울렸지만 Alertmanager 가 `suppressed` 로 표시해 alert-sink 로 보내지 않았다 ("원인 하나 · 알림 하나" 는 받는 쪽 기준).
- **M4 은행 10분 끊김** (약 1만 2천 건 밀림): 상태 조회 최대 **43.8ms** (평소 4 ~ 11ms) — 기준 50ms 이하라 V7 인덱스 불필요, 다만 4배 늘어 여유는 크지 않다. 시계열 수 최대 55,395 (실험이 쌓일수록 28,761 → 55,395, 대부분 Pod 가 새로 뜰 때마다 `pod` 라벨이 다른 시계열이 생기는 몫 — 추정). **카운터 vs DB**: Pod 재시작이 없으면 접수 카운터 = DB (12801 = 12801). 노드 kill 로 api 가 재시작되면 단순 차이는 −420 이라 실험은 `increase()` 로 쟀다 (≈1030 vs DB 1029), `increase()` 도 창 끝 외삽으로 −1.1%(3561 vs 3601) 어긋난다.
- **M5 점검 창**: 점검 중 · 뒤의 거짓 알림 **0건** (Prometheus 에서 울린 알림 자체가 없음, Alertmanager 표본 0).
- 죽은 Pod 의 시계열은 동작 뒤 0 ~ 4초에 끝났다 (Prometheus staleness — 옛 값이 남지 않음, B4).
- 스모크 [10] 은 로컬 통과 (입구 `/actuator/prometheus` 404). `k8s-smoke` 시간은 PR CI 에서 기록.

## 실험 중에 찾은 것 — alert-sink 가 알림을 조용히 잃었다 (위험 B7, 설계보다 나빴다)
- 설계 단계의 B7 은 "동시에 둘이 오면 하나가 거부되고 Alertmanager 가 재시도" 였다. 실제는 **거부가 아니라 성공 응답 뒤 유실**이라 재시도가 없었다. 처음 alert-sink 는 `{ printf 응답; sleep 1; } | nc -l` 반복이었는데, `sleep 1` 은 연결이 올 때가 아니라 루프가 시작될 때 흐른다. 1초 넘게 지나서 온 알림은 nc 가 입력이 끝난 채 연결을 받아 **응답(200)만 쓰고 본문을 읽기 전에 끝났다** → Alertmanager 는 다시 보내지 않고 로그에는 빈 줄만 남았다. 첫 회차(`bank-1` 등) 받은 알림의 절반쯤이 이렇게 사라졌다. 스모크 [10] 의 시험 알림도 5번 실패했다.
- **고침**: busybox `httpd` + CGI 스크립트(ConfigMap, `infra/k8s/monitoring/alert-sink.yaml` · `alert-sink/sink.sh`). 연결마다 프로세스가 따로 떠서 본문을 `CONTENT_LENGTH` 만큼 읽고 한 번에 로그로 쓴 뒤 200. 웹훅 주소는 `/cgi-bin/sink`. 확인: 시험 알림 5개를 2 ~ 3초 간격으로 보내 5/5 도착, 고친 뒤 M2 재실행(`bank-2`)에서 울림 · 해제 둘 다 받았다.
- 고치기 전 회차의 알림 도착은 Prometheus · Alertmanager 쪽 증거로 판정했다 (측정 결과 문서 "B7").
- 교훈: 대역(실험용 가짜)이라도 "받았다고 답하고 안 받는" 수신기가 가장 나쁘다. 실제 Slack · 텔레그램 자리에서도 "전달 성공 응답 = 저장됨" 을 확인할 것.

## 위험 · 다음
- **B5 Prometheus → 게이트웨이 송신 API 도달 (받아들인 위험)**: NetworkPolicy 가 Prometheus Pod 하나만 허용하지만, 지표 포트 8082 는 은행 송신 API 와 같은 포트라 Prometheus 가 뚫리면 송신 API 에 닿는다. 관리 포트 분리(`management.server.port`)가 근본 해결 — 범위가 커서 다음.
  - Prometheus 는 api:8080 업무 포트(`X-User-Id` 헤더를 믿는 API · `/admin`)에도 닿는다 — 입구가 이미 같은 헤더를 그대로 넘기므로 새 권한은 아니다.
- **RECEIVED 임계값 정정 (최종 리뷰 I1)**: `TransferPipelineStalled` 의 RECEIVED 나이는 `MIN(updated_at)` = **마지막 상태 변화 뒤 나이**다 (재발행도 갱신). 처음 30분은 접수 뒤 나이를 전제로 한 값이었고, 실측 정상 최대는 514초 (M4)라 **15분(900초)** 으로 낮췄다. 의미는 "RECEIVED 가 15분 넘게 아무 변화 없음 = 재발행도 멈춤 — 컨슈머 · 정리 작업 둘 다 의심". 컨슈머만 멈추면 재발행이 소진돼 TransferGaveUp 이 먼저 울린다.
- **카운터는 근사치.** Kafka 최소 한 번 전달 · Pod 재시작으로 건수가 정확하지 않다. 돈의 정답은 DB (`docs/database.md` 운영 쿼리), 지표는 추세 · 장애 신호용.
- **지표가 은행 복구를 늦게 보여 줄 수 있다.** 5초 수집 + 5초 격자로 대시보드에는 최대 약 8초 늦게 보인다. 사람이 판단할 시간 척도(알림 `for` 30초)에는 영향이 없다.
- **노드 장애는 지표가 가장 늦게 본다**: 지표 공백 시작 +9초, ScrapeTargetDown +64 · +69초 (`for 1m`). 장애 알림이 은행 연결 알림(30초)보다 늦다.
- 한계: kind 한 호스트 · 모의 은행 · 수집 해상도 5초 · alert-sink 는 대역 (실제 받는 곳의 재시도 · 속도 제한은 재지 않음).
- 운영이라면 (스펙 §12): 관리 포트 분리 · Prometheus Operator(또는 관리형) · 원격 저장(Thanos 등) · 영구 볼륨 · Envoy · MySQL · Kafka 브로커 exporter · Watchdog(생존 신호) · on-call · SLO 오류 예산 알림 · 사용자/계좌별 지표.
- 다음 후보: 컨슈머 정지를 직접 보는 lag 알림 (`max(kafka_consumer_fetch_manager_records_lag_max) > N for 5m`) — 지금 lag 는 대시보드에만 있다.
- 다음 후보: `/admin` 이 인증 없이 PC 입구에 열려 있음 (P9a 이전부터 — 허용 목록은 이 동작을 유지했다).
- 다음: **P9b 분산 추적**(OpenTelemetry — Observation API 는 필요한 곳만), **P9c 로그 모으기**.

## 검토한 대안
- **Prometheus Operator · kube-prometheus-stack**: CRD · 컨트롤러가 늘고 읽기 어렵다. Deployment + 설정 파일 하나가 이 규모에 맞다 (운영이면 Operator · 관리형).
- **Alertmanager → api → 텔레그램(D9)**: 감시 대상이 죽으면 알림 경로도 죽는다.
- **카운터만으로 "쌓임" 보기(D3)**: Pod 재시작에 카운터가 0 이 되고 "끝나지 않은 건" 은 사건이 아니라 상태다. DB 를 읽는 Gauge 가 맞다 (대신 조회 비용 — M4 에서 43.8ms 로 확인).
- **관리 포트 분리**: 맞지만 api · 게이트웨이 · 프로브 · 서비스 전부 손대야 해 이 PR 의 크기를 넘는다.
- **alert-sink 를 `nc` 한 줄로 유지**: 본문을 잃는 것이 측정됨 → 기각.
