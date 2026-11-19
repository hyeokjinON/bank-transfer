# ADR-0022: 로그 모으기 — Grafana Alloy · Loki · 수집 직전 가리기 (kind 에만)

- 상태: 채택 (2026-11-20, [#39](https://github.com/hyeokjinON/bank-transfer/pull/39) 머지 · 정리 PR #40 — 제안 2026-11-19)
- 관련: [ADR-0017](0017-kubernetes-kind.md) (kind) · [ADR-0020](0020-observability-metrics-alerts.md) (관측 · 알림) · [ADR-0021](0021-distributed-tracing.md) (추적) · [스펙](../superpowers/specs/2026-11-19-p9c-logs-design.md) (§14 계획 · 실험 단계에서 고친 점 포함) · [측정 결과](../perf/p9c-logs.md) · [OPERATION §19](../../OPERATION.md#19-로그-모으기--loki--alloy-p9c)

## 맥락
- 로그는 Pod 표준 출력에만 있다. 출금 한 건의 로그를 보려면 Pod 마다 `kubectl logs | grep` 을 돌려야 하고, Pod 가 바뀌면 사라진다.
- P9b 로 줄에 traceId 가 붙었지만 **로그와 trace 를 오가는 화면이 없다**. 모은 로그에는 계좌번호 · 금액 같은 민감값이 섞일 수 있어, 모으는 것 자체가 위험을 만든다.

## 결정
스펙 §2 의 F1 ~ F10 에, 구현 계획과 실행 중에 정한 것(스펙 §14 · 계획 "계획에서 정한 것" 1 ~ 25)을 더했다.
1. **범위 · 수집** (F1 · F2): `bank-transfer` namespace 전체 Pod 의 로그와 로그 ↔ trace 잇기. monitoring · kube-system · Envoy 는 모으지 않는다. 수집은 Grafana Alloy **DaemonSet**(노드마다 1개, 제어 노드 포함 3개)이 노드의 `/var/log/pods` 를 읽는다. 권한은 Role(그 namespace 의 Pod 읽기 — ClusterRole 아님), 컨테이너는 root(노드 로그 파일이 `root:root 0640`) + capability 전부 제외.
2. **저장** (F3): Loki 3.7.9 단일 바이너리 · 제어 노드 고정 · emptyDir · 보관 2일. 접속은 port-forward 뿐(3100). 라벨은 `namespace` · `app` · `container` 셋뿐이고 `pod` · `node` · `level` · `trace_id` 는 구조화 메타데이터(F5) — Loki 3.7 이 스스로 붙이는 `service_name` 라벨 · `detected_level` 메타데이터는 끈다.
3. **형식은 kind 에서만 JSON(ECS)** (F4): api · 게이트웨이 `LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs`(app-config 한 곳, 게이트웨이 env 는 `optional`). Compose · IntelliJ 는 콘솔 텍스트 그대로. 필드 지정은 하지 않는다(Boot 4.1.1 기본: `log.level` · `log.logger` · `error.*`, MDC `traceId` · `spanId`). 끄면 Alloy 가 텍스트 정규식으로 이어서 꺼낸다.
4. **처리 모듈 하나를 수집과 CI 가 같이 쓴다**: `pipeline.alloy`(꺼내기 level · trace_id → 메타데이터 → 가리기)를 `config.alloy`(수집)와 `check.alloy`(CI 표본)가 `import.file` 한다. `check.sh` [6/6] 이 표본 로그에 Alloy 를 10초 실제로 돌려 라벨 · 금지 패턴 · trace_id · level 을 단언한다.
5. **민감 정보는 이중 방어** (F6): ① 앱이 남기지 않는다 — 지갑 로그 · `Transfer.toString` · 예외 메시지(`StoreConflict`)에서 사용자 id · 멱등키 · 금액을 빼고, 앱 알림(`AlertNotifier`)의 로그 사본(`ALERT` 로거)은 금액 · 잔고(`amount=` · `balance=` · `ledgerSum=` · 대사 항목 `우리=` · `은행=` 값의 금액)를 가리고(텔레그램 본문은 그대로 — 운영자 조치용), Redisson 시간 초과 예외는 클래스 이름만(메시지에 락 이름 `lock:wallet:<사용자 id>` 가 실린다), Hibernate 제약 위반 WARN(`org.hibernate.orm.jdbc.error`)은 ERROR 로. ② Alloy 가 보내기 직전 모든 줄을 가린다 — 계좌(10 ~ 16자리, 영문 · 숫자에 붙지 않은 것, JSON 이스케이프 `\n` `\t` `\uXXXX` 뒤도 경계 → 앞부분을 같은 길이의 `*`) · 1원인증 코드 `BT****` · 금액 `***원`. Go 정규식에 앞뒤 보기가 없어 붙어 있는 두 계좌 중 하나가 빠지므로 **같은 규칙을 두 번**. ③ 금지 패턴 0건을 Loki 질의로 스모크 · 실험이 증명한다. 모의 은행 콘솔의 통장 줄은 원본 그대로(개발 중 1원인증 코드를 읽는 통로), Loki 사본만 가린다.
6. **잇기** (F7 · F8): Loki 파생 필드(`trace_id` → Tempo) + Tempo `tracesToLogsV2`(traceId 로 Loki 질의, 앞뒤 1분). 대시보드 ① 출금 흐름에 4행 "로그"(서비스 · 레벨별 양, api · 게이트웨이 ERROR · WARN 패널).
7. **게이트웨이 은행 호출 한 줄 추가** (스펙에 없던 것): 정상 출금에서 게이트웨이는 trace 안에서 로그를 한 줄도 쓰지 않아 "traceId 로 게이트웨이 로그" 가 안 된다. `withClient` 완료 때 `bank call type= outcome= reason= tranNo= reference= ms=` INFO 한 줄을 `bank.request` 구간 문맥에서 남긴다(계좌 · 금액 · 예금주 · 적요 없음). 모의 은행 줄과는 `bank.tran_no` 가 아니라 거래번호(`transfer.reference`)로 잇는다.
8. **로그 알림은 `ErrorLogSurge` 하나** (F9): Loki ruler → 기존 Alertmanager, `source=logs`, 5분 ERROR > N(**N = 10**), `for: 2m`. `BankDisconnected` · `GatewayNoLeader` 동안 억제(Alertmanager 억제 규칙). **Spring Kafka DLT 발행 줄(`DeadLetterPublishingRecovererFactory`)은 식에서 뺀다** — P9b 원자료에서 은행 끊김 한 번에 그 줄만 742 ~ 771줄이고 재접속 뒤에도 5분 창에 남아 억제 밖에서 울린다. DLT 는 지표 알림 `TransferDeadLettered` 가 맡는다. N 은 정상 상태 5분 ERROR 최대 0(7회차)이라 max(10, 0 × 3) = 10 그대로(실험으로 확정, rules.yml · p9c.sh 불변).
9. **수집이 실패해도 앱은 그대로** (F10): 앱은 표준 출력에만 쓴다.
10. **실험 장애는 1원인증 저장 실패**: MySQL 트리거(`p9c_fault_verification`)로 1원인증만 5초마다 실패시킨다 — 출금 20건/s 대비 약 1% 라 `ApiHigh5xx`(5%)가 울리지 않는 "지표로 못 잡는 오류"이고 출금 돈 흐름은 그대로다.

## 결과
측정 (2026-11-19, 한 PC · kind 3노드 — 운영 수치가 아니다, [측정 결과](../perf/p9c-logs.md)). 숫자는 그 문서와 `docs/perf/raw/p9c/` 에서 그대로 옮겼다. 판정(돈 정확성)은 **9회 전부 통과**, 구조화 로그를 끈 회차 뒤 스모크 [12/12] 도 통과.
- **L1 수집 지연**: 표지 줄이 표준 출력에서 Loki 에 보이기까지 p50 1.0 ~ 2.3초, p95 **2.4 ~ 2.6초**, 빠진 표본 0 (해상도 약 1초).
- **L2 JSON 비용**: 접수 p95 켬 평균 27.19ms / 끔 26.86ms → **+1.2%** (기준 ≤ +10%), 완료 /s 19.68 / 19.69 → **−0.05%** (기준 ≥ −5%). 바이트는 약 두 배 — api 11.1 대 5.7 KB/s (×1.95), 게이트웨이 9.6 대 5.1 KB/s (×1.88). Loki CPU 0.015 ~ 0.027 코어 · RSS 192 ~ 242MB, Alloy CPU 0.026 ~ 0.066 코어 · RSS 231 ~ 239MB.
- **L3 · L5 잃는 줄**: api Pod 강제 종료 **940/940**, Alloy 재시작 **506/506**, 중복 0 (기준 0).
- **L4 Loki 0대 60초**: 회차 중 기록은 api 가 쓴 813줄 중 270줄만 들어와 543줄 미도착이었지만, 약 19분 뒤 다시 세니 Loki 에 **830줄**(≥ 813) — **최종 잃음 0**, 도착 지연만 있었다. Alloy 가 보내기 실패를 다시 시도해 나중에 넣었고 도구의 기다림이 더 짧았다(원자료의 543 은 도구 한계, 유실 아님). 다시 시도하는 방식(지수 백오프)은 **추정 — 코드로 확인하지 않음. `loki.write` 가 막히면 읽기가 멈추는 역압일 수도 있다**. 꺼지기 전 데이터는 emptyDir 이라 사라졌다.
- **L6 ErrorLogSurge — 부분**: 억제 ✅(`inhibit_ok` true, 은행 끊김 중 새 ErrorLogSurge 0), 지표로 못 잡는 오류 ✅(`api5xx_fired` false, 500 응답 58건 = api ERROR 58줄), **도착은 계산 구간 [186, 256]초보다 18초 늦은 273.9초**(firing 에서 받는 곳까지 60초 — 원인 후보는 묶음 `group_interval` 대기 또는 ruler 의 다음 평가, 둘 다 **추정**, 가리지 않음). 통과로 올리지 않는다. 로그 알림은 넘은 뒤 약 3 ~ 4.5분 걸린다(지표 알림 50초보다 느림) — 급한 신호는 지표로, 로그 알림은 "놓치지 않기" 용.
- **DLT 제외의 실측 근거**: `bank-1` 에서 DLT 를 포함한 5분 ERROR 가 **773줄**(03:48 ~ 03:52) — 빼지 않았다면 은행 복구 뒤 억제 밖에서 울렸을 것. 제외한 `err5m_max` 는 40(앞 회차 errfault 의 꼬리).
- **L7 가리기**: 모든 회차 금지 패턴 **0** (실험은 계좌 10 ~ 14자리 식 · 최종 리뷰 수정 전 이미지 `dev-6cafa40` 으로 쟀다 — 16자리로 넓힌 식은 CI 표본(16자리 줄) · 스모크 [12] 로 확인). 가린 줄은 실제로 있었다 — 계좌 703 ~ 11468줄, 금액 634 ~ 7081줄, 1원인증 코드 58줄(errfault 주입 58건).
- **실험 · 구현 중에 찾은 것**: Loki 3.7 의 자동 라벨(`service_name`)과 `detected_level` 끄기, 계좌 가리기 경계에 JSON 이스케이프 추가(ECS 예외 메시지 · `stack_trace` 의 `\n` 뒤 계좌가 샜다 — 리뷰), Redisson 예외 메시지의 사용자 id, Hibernate 제약 위반 WARN 의 멱등키, 게이트웨이가 trace 안에서 로그를 안 쓰는 것, 측정 도구의 L4 기다림 한계.
- **CI** ([#39](https://github.com/hyeokjinON/bank-transfer/pull/39)): `k8s-smoke` **5분 32초** (20분 제한, 스모크 [12] 포함 — 성공 기준 7 통과). 앞 두 실행은 CI 환경 차이로 실패해 고쳤다: ① GitHub 러너의 `fs.inotify.max_user_instances=128` 을 kind 노드 3개가 같이 써서 제어 노드 Alloy 가 "too many open files" 로 죽음 → `import.file` 감시를 폴링(`detector = "poll"`)으로 + CI 에 sysctl 단계, ② CI 의 `mysql:8.4` 소켓 경로가 달라 스모크 [12] 의 `sql()` 이 붙지 못함 → TCP(`127.0.0.1`)로. Postman 직접 확인 `10. 로그 (P9c, kind)` 16/16 · Grafana 로그 ↔ trace 클릭 이동 통과 (2026-11-20). 자세히는 [측정 결과](../perf/p9c-logs.md) "CI".

## 위험 · 다음
- **Loki emptyDir**: Loki Pod 가 바뀌면 이전 로그가 모두 사라진다 (L4 에서 확인). 수집이 밀리는 동안은 Loki 로 장애를 보면 그 구간이 비어 보인다 — "로그가 없다" 를 "일이 없었다" 로 읽지 않는다.
- **과한 가리기**: 10 ~ 16자리 숫자는 모두 가려진다 — epoch 밀리초 · Kafka 오프셋 · 스냅숏 id 의 숫자 덩어리, `1원인증` → `***원인증` (진단 정보가 일부 줄어듦).
- **앱 알림의 로그 사본**: api 의 `AlertNotifier` 는 텔레그램을 켜도 먼저 로그(`ALERT` 로거)에 남기고 그 줄도 Loki 로 온다. 금액에 '원' 이 없어 Alloy 규칙에 걸리지 않으므로 앱이 로그 사본에서만 금액 · 잔고를 가린다(`AlertLogMask`). 텔레그램 본문은 그대로다. (Alertmanager → alert-sink 알림은 지표 알림 경로로, 이것과 별개.)
- **Loki · Alloy 에 클러스터 안 Pod 가 닿는다**: Loki 는 auth 를 껐고 push · delete API 가 열려 있으며, Alloy UI(12345)도 열려 있다. monitoring namespace NetworkPolicy(P9a 에서 넘긴 일)로 막을 것.
- **텍스트 여러 줄 스택**(Kafka · 모의 은행)은 줄마다 따로 수집된다 (JSON 쪽 api · 게이트웨이는 한 줄).
- **로그 알림 도착이 계산보다 늦다** (L6 ② — 원인은 추정).
- 운영이라면 (스펙 §11): Loki 객체 저장소 · 분산 모드 · 멀티 테넌트, 로그 보관 정책(법정 보관 기간 · 별도 감사 로그), monitoring · kube-system 로그, Kafka 여러 줄 스택 묶기, 로그에서 지표 만들기(recording rule), monitoring namespace NetworkPolicy, Alloy WAL(긴 Loki 장애에서 재시도 한도를 넘는 줄 보호).
- 다음: P9 는 P9c 로 끝난다 (머지 뒤 정리 PR 에서 이 ADR 을 `채택`). 이후 로드맵은 README · HANDOVER.

## 검토한 대안
- **앱이 OTLP 로 로그를 직접 보내기**: JVM 앱만 가능하고(Kafka · 모의 은행 등 제외), 보내기 버퍼가 차면 줄이 유실된다 → 표준 출력 + 노드 수집.
- **K8s API 로 로그 끌어오기**: API 서버에 부하를 주고 Pod 삭제 때 유실된다 → 노드 파일을 읽는 DaemonSet.
- **Promtail**: 2026-03 지원 종료 → Grafana Alloy.
- **Elasticsearch**: 무겁다 — 지금은 Grafana 한 화면(지표 · trace · 로그)이면 충분 → Loki.
- **traceId 를 라벨로**: 출금마다 새 스트림이 생겨 색인이 터진다 → 구조화 메타데이터.
- **Alloy 가리기만 (앱은 그대로)**: 로그 형식이 바뀌면 샌다 → 앱이 남기지 않는 것 + Alloy 가리기 + 금지 패턴 질의의 이중 방어(+ 증명).
