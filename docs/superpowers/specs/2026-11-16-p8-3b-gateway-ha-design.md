# P8-3b 게이트웨이 이중화 (active-standby · Lease 리더 선출 · 전문번호 구간 예약) — 설계

> 날짜: 2026-11-16 · 브랜치 `feat/p8-3b-gateway-ha` · 기준 main `58bb030` (P8-3a #29 · 정리 #30 머지)
> P8 전체: P8-1 게이트웨이 분리 → P8-2 이미지 · Compose → P8-3a Kubernetes (완료) → **P8-3b 게이트웨이 이중화 (이 문서)**
> 앞 단계가 넘긴 것 (ADR-0017 "위험 · 다음"): drain 퇴거 때 `Recreate` 게이트웨이 두 대가 4 ~ 7초 동시에 은행 연결 → 리더 선출 · Lease 로 연결 주인 하나. (Envoy outlier detection · 재접속 백오프 상한은 이번 범위 밖 — §14)

## 1. 목표와 성공 기준
게이트웨이를 2대로 늘리되 **은행에 붙는 것은 언제나 리더 1대**로 하고(active-standby), 은행이 요구하는 **전문번호 유일성**을 선출과 독립된 장치로 지킨다. 실무 제약(세션 하나 · 번호 유일)을 모의 은행 옵션으로 재현해 전 · 후를 실험으로 비교한다.

성공 기준:
1. 게이트웨이 2대 중 은행에 붙는 것은 **언제나 1대** — 모든 실험에서 모의 은행 동시 세션 최댓값 1.
2. **전문번호 중복 0** (모든 실험) + 구간 예약 경쟁 테스트 통과.
3. 정상 교대(drain · 롤링)에서 **결과 모름(UNKNOWN) 0**, 교대 공백 시간을 잰다.
4. 비정상 교대(강제 삭제 · 노드 정지)에서 돈 정확(접수 = 출금 행 = 은행 기록, 이중 0), 결과 모름은 결과조회로 전부 `COMPLETED`.
5. **H0 대조**: 이전 구성(1대 · `Recreate`)이 같은 제약에서 깨지는 것을 숫자로 남긴다.
6. IntelliJ · Compose 동작과 기존 테스트(623개)가 그대로 — 선출은 기본 꺼짐.
7. 처리량은 재지 않는다.

## 2. 근거
| 근거 | 내용 |
|---|---|
| 측정 | P8-3a D1 (3회): 새 게이트웨이 +6초 Ready, 옛 게이트웨이 +12초 종료 → 4 ~ 7초 동시 연결. `Recreate` 는 배포에만 적용, 퇴거는 새 Pod 를 먼저 만든다 (`docs/perf/p8-3a-k8s.md` 단계 5) |
| 단일 장애점 | 게이트웨이 1대 — 노드 장애면 대체까지 수 분 (P8-3a D2) |
| 실무 제약 (추정) | 은행 FEP 중 기관당 세션 수 제한, 기관 · 일자별 전문번호 유일 요구가 있다 |
| 지금 코드 | 전문번호 = `BankGatewayClient` 의 `AtomicLong`(인스턴스마다 1부터, `BankGatewayClient.kt:97`). 규격 v0.2 는 "세션 안 짝짓기용, 재사용" (`docs/protocol.md` 표 3행) |

## 3. 결정
| # | 결정 | 이유 | 버린 대안 |
|---|---|---|---|
| 1 | 범위는 이중화 하나 | 실험 · 이야기를 하나로 | outlier detection · 백오프 상한 함께 |
| 2 | **active-standby** (리더 1대만 은행 연결) | 세션 수 제한 · 번호 유일 두 제약을 가장 단순하게 | active-active (세션 여러 개 은행이면 실무에서 더 흔함 — §14) |
| 3 | **K8s Lease + fabric8 `LeaderElector`** | 새 인프라 없음, 표준, 게이트웨이 DB 없음 원칙(ADR-0015) 유지 | Lease 직접 구현 (위험), MySQL 잠금 (DB 장애 = 은행 연결 장애) |
| 4 | 리더 임기마다 `BankGatewayClient` 새로 생성 · 상실 시 즉시 close | 지금 클라이언트는 close 가 이벤트 루프까지 닫아 재사용 불가, 임기 단위로 상태를 끊는다 | stop/start 추가 |
| 5 | **트래픽 = 리더 라벨** (리더가 자기 Pod 에 `bank-transfer/gateway-role=leader`, Service selector 에 이 라벨). **readiness 는 둘 다 UP** — 대기자의 `bankConnection` = UP(`role: standby`), 리더는 은행 연결이 끊기면 DOWN (P8-3a B1 그대로). liveness 그대로 (§16) | 대기자가 Ready 여야 롤링(`maxUnavailable 0`) · `rollout status` · PDB 가 정상 동작한다. 리더만 Ready 로 두면 새 Pod 가 영원히 NotReady 라 롤링이 멈춘다. Patroni(`role=master` 라벨) 같은 실무 패턴 | readiness = 리더만 UP (롤링 정지), `Recreate` 로 되돌리기 (배포마다 공백) |
| 6 | Lease 15s · 갱신 기한 10s · 재시도 2s, 갱신 실패 시 자기 차단 | 옛 리더가 늦어도 10초에 끊고 새 리더는 15초 만료 뒤 획득 (노드 시계 차 5초 미만 전제) | 갱신 기한 = 유효 시간 |
| 7 | `bank.gateway.leader-election.enabled` 기본 false (항상 리더) | IntelliJ · Compose · 테스트 그대로 | 모든 환경에서 선출 |
| 8 | **전문번호 = ConfigMap 구간 예약** (`date` · `reservedTo`, 1,000개씩, `resourceVersion` CAS) | 번호 유일성이 **선출 정확성과 독립** — 두 대가 잠깐 겹쳐도 구간은 겹치지 않는다 (hi/lo) | Lease 주석에 저장, 시각 기반 번호 |
| 9 | 남는 번호는 건너뜀, 예약 실패 시 송신 안 함(`BankNotSentException`) | 유일성만 요구(추정), 실패는 "안 보냄" 재시도 경로 | 예약 실패에도 송신 |
| 10 | 날짜 전환은 점검 시간(23:50 ~ 00:10) 안 | 송신이 막혀 경계 문제 없음 | 자정 강제 리셋 |
| 11 | 정상 교대 순서: **새 요청 거절 → 처리 중 전문 비우기(최대 5s) → 연결 끊기 → Lease 반납** | 결과 모름 0, 반납 전에 세션을 비워 새 리더가 바로 붙음 | 반납 먼저, 비우기 없이 끊기 |
| 12 | 물러나는 시점 = **SIGTERM 종료 훅** (게이트웨이 preStop 없음) | 호출자(api)는 "안 보냄" 을 안전하게 재시도 — 기다릴 이유 없음. 공백 약 3 ~ 7초 (추정) | preStop sleep 10s 유지 (공백 약 13 ~ 17초), preStop 반납 호출 |
| 13 | 비정상 교대: Lease 만료 15s, 응답 못 받은 전문 = 결과 모름 → 결과조회 | 기존 안전망 (ADR-0006) | 다시 보내기 |
| 14 | 규격 v0.4 (§16): 전문번호 기관 · 일자(KST)별 유일, 응답코드 **`3001` 전문번호 중복 (처리 안 함)** | 실무 제약 재현 | v0.2 유지 |
| 15 | 게이트웨이는 `3001` → `503 BANK_NOT_SENT` | 은행이 처리하지 않음 — api 가 실패(환불)로 확정하면 안 된다 (P8-1 분류) | 응답 코드 그대로 |
| 16 | 모의 은행 옵션 (기본 꺼짐, K8s 에서만 켬): `--single-session` · `--session-idle-timeout=60s` · `--unique-tran-no` | 실무 제약 재현, 개발 흐름 불변. 60s = 게이트웨이 폴링(30s)의 2배 | 항상 켬 (개발 재시작마다 중복 거절) |
| 17 | `TranNoSource` 인터페이스 (`modules/bank-gateway`), 기본 메모리 카운터 · K8s 는 앱 모듈의 ConfigMap 구현 | 포트/어댑터(ADR-0001), 통신 모듈이 K8s 를 모름 | 클라이언트가 K8s API 직접 호출 |
| 18 | `BankConnectionOwner` 가 "지금 클라이언트" 의 단일 진실 — 엔드포인트 · 헬스 · 종료 훅이 Owner 를 거침 | 상태가 어긋날 틈 제거 | 컴포넌트별 콜백 구독 |
| 19 | 게이트웨이 2대 · RollingUpdate(0/1) · preStop 없음 · 분산(`matchLabelKeys`) · PDB `minAvailable 1` | 롤링이 안전해짐, 노드 하나 장애에 생존 | `Recreate` 유지 |
| 20 | RBAC: ServiceAccount `bank-gateway`, Role = Lease `bank-gateway-leader` · ConfigMap `bank-gateway-tran-no` 이름 한정 get · update (+ create), **Pod get · patch (namespace 범위 — 자기 라벨용, Pod 이름을 미리 몰라 이름 한정 불가)** | 최소 권한 (라벨 때문에 Pod 권한만 namespace 범위) | 넓은 권한 |
| 21 | NetworkPolicy 변경 없음 | 게이트웨이 → API 서버는 egress, 지금 정책은 ingress 만 | — |
| 22 | 스모크 [9]: 게이트웨이 Ready 2/2 · **리더 라벨 Pod 정확히 1개 = Lease 보유자** · Service 엔드포인트 = 그 Pod · 모의 은행 동시 세션 1 | PR 마다 "주인 하나" 회귀 확인 | 실험에서만 |

## 4. 구조
```
             ┌──────────── Lease bank-gateway-leader (holder = Pod 이름)
             │              ConfigMap bank-gateway-tran-no (date, reservedTo)
api ×2 ─▶ Service bank-gateway ─▶ gateway-A (리더, 라벨 gateway-role=leader, Ready) ──TCP 세션 1개──▶ 모의 은행 (--single-session …)
          (selector: app=bank-gateway,   gateway-B (대기, 라벨 없음, Ready — 은행 연결 없음, Service 대상 아님)
           gateway-role=leader)
```
- `LeaderElection` (앱): fabric8 `LeaderElector`, identity = Pod 이름(`POD_NAME`), namespace = `POD_NAMESPACE`. 콜백 → `BankConnectionOwner`.
- `BankConnectionOwner` (앱): `onStartLeading` → `TranNoSource` 로 새 `BankGatewayClient` 생성 · `start()`; `onStopLeading` → 즉시 `close()`. 선출 꺼짐이면 기동 때 바로 리더.
- `GatewayEndpoints` · `BankConnectionHealthIndicator`: Owner 의 현재 클라이언트를 쓴다. 리더가 아니면 엔드포인트는 `503 BANK_NOT_SENT`, 헬스는 **UP(`role: standby`)** — 대기자도 Ready. 리더인데 은행 연결이 없으면 DOWN(`role: leader, reason: not connected`).
- `LeaderLabel` (앱): 리더가 되면 자기 Pod 에 `bank-transfer/gateway-role=leader` 를 붙이고(JSON merge patch), 물러날 때 뗀다. 선출 꺼짐이면 아무것도 안 한다 (Compose · IntelliJ 는 Service selector 가 없다).
- 종료 훅: `SmartLifecycle`(웹 서버보다 먼저 멈추는 phase) — §6.

## 5. 전문번호 구간 예약
- ConfigMap `bank-gateway-tran-no`: `data.date`(KST `yyyyMMdd`) · `data.reservedTo`(정수). 없으면 처음 예약자가 만든다(create 충돌이면 다시 읽기).
- 예약: 읽기 → `date` 가 오늘이면 `start = reservedTo + 1`, 아니면 `start = 1` → `reservedTo = start + 999` 로 **resourceVersion 조건부 update** → 409(충돌)면 다시 읽고 반복(최대 5회). 성공한 구간 `[start, start+999]` 만 쓴다.
- 구간의 끝 100개 전에 다음 구간을 미리 예약(백그라운드), 구간이 바닥나도 다음 예약이 없으면 송신 안 함(`BankNotSentException`).
- 날짜가 바뀌면(첫 송신 때 KST 오늘 ≠ 구간의 날짜) 새 날짜 구간을 예약. `MAX_TRAN_NO`(99,999,999) 를 넘으면 송신 안 함 (하루 1억 건 — 현실적으로 도달 안 함).
- 선출 꺼짐(K8s 밖)이면 메모리 카운터(지금과 같음).

## 6. 교대
### 6-1. 정상 교대 (SIGTERM — drain · 롤링 · 삭제)
1. 송신 문을 닫는다 — 새 요청 `503 BANK_NOT_SENT`.
2. 응답 대기 전문(`pendingCount`)이 0이 될 때까지 최대 5초(응답 시간 초과 3초 + 여유).
3. 은행 연결 close.
4. 자기 Pod 의 리더 라벨 떼기 (Service 대상에서 빠짐).
5. Lease 반납(holder 비움 — fabric8 `releaseOnCancel`).
6. 나머지 Spring graceful 종료. `terminationGracePeriodSeconds` 45 그대로.
- 대기자: 2초(재시도 주기) 안에 획득 → 구간 예약 → 접속 → **자기 Pod 에 리더 라벨** → Service 엔드포인트 반영(수 초). 공백 약 3 ~ 7초 (추정, H1 · H2 로 잰다).

### 6-2. 비정상 교대
- SIGKILL · 노드 손실: 반납 없음 → 15초 만료 뒤 대기자 획득. 노드째 사라지면 은행 쪽에 FIN 이 없어 옛 세션이 남는다 → 모의 은행 `--session-idle-timeout=60s` 가 정리 → 새 리더 접속(그 전까지 거부 · 재접속 백오프). 공백 약 60초 (추정, H4).
- API 서버와만 단절: 리더는 10초 갱신 기한에 자기 차단(연결 close), 대기자는 15초 만료 뒤 획득 (H5). 옛 리더는 API 서버에 닿지 않아 **라벨을 못 뗀다** → 잠깐 리더 라벨 Pod 가 둘 — 옛 리더로 간 요청은 연결이 없어 `503 BANK_NOT_SENT` 로 재시도된다. **은행 동시 세션은 여전히 최대 1** (라벨이 아니라 연결이 기준).
- 응답 못 받은 전문: `BANK_UNAVAILABLE`(502) → api 결과 모름 → 결과조회(P3-3a)로 확정.

## 7. 규격 v0.4 · 모의 은행
- `docs/protocol.md`: 전문번호 정의 변경, 응답코드 표에 `3001`, 변경 이력 v0.4 (§16).
- 모의 은행 옵션 (`MockBankOptions`):

| 옵션 | 동작 | 로그 |
|---|---|---|
| `--single-session` | 세션이 이미 있으면 새 연결을 받자마자 close | `session rejected` (+ 현재 세션 수) |
| `--session-idle-timeout=<초>` | 그 시간 동안 받은 전문이 없으면 세션 close (폴링 포함) | `session evicted idle` |
| `--unique-tran-no` | 같은 KST 날짜에 같은 전문번호 요청 → 처리하지 않고 응답코드 `3001` | `duplicate tran no` |

- 모든 연결 수락 · 종료 때 `sessions=<현재> max=<최대>` 를 남긴다.

## 8. 매니페스트 (`infra/k8s`)
- `base/bank-gateway.yaml`: Service selector 에 `bank-transfer/gateway-role: leader` 추가, replicas 2, `RollingUpdate` (maxUnavailable 0, maxSurge 1), preStop 제거, `serviceAccountName: bank-gateway`, env `POD_NAME`(fieldRef) · `POD_NAMESPACE`(fieldRef) · `BANK_GATEWAY_LEADERELECTION_ENABLED=true`, `topologySpreadConstraints`(hostname, ScheduleAnyway, `matchLabelKeys: [pod-template-hash]`).
- `base/rbac-gateway.yaml` (새): ServiceAccount · Role(Lease · ConfigMap 이름 한정 + Pod get · patch) · RoleBinding.
- `base/pdb.yaml`: 게이트웨이 PDB `minAvailable: 1` 추가.
- `base/mock-bank.yaml`: args 에 세 옵션.
- `experiment-*` overlay (실험용): `experiment-gateway-single` — H0 대조 (게이트웨이 replicas 1 · `Recreate` · 선출 끔).

## 9. 실험 (`load/k8s/run.sh` 확장)
| 실험 | 방법 | 볼 것 |
|---|---|---|
| H0 대조 | `experiment-gateway-single` + 모의 은행 옵션 켬, 부하 중 게이트웨이 노드 drain ×1 | 세션 거부 수, `3001` 중복 수, 결과 모름 수, 공백 |
| H1 정상 교대 | 부하 중 **리더가 있는 워커** drain ×2 | 동시 세션 최대(1), 거부 · 중복(0), 결과 모름(0), 공백(Ready 게이트웨이 0 인 시간) |
| H2 롤링 | 부하 중 게이트웨이 이미지 태그 교체 ×2 | H1 과 같음 |
| H3 리더 강제 삭제 | `--force --grace-period=0` ×2 | 정상 교대가 일어났는지(SIGTERM) 만료 대기인지, 공백 |
| H4 리더 노드 정지 | `docker stop` ×1 | 공백(약 60초 예상), 거부 수, 결과 모름 → 결과조회로 전부 `COMPLETED` |
| H5 API 서버 단절 | 리더 Pod → API 서버(443/6443) egress 차단 NetworkPolicy ×1 | 자기 차단 시각(≤ 10s), 대기자 획득(≥ 15s), 동시 세션 최대 1. **kindnet 이 egress 를 집행하지 않으면 단위 테스트로 대신하고 그렇게 적는다** |
- 표본기(`timeline.sh`)에 Lease 보유자 · 게이트웨이 Ready · 모의 은행 동시 세션(로그 기반)을 더한다. 판정은 P8-3a 와 같다 + 결과 모름 건수.

## 10. 테스트
- `TranNoSource` ConfigMap 구현: 두 예약자 동시 경쟁 → 구간 안 겹침, 날짜 변경 → 1부터, 409 재시도, 예약 실패 → 송신 안 함 (fabric8 `kubernetes-server-mock`).
- 선출 · Owner: 획득 → 접속, 상실 → 즉시 close, 갱신 실패 → 자기 차단, 종료 훅 순서(거절 → 비우기 → 끊기 → 반납), 선출 꺼짐 → 기동 즉시 리더.
- 모의 은행: 세 옵션 각각.
- 게이트웨이: `3001` → `503 BANK_NOT_SENT`, 리더 아님 → `503` · readiness **UP(standby)**, 리더 + 은행 끊김 → readiness DOWN.
- 리더 라벨: 리더 획득 → 자기 Pod 에 라벨, 물러남 → 라벨 제거 (fabric8 가짜 API 서버).
- 기존 623개 그대로 통과.

## 11. CI · 스모크
- `smoke.sh` [0] · [6] · [7] 의 게이트웨이 가정을 고친다: 준비 확인은 Ready 2/2, readiness · `id -u` 는 리더 라벨 Pod 로.
- `smoke.sh` [9]: 게이트웨이 2대 Ready, 리더 라벨 Pod 정확히 1개 = Lease holder, Service 엔드포인트 = 그 Pod, 모의 은행 로그의 최신 `sessions=1`.
- `k8s-smoke` 잡 변경 없음 (시간 확인 — 지금 5분 45초).

## 12. 변경 목록
- 코드: `modules/bank-gateway` (`TranNoSource`, `3001` 처리), `apps/bank-gateway` (선출 · Owner · ConfigMap 구간 예약 · 종료 훅 · 헬스 · 엔드포인트 · 설정), `apps/mock-bank` (옵션 3 · 로그).
- 의존성: fabric8 `kubernetes-client` · `kubernetes-server-mock`(테스트) — **버전은 계획에서 고정 (2주 이상 지난 안정판)**.
- 매니페스트 · 스크립트 · CI 스모크 · 실험 도구 · 문서 (§15).

## 13. 위험
- **fabric8 · Boot 4 의 Jackson 버전 차** — (§16) fabric8 8.0.0 은 Jackson 3(3.2.3)이라 "Jackson 2 공존" 문제는 없고, Boot 4.1.1 BOM 이 3.1.5 로 내려 맞추는 차이가 위험. 계획 Task 1 에서 확인, 안 되면 Jackson BOM 을 3.2.3 으로 올리고, 그래도 안 되면 설계로 돌아간다 (대안: Lease 직접 구현 B).
- 시계 차: 자기 차단의 안전은 노드 간 시계 차 < 5초 전제. kind 는 같은 호스트 시계라 문제 없음, 운영은 NTP 필수.
- API 서버 장기 장애 = 리더 자기 차단 = 송신 중단 (안전 쪽으로 실패 — 의도).
- 노드 손실 공백 약 60초 (세션 정리 대기) — 실무 은행의 세션 정리 시간에 좌우된다.
- H5 egress 집행 미확인.

## 14. 범위 밖 ("실제 운영이라면")
고정 출구 IP(egress gateway · NAT)와 은행 IP 등록, 망분리 · 전용선, 대외계 VM + VIP(keepalived) 방식, 업무개시 · 종료 전문, active-active(세션 여러 개 은행 + 번호 공유 발급 — 이 설계의 구간 예약으로 확장 가능), Envoy outlier detection, 재접속 백오프 상한, 여러 은행, 서비스 간 인증(mTLS).

## 15. 문서 · 작업 순서
ADR-0018 (제안 → 머지 뒤 채택), `docs/protocol.md` v0.4, 측정 `docs/perf/p8-3b-gateway-ha.md`, OPERATION §16, README · ARCHITECTURE · HANDOVER · CHANGELOG · `infra/k8s/README.md`.
작업 순서: 의존성 확인(Jackson) → `TranNoSource` · 규격 v0.4 · 모의 은행 옵션 → Owner · 선출 · 구간 예약 · 종료 훅 → 매니페스트 · 스모크 [9] → 실험 H0 ~ H5 → 문서.

## 16. 계획 단계에서 고친 점 (2026-11-16)
| 고친 곳 | 내용 | 이유 |
|---|---|---|
| §3 결정 5 · §4 · §6 · §8 · §11 | "readiness = 리더만 UP" → **리더 라벨 + Service selector, 둘 다 Ready** | 대기자가 영원히 NotReady 면 `maxUnavailable 0` 롤링이 새 Pod 를 기다리다 멈추고, `rollout status` · `up.sh` 준비 대기 · 스모크 [0] · 실험 도구 사전 확인("Ready = replicas")이 모두 막힌다 |
| §3 결정 20 | RBAC 에 Pod get · patch (namespace 범위) | 리더 라벨을 자기 Pod 에 붙이고 떼려면 필요. Pod 이름을 미리 몰라 이름 한정 불가 — 최소 권한이 조금 넓어진 대가 |
| §6 | 교대 순서에 라벨 떼기 · 붙이기 | 정상 교대: 끊기 → 라벨 떼기 → 반납. 새 리더: 접속 → 라벨 붙이기 |
| §3 결정 14 · §7 · §15 | 규격 **v0.3 → v0.4** | `docs/protocol.md` 변경 이력에 v0.3(2026-10-29 대사 파일)이 이미 있다 |
| §12 · §13 | fabric8 **8.0.0** 고정 (2026-09-24, Jackson 3 · JDK HttpClient, vertx 제외) | 2주 이상 지난 안정판. Jackson 2 가 아니라 Jackson 3 계열 — 위험은 버전 차(3.2.3 ↔ Boot 3.1.5) |
| §4 · §6-2 | K8s 클라이언트 빨리 실패 (재시도 0 · 연결 2s · 요청 5s) | fabric8 기본 재시도(지수 백오프)는 API 서버 단절 때 갱신 실패를 늦게 알려 "10초 안 자기 차단" 을 흐린다 |
