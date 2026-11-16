# P8-3c 게이트웨이 필수 분산 실험 결과

> 결정 [ADR-0019](../adr/0019-gateway-required-spread.md) · 도구 `load/k8s/run.sh` (`PHASE=p8-3c`) · 원자료 `raw/p8-3c/` (`<동작>-<회차>.json` 요약 · `.k6.json` · `.timeline.txt` 상태 표본)
> 환경: P8-3b 와 같은 PC · kind 3노드(제어 1 · 워커 2) · 이미지 `dev-aba0c2f` (코드는 P8-3b 와 같고 매니페스트만 바뀜). 게이트웨이 2대, 분산 규칙 `DoNotSchedule` · `nodeTaintsPolicy Ignore` · `matchLabelKeys [pod-template-hash]`.
> 부하 · 판정: P8-3b 와 같다 — k6 출금 20건/s 60초, 판정은 돈 정확성(접수된 키 = 출금 행 = 거래번호, 전부 `COMPLETED`, 은행 기록 빠짐 0 · 이중 0, 환불 0, 잔고 = 원장 합). **전 회차 통과.**
> 새 지표: `gw_pending_s` = 게이트웨이 Pod 가 노드 없이(Pending) 기다린 시간, `gw_colocated_s` = Ready 게이트웨이 2대 이상이 모두 한 노드에 있던 시간, `gw_nodes_end` = 끝난 뒤 Ready 게이트웨이가 있는 노드 수. 표본 간격 1 ~ 2초 (±1.5초).

## 요약
| 질문 | 결과 |
|---|---|
| 롤링 업데이트가 멈추지 않나 (필수 분산의 함정) | **멈추지 않았다** (S1). 끝난 뒤 두 워커에 하나씩 |
| 워커가 빠지면 대체 Pod 는 어디로 가나 | **남은 워커로 가지 않고 Pending** — drain 42초 (S2), 노드 SIGKILL 70.5초 (S3) |
| 워커가 돌아오면 | **그 워커에 다시 뜬다** — 나뉜 상태가 사람 손 없이 복구 (S2 · S3) |
| 한 노드에 몰린 시간 | **전 회차 0초** |
| P8-3b 의 353초 경로 (워커 하나 장애 → 복귀 → 다른 워커 장애) | **353.4초 → 14.4초** (S3 → S4). 두 번째 장애 때 대기자가 다른 워커에 살아 있어 Lease 만료(15초)만에 이어받았다 |

## 결과 표
| 실험 | 동작 | Pending | 몰림 | 끝난 뒤 노드 | 은행 공백 | 새 리더 | 세션 거부 · 번호 중복 · 결과 모름 |
|---|---|---|---|---|---|---|---|
| S1 `gwroll-1` | 게이트웨이 롤링 (이미지 태그 교체) | 0초 | 0초 | 2 | 2.5초 | 8.8초 (롤링 시작 기준) | 0 · 0 · 0 |
| S2 `drain-1` | 리더 워커(worker2) drain → 부하 끝에 uncordon | **42.0초** | 0초 | 2 (원래 워커로) | 3.1초 | 3.3초 | 0 · 0 · 0 |
| S3 `gwnode-1` | **대기자** 워커(worker2) SIGKILL 420초 → 복구 | **70.5초** | 0초 | 2 (원래 워커로) | 없음 (리더 무관) | 교대 없음 | 0 · 0 · 0 |
| S4 `gwnode-2` | 이어서 **리더** 워커(worker) SIGKILL 420초 → 복구 | **71.8초** | 0초 | 2 (원래 워커로) | **14.2초** | **14.4초** (Lease 만료) | 0 · 0 · 0 |

은행 공백 = 모의 은행 로그의 `session closed` → 다음 `session opened`. 비교 대상(P8-3b, `ScheduleAnyway`): 롤링 H2 1.1 · 3.4초, drain H1 3.0 · 3.7초, 노드 SIGKILL H4′ 14.3초, **두 대가 한 노드에서 노드 SIGKILL 353.4초.**

## 무엇이 바뀌었나 — S2 drain 의 Pod 흐름 (표본 `drain-1.timeline.txt`)
```
동작 전    리더 rb7lk (worker2, Ready, leader) · 대기 zq2wk (worker, Ready)
+0.4초    drain worker2 → rb7lk 퇴거(정상 교대 시작), 대체 dvqvq 생성 → 노드 없음 (Pending)
+3.5초    zq2wk 가 리더 (Lease 획득 3.3초, 은행 공백 3.1초)
+42.4초   부하가 끝나 uncordon → dvqvq 가 worker2 에 배치
+47.1초   dvqvq Ready (대기자) — 두 워커에 하나씩
```
P8-3b(`ScheduleAnyway`)에서는 이 대체 Pod 가 바로 남은 워커(worker)로 가서 두 대가 한 워커에 남았다 — 353초 경로의 시작.

## S3 — 스케줄러가 남긴 사유
대체 Pod 가 Pending 이 된 순간의 Events (실측):
```
Warning  FailedScheduling  default-scheduler  0/3 nodes are available: 1 node(s) didn't match pod topology spread constraints,
                                               2 node(s) had untolerated taint(s). …
```
- `1 node … topology spread constraints` = 살아 있는 worker 에는 이미 게이트웨이가 있어 분산 규칙이 막음 (의도).
- `2 node(s) had untolerated taint(s)` = 죽은 worker2(`unreachable`) · 제어 노드(앱을 안 받음).
- 죽은 노드의 옛 Pod 는 노드가 돌아올 때까지 `Terminating` 으로 남는다 (kubelet 이 확인해 줄 수 없음). 대체 Pod 는 노드 장애 감지(52초) + 축출 대기(300초) 뒤에 만들어진다.
- 첫 시도 5xx 369 · 377건(S3 · S4) · 포기한 키 116 · 115 는 **같은 워커의 api Pod** 때문이다 (노드 장애 감지 전 약 50초 동안 Envoy 가 사라진 api 로 보냄 — P8-3a D2 와 같은 현상, outlier detection 은 범위 밖). 연결 단계 실패라 출금 행이 없고 돈은 정확하다.

## 한계
- 워커 2개뿐이라 "빠진 워커 대신 세 번째 워커에 뜨는" 경우는 시험하지 않았다.
- 회차당 1번. 시간 지표는 kubectl 표본(±1.5초).
- 워커가 빠진 동안 게이트웨이는 1대 — 그 사이 남은 워커가 죽는 이중 장애는 재지 않았다 (몰려 있었어도 같은 결과).
