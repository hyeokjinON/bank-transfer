# ADR-0019: 게이트웨이 두 복제본 필수 분산 — `DoNotSchedule` 토폴로지 분산

- 상태: 채택 (2026-11-17, [#33](https://github.com/hyeokjinON/bank-transfer/pull/33) 머지 `0aee59d` — 제안 2026-11-16)
- 관련: [ADR-0018](0018-gateway-ha-leader-election.md) (게이트웨이 이중화 — "위험 · 다음" 의 "복제본 2개가 한 노드에 있으면 이중화가 아니다" 가 이 결정의 출발점) · [ADR-0017](0017-kubernetes-kind.md) (kind · api 분산 `matchLabelKeys`) · [측정 결과](../perf/p8-3c-gateway-spread.md) · OPERATION §16-3

## 맥락
- P8-3b 는 게이트웨이를 2대(리더 · 대기)로 만들었지만 배치 규칙은 `ScheduleAnyway`(되도록 나눔)였다. 실험 중 워커 하나가 빠진 동안 대체 Pod 가 **남은 워커로 갔고**, 워커가 돌아와도 K8s 는 이미 뜬 Pod 를 옮기지 않아 **두 대가 한 워커에 남았다.** 그 워커를 SIGKILL 하자 리더 · 대기가 함께 사라져 **새 리더까지 353.4초** (P8-3b H4′ 변형, 노드 장애 감지 약 50초 + 축출 대기 300초).
- 같은 실험에서 두 대가 다른 워커면 새 리더 14.6초 (Lease 만료). 이중화의 효과는 **배치가 나뉘어 있을 때만** 나온다.
- 이 클러스터의 게이트웨이 자리는 워커 2개(`nodeSelector: role=app`)뿐이다.

## 결정
1. **게이트웨이 `topologySpreadConstraints` 를 `whenUnsatisfiable: DoNotSchedule`** 로 바꾼다 (`maxSkew 1`, `topologyKey: kubernetes.io/hostname`). 나눌 수 없으면 같은 노드에 띄우지 않고 **Pending 으로 기다린다.**
2. **`matchLabelKeys: [pod-template-hash]` 를 유지한다** — 같은 리비전끼리만 센다. 그래야 롤링(`maxSurge 1 · maxUnavailable 0`)의 새 Pod 가 옛 Pod 가 있는 워커에도 잠깐 뜰 수 있어 배포가 멈추지 않는다.
3. **`nodeTaintsPolicy: Ignore` · `nodeAffinityPolicy: Honor` 를 명시한다** (둘 다 기본값 — 의도를 매니페스트에 남긴다). Ignore = cordon · 장애로 taint 가 붙은 워커도 "자리" 로 센다 → 대체 Pod 가 남은 워커로 몰리지 않고, 그 워커가 돌아오면 그곳에 뜬다. Honor = `nodeSelector` 에 맞는 워커만 자리로 센다 (제어 노드 제외).
4. **스모크 [9] 에 "두 게이트웨이가 서로 다른 워커" 를 넣는다** — PR 마다 CI 가 확인한다.
5. api 는 바꾸지 않는다 (`ScheduleAnyway` 유지). api 는 상태가 없고 몰려도 처리량 · 가용성만 줄어든다 — 은행 연결 주인 문제가 없다.

## 결과
측정 (2026-11-16, 한 PC · kind 3노드 — 운영 수치가 아니다, [측정 결과](../perf/p8-3c-gateway-spread.md)). 판정(돈 정확성)은 전 회차 통과.
- **S1 롤링**: 멈추지 않고 끝났고 끝난 뒤 두 워커에 하나씩 (몰림 0초 · Pending 0초). 은행 공백 2.5초, 세션 거부 · 번호 중복 · 결과 모름 0.
- **S2 리더 워커 drain**: 리더가 다른 워커로 3.3초에 넘어갔고(은행 공백 3.1초), 쫓겨난 자리의 대체 Pod 는 **42초 Pending → uncordon 뒤 원래 워커**에 떴다. 몰림 0초. (P8-3b 에서는 이 대체 Pod 가 남은 워커로 가 몰림이 시작됐다.)
- **S3 대기자 워커 SIGKILL**: 리더 교대 없음(은행 세션 그대로). 축출 뒤 대체 Pod **70.5초 Pending → 노드 복구 뒤 원래 워커**. 스케줄러 사유 `didn't match pod topology spread constraints` (실측).
- **S4 이어서 리더 워커 SIGKILL**: 다른 워커의 대기자가 **14.4초**에 리더 (은행 공백 14.2초 = Lease 만료). P8-3b 에서 같은 순서(한 워커 장애 → 복귀 → 다른 워커 장애)는 두 대가 한 워커에 몰려 **353.4초**였다. 대체 Pod 는 다시 71.8초 Pending → 원래 워커.
- 전 회차 몰림 0초, 세션 거부 · 번호 중복 · 결과 모름 0. 노드 SIGKILL 회차의 첫 시도 5xx 369 · 377 은 같은 워커의 **api** Pod 때문(노드 장애 감지 전 약 50초 — P8-3a D2 와 같은 현상)이고 돈은 정확하다.

## 위험 · 다음
- **워커가 빠진 동안은 대기자가 없다.** 게이트웨이 1대(리더)로 버틴다. 다만 몰려 있었어도 그 워커 장애에는 둘 다 죽으므로, 잃는 것은 "리더 프로세스만 죽을 때 같은 워커의 대기자가 빨리 이어받는 것" 뿐이다 (그 경우에도 kubelet 재시작 + Lease 재획득으로 돌아온다 — 약 15 ~ 30초, 추정).
- **워커가 오래 못 돌아오면** 두 번째 게이트웨이는 계속 Pending 이다. 운영이라면 워커를 3개 이상 두거나 새 워커를 붙인다. 분산을 풀어 같은 워커에 띄우는 것은 권하지 않는다.
- 이 클러스터는 워커 2개라 "빠진 워커 대신 세 번째 워커에 뜨는" 경우는 시험하지 않았다 (`maxSkew 1` 이면 될 것으로 본다 — 추정).
- 아직 안 한 것: descheduler, 노드 도달 불가 `tolerationSeconds` 단축(축출 대기 300초 → 짧게 — 짧은 끊김에도 옮기는 대가), Envoy outlier detection, 재접속 백오프 상한.

## 검토한 대안
- **필수 pod anti-affinity (`requiredDuringScheduling…`, `app=bank-gateway`)**: 옛 리비전 Pod 까지 센다 → 워커 2개가 모두 차 있으면 롤링의 새 Pod(`maxSurge 1`)가 갈 곳이 없어 **배포가 멈춘다.** `maxSurge 0 · maxUnavailable 1` 로 바꾸면 되지만 롤링 동안 대기자가 없어진다. 분산 규칙 + `matchLabelKeys` 가 더 작다.
- **`nodeTaintsPolicy: Honor`**: taint 붙은(cordon · 장애) 워커를 계산에서 빼므로 대체 Pod 가 남은 워커로 간다 — 지금 문제 그대로.
- **descheduler 로 몰린 뒤 다시 퍼뜨리기**: 사후 처리이고 몰린 동안의 위험은 그대로, 설치할 것이 늘어난다.
- **`ScheduleAnyway` 유지 + 운영 절차(`rollout restart`)**: 사람이 알아채야 한다. P8-3b 실험에서 실제로 놓쳤다.
