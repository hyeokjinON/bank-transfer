# P6-1 컨슈머 코루틴 — 설계

> 상태: 설계 승인 (2026-11-04) · 다음: 구현 계획 · 결정 기록: ADR-0012 (구현과 함께 작성)
> 앞 단계: [P3 설계](2026-10-19-p3-transfer-design.md) (컨슈머 · 재시도 토픽), [P4-2 설계](2026-10-27-p4-2-lock-comparison-design.md) (전략 공존 · k6 측정 도구), [ADR-0005](../../adr/0005-kafka-delivery-and-consumer-idempotency.md) ("컨슈머 3개가 모두 기다리면 처리량이 막힌다 → P6 · P7 에서 비교")

## 1. 목적과 범위
출금 컨슈머(`transfer.requested`)는 은행 응답을 `CompletableFuture.get()` 으로 최대 3초 기다린다. 파티션 3 · 컨슈머 스레드 3이라 **동시에 기다릴 수 있는 출금은 3건**이고, 처리량은 대략 `3 ÷ 은행 응답 시간` 에서 막힌다. 컨슈머를 늘려도 파티션 수를 넘으면 논다.

P6-1 은 이 경로 하나를 **Kotlin 코루틴(`suspend` 리스너)** 버전으로도 만들어, 설정으로 고르게 하고 같은 PC · 같은 도구로 처리량을 잰다. 이어서 코루틴 쪽의 **용량 탐색**(은행 동시 호출 상한 · DB 풀을 키워 이 PC 에서 어디가 먼저 바닥나는지)을 한다.

- **지우지 않는다**: 지금의 블로킹 코드는 그대로 두고 기본값이다. 두 구현은 파일로 나뉘어 나란히 읽힌다 (P4-2 의 차감 전략 4종과 같은 방식).
- **범위 밖**: 웹 API(P6-2 — WebFlux 는 별도 앱 모듈), 정리 작업의 결과조회 · 1원인증의 코루틴화, 가상 스레드(P7), 파티션 · 컨슈머 수를 늘린 블로킹 비교, 여러 API 인스턴스, CI 에서의 측정(CI 는 정확성 테스트만).

## 2. 결정 요약
| 결정 | 내용 | 이유 |
|---|---|---|
| 바꾸는 경로 | 컨슈머의 출금 처리(은행 이체 호출)만 | 문서로 남겨 둔 실제 병목. 측정 대상이 하나라 결과가 선명하다 |
| 방식 | Spring Kafka 4.1 의 `suspend` `@KafkaListener` (비동기 반환), 은행 호출은 `await()` | 프레임워크가 지원하는 길. 레코드 단위 ack · `@RetryableTopic` 을 그대로 쓴다 (재시도 토픽 호환은 Task 1 실험으로 확인) |
| 고르는 법 | `transfer.consumer.mode = blocking`(기본) \| `coroutine` | 공존 · 같은 측정 도구 · 설정만 바꿔 공정 비교 |
| 판정 로직 | `TransferProcessor.process` 를 `begin` · `finish` 로 나누어 두 모드가 같이 쓴다 | 상태 판정(CONFLICT 등)을 복사하지 않는다. 다른 것은 은행 호출 한 줄뿐 |
| DB 호출 위치 | 코루틴 모드에서 `withContext(Dispatchers.IO)` | suspend 리스너가 은행 응답 뒤 **Netty 이벤트 루프 스레드**(은행 클라이언트에 하나뿐)에서 이어질 수 있다. 거기서 JDBC 를 기다리면 모든 은행 응답이 멈춘다 |
| 동시성 상한 | `Semaphore` 로 은행 동시 호출 수 제한 (기본 64) | 처리 중 메시지 수는 `asyncAcks` 의 poll 단위 정지로 최대 3 × 50 = 150 에 묶인다(문서에서 추론, Task 1 확인). 상한은 그와 별개로 **은행에 동시에 보내는 수**를 지킨다 (실제 은행도 기관별 동시 요청을 제한한다) |
| 측정 | 밀린 N건을 비우는 시간 (은행 장애 복구 뒤 쌓인 출금을 처리하는 상황) | DB 시각으로 계산해 API 기동 시간이 섞이지 않는다 |

## 3. 구조
```
transfer.consumer.mode = blocking (기본) | coroutine

 blocking  : TransferRequestedListener            fun on(record, ack)
               └ TransferProcessor.process(id)     = begin → bank.transfer() (.get()) → finish
 coroutine : SuspendingTransferRequestedListener  suspend fun on(record)
               ├ semaphore.withPermit {                                       퍼밋을 기다리는 동안 행은 RECEIVED
               │    withContext(db) { processor.begin(id) }                    RECEIVED → SENDING
               │    suspendBank.transfer(...) }                                await — 스레드를 잡지 않는다
               └ withContext(db) { processor.finish(id, outcome) }            결과 기록 · 판정 (퍼밋 밖)
```

Task 5 리뷰에서 순서를 바꿨다 — 퍼밋을 기다리는 동안 SENDING 이면 30초 뒤 정리 작업이 결과조회 · 환불한 뒤 송금될 수 있다 (CONFLICT). 퍼밋은 begin 의 짧은 DB 호출 하나를 함께 덮는다.

- **`modules/transfer`** (순수 Kotlin, 코루틴 라이브러리 의존 없음 — `suspend` 는 언어 기능이다)
  - `TransferProcessor.begin(id): Begin` — `Begin.Started(transfer)` · `Begin.NotFound` · `Begin.AlreadyHandled`. 지금 `process` 의 앞부분(조회 · `RECEIVED → SENDING` 조건부 전이) 그대로.
  - `TransferProcessor.finish(id, outcome): ProcessResult` — 지금 `process` 의 뒷부분(`record` · CONFLICT 판정) 그대로.
  - `process(id)` 는 `begin` → `bank.transfer` → `finish` 를 이어 부를 뿐이다. 동작이 같다는 근거는 **기존 `TransferProcessor` 테스트를 고치지 않고 통과**하는 것.
  - `SuspendBankTransferPort` (`suspend fun transfer(...)`: `BankTransferOutcome`) 를 `BankTransferPort` 옆에 둔다.
- **`apps/api`**
  - `config/SuspendBankTransferAdapter`: `client.transfer(...).await()`. 예외를 "보냈나 / 모르나"로 나누는 규칙은 블로킹 `BankTransferAdapter` 와 **한 함수**(`classifyBankFailure`)를 같이 쓴다. `CancellationException` 은 분류하지 않고 다시 던진다 (§4).
  - `messaging/SuspendingTransferRequestedListener`: 블로킹 리스너와 같은 토픽 · 같은 `@RetryableTopic` 설정 · 같은 그룹. `Acknowledgment` 를 받지 않는다 — 함수가 끝나면 프레임워크가 ack 하고, 예외를 던지면 오류 처리기(재시도 토픽)로 간다. DB 를 부를 디스패처(`db`, 기본 `Dispatchers.IO`)와 `Semaphore` 를 생성자로 받는다 (테스트가 바꿔 끼운다).
  - CONFLICT 경고 · `RETRY_LATER` → `RetryLaterException` · `@DltHandler` 의 분기 · 로그 · 알림은 공용 코드로 뽑아 두 리스너가 같이 쓴다. DLT 처리는 두 모드 모두 블로킹 그대로(드물고 짧다).
  - 두 리스너는 `@ConditionalOnProperty` 로 하나만 빈이 된다 (`blocking` 은 `matchIfMissing = true`).
- **의존성**: `kotlinx-coroutines-core`(`CompletionStage.await()`), `kotlinx-coroutines-reactor`(Spring 이 suspend 리스너를 부를 때 필요). WebFlux 는 넣지 않는다.
- **바꾸지 않는 것**: 웹, 정리 작업(결과조회는 블로킹 그대로), 1원인증, 파티션 수(3), `listener.concurrency`(3), `max-poll-records`(50), 은행 게이트웨이(단일 연결 · 이벤트 루프 1개).

## 4. 오류 처리 — 코루틴 모드도 블로킹과 같은 결과
| 상황 | 처리 |
|---|---|
| 은행 연결 없음 (`NotSent`) | `RECEIVED` 로 되돌리고 `RetryLaterException` → 재시도 토픽 (5s · 30s · 60s) → DLT (지금과 같다) |
| 응답 시간 초과 · 보낸 뒤 끊김 · 그 밖의 예외 | `UNKNOWN` → 정리 작업의 결과조회로 확정 (지금과 같다) |
| **코루틴 취소** (은행 응답을 기다리는 중 앱 종료 등) | 보냈는지 모른다. **아무것도 기록하지 않고** 취소를 다시 던진다. 출금은 `SENDING` 으로 남고, 정리 작업이 30초 뒤 `UNKNOWN` → 결과조회로 확정한다. **`NotSent` 로 분류해 다시 보내는 일은 없어야 한다** (이중 지급) |
| 처리 중 앱 종료 | ack 하지 않은 레코드는 재기동 뒤 다시 온다. 이미 `SENDING` 인 건은 `begin` 의 조건부 전이가 `AlreadyHandled` 로 걸러 은행에 다시 가지 않는다 (블로킹 모드 장애와 같은 길) |
| CONFLICT · DLT | 공용 코드 — 경고 · 알림이 같다 |

- `await()` 는 코루틴이 취소되면 은행 future 도 취소한다. 게이트웨이가 취소된 요청의 대기 표(`tranNo → future`) 항목을 남기지 않는지 계획 단계에서 확인한다 (남으면 늦은 응답이 올 때까지 메모리에 남는다).
- **알려 두는 차이**: 같은 파티션 안 메시지의 처리 순서를 지키지 않는다. 출금은 서로 독립이고 상태 전이가 조건부라 영향이 없다 (ADR 에 적는다).
- 문서상 비동기 리스너는 `DefaultErrorHandler` 에서 실패 레코드를 한 번 더 받는다. 우리는 `@RetryableTopic` 이라 해당하지 않지만, 재시도 토픽 컨테이너의 실제 호출 횟수를 Task 1 에서 센다.

## 5. 설정 (기본값)
```yaml
transfer:
  consumer:
    mode: blocking                    # blocking | coroutine. 그 밖의 값은 기동 실패
    coroutine:
      max-in-flight-bank-calls: 64    # 은행 동시 호출 상한, 1 ~ 1024 밖이면 기동 실패
```
모의 은행 실행 인자: `--response-delay=<n>ms` (0 ~ 2999ms, 은행 타임아웃 3초 미만. 기본 0). 모의 은행은 **동시에 응답을 기다리는 요청 수의 최댓값**을 세어 메서드로 돌려주고(테스트용), 종료할 때 로그로 남긴다(측정용).

## 6. 측정
### 측정 한 회 (`load/consumer/run.sh <mode> <지연ms> <회차> [상한] [hikari]`)
1. **정리 · 시드**: 측정 사용자(`load-p61-*`) · VERIFIED 계좌 · 잔고, `RECEIVED` 출금 N건 + 미발행 아웃박스 N행. 수취 계좌는 모의 은행에 있는 계좌(→ `COMPLETED`). 컨슈머 그룹 오프셋을 원래 토픽 · 재시도 토픽 모두 끝으로 옮겨 이전 회차의 메시지가 섞이지 않게 한다.
2. **발행만**: API 를 발행기 켬 · 리스너 끔으로 띄워 아웃박스 미발행이 0 이 되면 내린다.
3. **처리만**: 모의 은행을 그 지연으로 띄우고, API 를 발행기 끔 · 리스너 켬 · 그 모드로 띄워 N건이 모두 최종 상태가 되면 내린다.
4. **계산 (DB 시각)**: 처리량 = N ÷ (마지막 `updated_at` − 첫 `sending_at`). 건별 지연 = `updated_at − sending_at` 의 p50 · p95. 1초마다 Actuator `system.cpu.usage` · `process.cpu.usage` 와 `docker stats`(MySQL · Kafka)를 기록해 평균 · 최대를 남긴다.
5. **정확성**: N건 모두 `COMPLETED`, 모의 은행 거래 기록에 거래번호마다 정확히 한 번 (은행에 두 번 간 출금 없음), 잔고 = 원장 합. 하나라도 어긋나면 결과 파일을 `.invalid` 로 바꾼다 (P4-2 와 같다).

측정 API 는 다른 스케줄(정리 작업 · 일마감 대사 · 보존 배치)을 모두 끄고, 모의 은행은 **별도 `--bank-files` 디렉터리**를 쓴다 — 개발용 오늘 거래 기록에 측정 출금 수천 건이 섞이면 다음 날 01시 대사가 그 줄들을 `UNKNOWN_REFERENCE` 로 쏟아낸다. IntelliJ 등으로 띄운 다른 API 인스턴스는 끈다 (같은 그룹으로 메시지를 나눠 가진다).

### 비교 (24회 + 대조 3회)
| | 값 |
|---|---|
| 모드 | blocking / coroutine (상한 64) |
| 은행 지연 | 0 / 20 / 100 / 500 ms |
| 회차 | 칸마다 3회, 지표마다 중앙값 |
| 대조 실험 | coroutine · **상한 3** · 100ms · 3회 — 컨슈머 스레드 3개와 같은 동시성이면 blocking 과 비슷해야 한다. 차이를 만든 것이 "코루틴이라서"가 아니라 **동시에 은행을 기다릴 수 있는 수**임을 보인다 (리틀의 법칙: 처리량 = 동시성 ÷ 지연) |
| N | 칸마다 20초 이상 걸리게: 기본 5,000, blocking 100ms 1,500, blocking 500ms 400 (예상 처리량 = 3 ÷ 지연). 첫 회차로 보정하면 문서에 적는다 |

### 용량 탐색 (coroutine · 지연 100ms · N 20,000)
1. Hikari 20 에서 은행 동시 호출 상한 16 → 32 → 64 → 128 → 256.
2. 가장 좋았던 상한에서 Hikari 10 / 20 / 40.
3. 멈춤: 시스템 CPU 평균 80% 도달 · 오류 · 처리량 증가 5% 미만 중 하나.
4. blocking 은 탐색하지 않는다 — 파티션 수(3)에서 막히는 것 자체가 결과다 (한 줄로 적는다).

### 결과 문서 `docs/perf/p6-1-consumer-coroutines.md` (사용자 요청 — 문서만 보고 분석할 수 있게)
- **무엇을 왜 재는가**: 컨슈머가 막히는 지점, 왜 웹이 아니라 컨슈머인가, 왜 "밀린 N건 비우기" 인가.
- **다이어그램 (Mermaid, GitHub 에서 바로 그려진다)**: 블로킹과 코루틴이 은행을 기다리는 모습 (시퀀스 · 시간축), 측정 한 회의 순서도, 스레드 · 디스패처 · 이벤트 루프 관계도, 예상(리틀의 법칙) 대 실제 처리량 그래프 (`xychart` — GitHub 가 그리지 못하면 표로 대신).
- **나타날 수 있는 현상과 읽는 법**: 지연 0ms 에서 두 모드가 비슷함(DB 가 병목), 상한을 키우면 다음 한계가 DB 로 넘어감(`Dispatchers.IO` 64 스레드 · Hikari 20), poll 묶음 50건 중 가장 늦은 건이 다음 묶음을 붙잡음, CPU 가 먼저 차면 부하를 만드는 쪽 · 인프라도 같은 CPU 를 쓰는 탓일 수 있음.
- **조건 · 결과 표 · 해석 · 한계**: P4-2 문서와 같은 형식. 표 위에 "이 PC 기준 상대 비교이며 운영 수치가 아니다" 를 적는다 (k6 · API · MySQL · Kafka · 모의 은행이 한 CPU 를 나눠 쓴다, 인스턴스 하나).
- 원자료 `docs/perf/raw/p6-1/` (P4-2 와 같은 JSON 형식).

## 7. 테스트
1. **Task 1 — 실험 (결과에 따라 진행)**: Testcontainers(Kafka · MySQL)로 확인해 테스트로 남긴다.
   - suspend 리스너에서 던진 예외가 `@RetryableTopic` 의 재시도 토픽 → DLT 로 가는가, 재시도 토픽 컨테이너의 호출 횟수.
   - poll 한 묶음이 다 끝나기 전에 다음 묶음이 오지 않는가 (처리 중 최대 수).
   - suspend 리스너가 future 를 완료한 스레드(은행 이벤트 루프)에서 이어지는가 — `withContext` 가 필요한 근거.
   - **재시도 토픽이 suspend 리스너에서 동작하지 않으면 구현을 멈추고 선택지를 들고 사용자와 다시 정한다.**
2. **`TransferProcessor`**: 기존 테스트는 고치지 않고 통과. `begin` · `finish` 테스트 추가.
3. **`SuspendBankTransferAdapter`**: 완료 · 거절 · 미전송 · 시간 초과 · 취소(다시 던짐, `NotSent` 아님).
4. **스레드**: 가짜 은행이 이름 붙인 스레드에서 future 를 완료할 때 `begin` · `finish` 의 DB 호출이 그 스레드가 아닌 곳에서 도는가.
5. **상한**: 상한 K 에서 동시에 걸린 은행 호출이 K 를 넘지 않는다.
6. **흐름 (coroutine 모드, Testcontainers Kafka · MySQL + 모의 은행)**: 완료, 거절 + 환불, 연결 없음 → 재시도 토픽 → 복구, 시간 초과 → `UNKNOWN`, DLT. 모의 은행 지연을 넣고 30건 → 모의 은행의 최대 동시 대기 수가 3 을 넘는다. 거래번호마다 은행에 한 번.
7. **설정**: 모드 기본값 `blocking` · 잘못된 값과 상한 범위 밖은 기동 실패 · 모드마다 리스너 빈이 하나.
8. **변이 검사** (커밋하지 않고 되돌린다): `withContext(db)` 제거, 취소를 `NotSent` 로 분류, `Semaphore` 제거 — 셋 모두 테스트가 잡아야 한다.

## 8. 위험과 한계
- 재시도 토픽 · 비동기 반환 조합은 문서에 명시돼 있지 않다 (Task 1 의 진행 조건).
- 처리 순서: 같은 파티션 안에서 섞인다 (§4).
- 측정은 한 PC · 인스턴스 하나다. 절대 수치는 운영으로 옮기지 못하고, 서버 여러 대의 효과(인스턴스 사이 경합 · 네트워크 · 로드밸런서)는 재지 않는다. 평소처럼 "초당 몇 건씩 도착하는" 요청 모양(열린 모델)은 웹 비교(P6-2 · P7)에서 다룬다.
- 은행 게이트웨이는 단일 연결 · 이벤트 루프 1개다. 상한을 크게 키우면 그 한 회선이 병목일 수 있고, 측정이 그것을 보여 줄 수 있다 (게이트웨이 구조는 바꾸지 않는다).
- 코루틴 모드는 기본값이 아니다. 운영 기본을 바꿀지는 ADR-0012 에 판단만 남긴다.

## 9. 결정 기록 · 문서
- ADR-0012 (제안 — 머지 때 채택으로): 방식 · `withContext` 근거 · 상한 · 취소 처리 · 순서 차이 · 측정 결과 요약.
- `docs/perf/p6-1-consumer-coroutines.md` + `docs/perf/raw/p6-1/` (§6).
- OPERATION: 모드 전환 · 측정 절차(새 절). ARCHITECTURE · README(로드맵 P6-1 · 기술 스택에 코루틴) · HANDOVER · CHANGELOG · SETUP(테스트 수).
- 실행 구성: `7. API (코루틴 컨슈머)` (`--transfer.consumer.mode=coroutine`).
- 시작 전에 `main` 의 지금 커밋에 태그 `p5-done` (원격에 올리는 것은 사용자 확인 뒤).
