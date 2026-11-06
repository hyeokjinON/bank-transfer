# P6-2 웹 1원인증 경로 WebFlux 비교 — 설계

> 상태: 설계 승인 (2026-11-05) · 다음: 구현 계획 · 결정 기록: ADR-0013 (구현과 함께 작성)
> 앞 단계: [P6-1 설계](2026-11-04-p6-1-consumer-coroutines-design.md) (suspend · `withContext(IO)` · 진행 조건 실험 · 측정 격리), [ADR-0012](../../adr/0012-consumer-coroutines.md), [P4-2 설계](2026-10-27-p4-2-lock-comparison-design.md) (전략 공존 · k6 측정 도구), [ADR-0001](../../adr/0001-modular-monolith-ports.md) (모듈러 모놀리스 · 포트-어댑터)

## 1. 목적과 범위
웹 요청 중 **은행을 기다리는 곳은 1원인증 요청(`POST /api/v1/account-verifications`) 하나**다. 성명조회 → 1원 이체로 은행을 두 번 기다린다 (코드 확인됨). 출금 접수(`POST /api/v1/transfers`)는 DB 트랜잭션 하나로 202 를 돌려주고 은행을 기다리지 않는다 — WebFlux 로 옮겨도 JDBC 를 기다리는 것은 같아 비교 대상에서 뺀다.

지금의 1원인증은 이미 `CompletableFuture` 를 돌려주는 **서블릿 비동기**라, 은행을 기다리는 동안 Tomcat 스레드를 놓아 준다. 그래서 P6-2 는 네 판을 같은 PC · 같은 도구로 비교한다.

| 판 | 앱 | 은행을 기다리는 방법 |
|---|---|---|
| MVC 블로킹 (대조군) | `apps/api` | 컨트롤러가 `join()` — Tomcat 스레드가 끝까지 기다린다 |
| MVC 비동기 (지금 기본) | `apps/api` | `CompletableFuture` 반환 (서블릿 비동기) |
| WebFlux 코루틴 | `apps/api-reactive` (신규) | suspend 컨트롤러, `await()` |
| WebFlux Reactor | `apps/api-reactive` | `Mono.fromFuture` 연산자 체인 |

- **가설 (추정)**: 처리량의 차이는 프레임워크가 아니라 "은행을 기다리는 동안 스레드를 잡는가"에서 난다. MVC 블로킹은 대략 `Tomcat 스레드 200 ÷ 2D` (D = 은행 지연, 호출 두 번)에서 막히고, 비동기 세 판은 비슷하다. 남는 차이는 스레드 수 · 지연 분포 · 끊김(취소) 동작. P6-1 의 결론("빠른 것은 코루틴이 아니라 동시에 기다릴 수 있는 수")을 웹에서 확인하는 단계다.
- **지우지 않는다**: 지금의 MVC 비동기가 기본이다. 운영 기본은 바꾸지 않는다 (§8).
- **범위 밖**: R2DBC, 출금 접수의 WebFlux 판, 리액티브 앱의 `/confirm` · 그 밖의 API, WebClient, Reactor Kafka, 가상 스레드(P7 — 같은 측정 도구를 다시 쓴다), 여러 인스턴스, 꼬리 지연이 있는 은행, 1원인증 동시 호출 상한, `NonCancellable`, CI 에서의 측정(CI 는 정확성 테스트만).

## 2. 결정 요약
| 결정 | 내용 | 이유 |
|---|---|---|
| 비교 경로 | 1원인증 요청만 | 웹에서 은행을 기다리는 유일한 경로. 측정 대상이 하나라 결과가 선명하다 |
| 앱 | WebFlux 는 별도 앱 `apps/api-reactive` | 한 앱에 MVC 와 WebFlux 를 함께 두면 Spring 은 MVC 를 고른다. 새 앱은 1원인증 요청 하나만 한다 (Kafka · 스케줄러 · 배치 · Flyway 끔) |
| 저장소 | JPA 엔티티 · `JpaVerificationRepository` 를 새 모듈 `modules/account-infra` 로 옮겨 두 앱이 같이 쓴다 | 같은 테이블을 다루는 코드가 두 벌이 되지 않는다. 테이블 소유(Flyway)는 `apps/api` 그대로 |
| DB 호출 | JDBC 를 이벤트 루프 밖에서 — 코루틴 `withContext(io)`, Reactor `publishOn(boundedElastic())` | 이벤트 루프(HTTP · 은행 게이트웨이)에서 JDBC 를 기다리면 그 루프의 모든 처리가 멈춘다 (P6-1 실험 3) |
| 판정 | `modules/account` 의 순수 함수(`checkHolder` · `newDraft` · `issue`)를 네 판이 같이 쓴다. 지금의 `request()` 도 이 함수로 다시 짠다 | 판정을 복사하지 않는다 (P6-1 의 begin/finish 와 같은 원칙). 실행 위치는 앱의 몫이라 account 모듈에 코루틴 · Reactor 의존이 들어가지 않는다 |
| 고르는 법 | `account.verification.web-mode` — `apps/api`: `async`(기본) \| `blocking`, `apps/api-reactive`: `coroutine`(기본) \| `reactor`. 컨트롤러는 판마다 하나, `@ConditionalOnProperty` 로 하나만 빈 | P4-2 · P6-1 과 같은 기동 설정 방식. 그 밖의 값은 기동 실패, 지금 값은 `/actuator/info` |
| 끊김 | 클라이언트가 끊으면 취소되게 둔다 (재요청으로 회복). 1원 이체 대기 중 끊김은 경고 로그 | 응답을 못 받은 사용자는 인증 건 id 를 몰라 저장해도 확인할 수 없다. 대가는 최악에 1원 두 번 — 출금과 달리 돈을 잃지 않는다 |
| 오류 응답 | 웹 계층 코드는 공유하지 않고 **계약 테스트**(기대표 파일 하나를 두 앱 테스트가 읽음)로 같은 상태 · `code` 를 지킨다 | MVC · WebFlux 의 API 가 달라 공용 웹 모듈은 크다. 갈라지면 테스트가 깨진다 |
| 측정 | 열린 모델 (k6 `constant-arrival-rate`, 초당 R건 도착) | 웹 요청이 실제로 오는 모양. P6-1 의 "밀린 N건 비우기"와 다르다 |

## 3. 구조

### 3-1. 모듈
```
modules/account          (수정) 판정 순수 함수 — VerificationRequestRules(가칭): checkHolder · newDraft · issue
                                AccountVerificationService.request() 는 이 함수들로 다시 짠다 (동작 · 기존 테스트 그대로)
modules/account-infra    (신규) account 도메인의 바깥 어댑터 — 두 앱이 같이 쓴다
                                · AccountVerificationEntity · AccountVerificationJpaRepository · JpaVerificationRepository  ← apps/api 에서 이동
                                · 은행 응답 해석 holderNameOf · requireOk · BankRejectedException                    ← apps/api 에서 이동
apps/api                 (수정) MVC. web-mode = async(기본) | blocking
apps/api-reactive        (신규) WebFlux (Reactor Netty). POST /api/v1/account-verifications 하나. web-mode = coroutine(기본) | reactor
```

- `modules/account-infra` 는 Spring Data JPA · 게이트웨이(`modules/bank-gateway`) · 프로토콜 응답 코드에 의존한다. 도메인 모듈(`account`)은 여전히 Spring · Netty 의존이 없다 (ADR-0001).
- 1원인증 이체의 이용기관 거래번호(`V` 접두, 저장 안 함)는 지금과 같은 생성기를 두 앱이 쓴다.
- 새 앱도 자기 게이트웨이 클라이언트로 모의 은행에 TCP 로 붙는다 (모의 은행은 여러 클라이언트를 받는다 — `DefaultChannelGroup`, 코드 확인됨). 점검 시간 전문 차단(`sendGate`) · 웹 점검 가드(503 + `Retry-After`)를 같은 방식으로 넣는다.
- 두 앱의 Hikari 최대 풀은 같은 20 (풀이 다르면 측정이 풀 크기를 재게 된다, P6-1 결과 문서 §7-2).
- 개발용 포트: `apps/api` 8080 그대로, `apps/api-reactive` 8081. 측정용은 §6.

### 3-2. 판정 함수 (`modules/account`)
| 함수 | 하는 일 |
|---|---|
| `checkHolder(found: String?, input: String): String` | `null` → `AccountNotFound`, 공백을 뺀 이름이 다르면 `HolderNameMismatch`, 같으면 은행이 준 예금주명 |
| `newDraft(): VerificationDraft` | 인증 건 id · 4자리 코드(`SecureRandom`) · 코드 해시 · 적요(`BT` + 코드) |
| `issue(draft, command, holderName): AccountVerification` | 저장할 인증 건 (`SENT`, 실패 0, 만료 = 지금 + ttl) |

### 3-3. 네 판의 흐름
```
MVC 비동기 (지금)   bank.inquireHolderName ─thenCompose→ checkHolder · newDraft ─→ bank.transfer ─thenApply→ repository.save(issue(...))
                    (게이트웨이 어댑터가 thenApplyAsync(앱 실행기)로 넘겨 저장은 앱 실행기 스레드)
MVC 블로킹 (대조)   위와 같은 서비스를 컨트롤러가 join() — Tomcat 스레드가 끝까지 기다린다

WebFlux 코루틴      suspend fun request(cmd):
                      found  = holderNameOf(client.inquireName(...).await())      ← 스레드를 잡지 않는다
                      holder = checkHolder(found, cmd.holderName); draft = newDraft()
                      requireOk(client.transfer(..., 1, draft.memo, ref).await())
                      withContext(io) { repository.save(issue(draft, cmd, holder)) }   ← JDBC 는 IO 스레드
WebFlux Reactor     Mono.fromFuture { client.inquireName(...) }.map(::holderNameOf)
                      .map { found -> checkHolder(found, cmd.holderName) to newDraft() }        ← (holder, draft)
                      .flatMap { (holder, d) -> Mono.fromFuture { client.transfer(...) }.map(::requireOk).thenReturn(holder to d) }
                      .publishOn(Schedulers.boundedElastic())                                   ← 이 아래는 블로킹 허용 스레드
                      .map { (holder, d) -> repository.save(issue(d, cmd, holder)) }
```
- 은행 동시 호출 상한은 어느 판에도 두지 않는다 (지금 1원인증에 없다 — 한 판에만 두면 공정하지 않다). 게이트웨이 응답 시간 초과 3초는 모든 판에 같다.
- 판 사이에 일부러 남기는 차이는 블로킹 대조군의 `join()` 하나뿐이다.
- 코루틴 판의 `io` 디스패처는 생성자로 받는다 (기본 `Dispatchers.IO`, 테스트에서 바꿀 수 있다).

## 4. 오류 · 취소 · 스레드

### 4-1. 진행 조건 실험 (구현 전, 진짜 모의 은행 — 통과해야 다음 Task)
| # | 질문 | 예상 (추정) | 예상과 다르면 |
|---|---|---|---|
| E1 | WebFlux suspend 컨트롤러에서 `await` 뒤 코드는 어느 스레드에서 이어지는가 | 응답을 완료한 은행 게이트웨이 루프 (`Unconfined`) | `withContext(io)` 는 그대로, 설명 · 스레드 테스트 기대값을 실제에 맞춘다 |
| E2 | 응답 전에 클라이언트가 끊으면 코루틴이 취소되는가 · Reactor 판은 구독이 취소되는가 · MVC 비동기는? | 둘 다 취소, MVC 비동기는 끝까지 진행 · 저장 | §4-3 표를 실제 동작으로 고친다 |
| E3 | 취소될 때 게이트웨이 future 도 취소되고 대기 표 항목이 응답 시간 초과 뒤 지워지는가 (`await` · `Mono.fromFuture`) | 둘 다 future 를 취소 | 대기 표가 새는 문제 — 취소를 직접 연결한다 |
| E4 | 은행 시간 초과 · 연결 없음이 `CompletionException` 에 싸이지 않고 원래 예외로 오는가 | `await` · `Mono.fromFuture` 모두 벗겨서 준다 | P6-1 처럼 한 겹 벗긴다 |

### 4-2. 스레드 규칙
- JDBC 는 두 이벤트 루프(HTTP 를 받는 Reactor Netty 루프, 은행 게이트웨이 루프) 어디에서도 돌지 않는다.
- 판마다 저장 시점의 스레드 이름을 기록해 이벤트 루프가 아님을 확인하는 테스트를 둔다. 변이 검사: `withContext` · `publishOn` 을 지우면 실패해야 한다.

### 4-3. 끊김 (WebFlux 두 판)
| 끊긴 시점 | 은행 | DB | 처리 |
|---|---|---|---|
| 성명조회 대기 | 조회만 | 없음 | 그대로 취소 |
| 1원 이체 대기 | 1원이 **나갔을 수 있음** | 없음 | 그대로 취소 + 경고 로그 (단계 `transfer` · 은행코드 · 마스킹한 계좌). 사용자는 다시 요청 |
| 저장 중 | 나감 | JDBC 가 끝까지 돌아 저장됨 (추정, E2 로 확인) | 응답만 못 받음. 남은 `SENT` 행은 만료 30일 뒤 보존 배치가 지운다 (P5-2) |

- `CancellationException` 은 오류 응답으로 바꾸지 않고 다시 던진다 (끊긴 연결이라 보낼 곳이 없고, 삼키면 구조적 동시성이 깨진다).
- MVC 판은 끊겨도 지금처럼 끝까지 진행한다. 이 차이를 E2 · ADR 에 남긴다.

### 4-4. 오류 응답 (두 앱이 같아야 한다 — 계약 테스트)
| 상황 | 상태 · `code` |
|---|---|
| `X-User-Id` 없음 · 형식 틀림 | 400 `USER_ID_REQUIRED` (본문 검증보다 먼저) |
| 본문 검증 실패 | 400 (지금 MVC 와 같은 `code`) |
| 계좌 없음 | 404 `ACCOUNT_NOT_FOUND` |
| 예금주 불일치 | 422 `HOLDER_NAME_MISMATCH` |
| 은행 거절 | 502 `BANK_REJECTED` |
| 연결 없음 | 503 `BANK_UNAVAILABLE` |
| 점검 시간 | 503 `BANK_MAINTENANCE` + `Retry-After` |
| 은행 응답 3초 초과 | 504 `BANK_TIMEOUT` |
| 그 밖 | 500 `INTERNAL_ERROR` (메시지 숨김) |

응답에는 인증코드를 절대 넣지 않는다 (지금과 같음).

## 5. 설정 (기본값)
| 앱 | 키 | 기본 | 허용 |
|---|---|---|---|
| `apps/api` | `account.verification.web-mode` | `async` | `async` \| `blocking` (소문자 그대로, 그 밖은 기동 실패) |
| `apps/api-reactive` | `account.verification.web-mode` | `coroutine` | `coroutine` \| `reactor` |
| `apps/api-reactive` | `server.port` | 8081 | |
| 둘 다 | `spring.datasource.hikari.maximum-pool-size` | 20 | |

`/actuator/info` 에 두 앱 모두 `web-mode` 와 Hikari 최대 풀을 보인다 (측정 사전 확인이 읽는다).

## 6. 측정
### 측정 칸
| 구분 | 내용 | 횟수 |
|---|---|---|
| 본 측정 | 4판 × 은행 지연 D 100 · 500ms × 도착률 R 100 · 200 · 400건/s, 칸마다 3회 중앙값 | 72회 |
| 원인 확인 (대조) | MVC 블로킹, Tomcat 최대 스레드 200 → 400, D 500 · R 400 — 천장이 스레드 수를 따라 움직이는지 | 3회 |
| 한계 탐색 | 비동기 세 판, D 500, R 800 → 1600 → … 성공률 99% 미만 · 201 의 p99 3초 초과 · 시스템 CPU 80% 중 하나에 닿으면 멈춤 | 판마다 몇 회 |

- 회차 하나 = 워밍업 10초 + 측정 30초. 본 측정은 앱 재기동 포함 약 1시간 반 (추정).
- D 500 을 넣은 이유: D 100 이면 블로킹 천장이 약 1,000건/s 라 이 PC 의 다른 한계가 먼저 올 수 있다. D 500 이면 약 200건/s 라 R 400 에서 차이가 드러난다 (추정).

### 한 회차에 기록하는 것
- 201 처리량, 201 의 p50 · p95 · p99 (실패는 빨리 끝나 섞이면 백분위가 좋아 보인다 — P4-2 와 같음)
- 실패 상태 코드별 수, k6 `dropped_iterations` (0 이 아니면 부하 발생기 한계로 표시)
- 측정 구간 `jvm.threads.live` 최댓값 (1초마다 액추에이터), 앱 프로세스 CPU, 시스템 CPU
- **정확성**: 회차의 201 수 = DB 에 생긴 측정 사용자의 `SENT` 행 수 = 모의 은행이 받은 1원 이체 수. 1원 이체가 더 많으면 끊김이 생긴 것 — 회차 무효, 원인 기록. k6 요청 시간 초과는 10초 (3초 × 2 보다 길어 정상이면 k6 가 먼저 끊지 않는다)

### 격리 (P6-1 첫 시험 실행 사고에서)
- 측정용 모의 은행 9190 (`--response-delay`), 측정용 앱 포트 18080(`apps/api`) · 18081(`apps/api-reactive`). 개발용 API(8080) · 개발용 은행(9090)과 겹치지 않는다.
- `apps/api` 측정 인스턴스는 Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존 스케줄을 끈 구성으로 띄운다 (개발용 컨슈머 그룹 메시지를 나눠 갖지 않게). 꺼진 상태는 시작 전에 확인한다.
- 측정 사용자는 `p62-` 접두, 계좌는 모의 은행 기본 계좌. `cleanup` 은 측정 사용자의 1원인증 행만 지운다.
- 시작 전 읽기 전용 사전 확인: 측정 포트 · 측정 은행 포트에 다른 프로세스가 없는지, `/actuator/info` 의 `web-mode` · 풀이 회차 인자와 같은지, 측정 사용자 행이 비어 있는지. 하나라도 어긋나면 시작하지 않는다.
- 도구: `load/verification/` (`run.sh` · k6 스크립트 · `summarize.sh` · SQL). 절차는 OPERATION §12.

### 결과 문서 `docs/perf/p6-2-web-reactive.md`
P6-1 결과 문서와 같은 짜임: 테스트 방법 · 왜 이렇게 쟀나 · 나올 수 있는 현상 · 결과표 · 그림 · 확인 / 추정 구분 · 원자료(회차별 요약 파일).

### 판단 기준 (미리 정함)
- 비동기 세 판의 처리량 차이가 5% 안이면 "같다"고 쓰고, 차이는 스레드 수 · 지연 분포로 설명한다.
- 운영 기본은 MVC 비동기 그대로. WebFlux 앱은 비교용으로 남긴다 (ADR-0013).

## 7. 테스트
| 위치 | 테스트 |
|---|---|
| `modules/account` | 판정 함수 단위 (공백 무시 · 없음 · 불일치 · 적요 접두 · 해시). **기존 `AccountVerificationServiceTest` 는 고치지 않고 통과** |
| `modules/account-infra` | `apps/api` 에서 옮겨 오는 JPA 저장소 테스트 (Testcontainers MySQL), 은행 응답 해석 단위 |
| `apps/api` | `web-mode` 두 값 각각 201 · 행 저장, 잘못된 값은 기동 실패, 기존 API 통합 테스트 그대로 통과 |
| `apps/api-reactive` | 진행 조건 실험 E1 ~ E4. 판(코루틴 · Reactor)마다 성공 · 계좌 없음 · 불일치 · 거절 · 시간 초과 · 연결 없음 · 점검 · 저장 스레드 이름. 끊김: 모의 은행 지연 중 연결을 끊으면 행 없음 · 경고 로그 · 게이트웨이 대기 표 정리 |
| 두 앱 공통 | 오류 계약 테스트 — 기대표 파일 하나(요청 → 상태 · `code`)를 두 앱 테스트가 읽는다 |

- **변이 검사** (커밋하지 않고 되돌림): `withContext(io)` 제거 · `publishOn` 제거 · 취소를 오류 응답으로 바꾸기 · 블로킹 컨트롤러의 `join()` 을 future 반환으로 — 모두 테스트가 잡아야 한다.
- 실행 구성 추가: `8. API 리액티브 (코루틴)`, `9. API 리액티브 (Reactor)`, `10. API (1원인증 블로킹)`. `http/account-verification.http` · Postman 에 리액티브 앱 주소 변수.
- **손 확인** (머지 전): 리액티브 앱에 1원인증 요청 → 201 → 모의 은행 콘솔의 코드 → `apps/api` `/confirm` → `VERIFIED` → 그 계좌로 출금 → `COMPLETED`. 끊김: 모의 은행 지연 2초에서 `curl --max-time 1` → 행 없음 · 경고 로그.

## 8. 위험과 한계
- 한 PC · 인스턴스 하나의 상대 비교다. 절대 수치는 운영으로 옮기지 못한다.
- 모의 은행은 고정 지연이다. 꼬리 지연이 있는 은행에서는 재지 않는다.
- WebFlux 판의 DB 는 여전히 블로킹 JDBC(IO · boundedElastic 스레드)다. "완전한 논블로킹"이 아니며, R2DBC 비교는 범위 밖이다.
- 1원 이체 대기 중 끊김은 1원을 한 번 더 쓰게 할 수 있다 (§4-3, 받아들인 대가).
- 운영 기본을 바꾸지 않는 이유: 비동기 세 판이 비슷할 것으로 예상되고, 별도 앱은 배포 · 관측 단위가 하나 더 생긴다. 측정 결과가 예상과 크게 다르면 ADR-0013 에 다시 판단한다.

## 9. 결정 기록 · 문서
- **ADR-0013** "웹 1원인증 경로의 WebFlux 비교" (제안 → 머지 후 채택): 진행 조건 실험 결과, 취소 정책, 운영 기본 유지 판단
- 결과 문서 `docs/perf/p6-2-web-reactive.md`
- OPERATION §12 (측정 절차), ARCHITECTURE (모듈 두 개), README (로드맵 P6-2 · 테스트 수), SETUP (포트 8081), HANDOVER, CHANGELOG
- 용어 정리: 저장소 문서의 "퍼밋"을 "은행 호출 자리(세마포어 permit)"로 바꾼다 — ADR-0012, P6-1 결과 문서, HANDOVER, 코루틴 리스너 주석 (velog #11 은 2026-11-05 에 바꿨다)
- 브랜치 `feat/p6-2-web-reactive`, PR 하나. 머지 후 정리 PR, velog #12 (비공개)
