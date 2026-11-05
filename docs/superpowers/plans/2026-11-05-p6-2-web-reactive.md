# P6-2 웹 1원인증 경로 WebFlux 비교 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 1원인증 요청(`POST /api/v1/account-verifications`)을 네 판(MVC 블로킹 · MVC 비동기 · WebFlux 코루틴 · WebFlux Reactor)으로 만들어 설정으로 고르게 하고, 같은 PC · 같은 도구(k6 열린 모델)로 처리량 · 지연 · 스레드 수를 비교한다.

**Architecture:** 판정은 `modules/account` 의 순수 함수(`VerificationRequestRules`)에 두고, 바깥 어댑터(JPA 저장소 · 은행 응답 해석)는 새 모듈 `modules/account-infra` 에 한 벌만 둔다. MVC 두 판은 `apps/api` 안에서 컨트롤러 둘(`@ConditionalOnProperty`)로, WebFlux 두 판은 새 앱 `apps/api-reactive` 안에서 흐름 둘(`CoroutineVerificationFlow` · `ReactorVerificationFlow`)과 컨트롤러 둘로 나뉜다. JDBC 는 어느 판에서도 이벤트 루프에서 돌지 않는다(코루틴 `withContext(io)`, Reactor `publishOn(boundedElastic())`). 측정은 `load/verification/run.sh` 가 측정용 모의 은행(9190) · 측정용 포트(18080 · 18081)로 앱을 띄우고 k6 `constant-arrival-rate` 로 잰 뒤 DB · 은행 거래 기록과 맞춰 본다.

**Tech Stack:** Kotlin 2.3.21 · Java 21 · Spring Boot 4.1.1 (Spring 7.0.9 — WebMVC · WebFlux · Data JPA) · Reactor Core 3.8.7 · Reactor Netty (Boot BOM) · kotlinx-coroutines 1.10.2 (`-core`, `-reactor`) · Netty 4.2 · Hibernate 7 · MySQL 8.4 · Testcontainers 2.0.5 · JUnit 5 · k6 2.3.0 (Docker) · Gradle 9 Kotlin DSL

**Spec:** [docs/superpowers/specs/2026-11-05-p6-2-web-reactive-design.md](../specs/2026-11-05-p6-2-web-reactive-design.md) (승인 2026-11-05. 앞 단계: [P6-1 설계](../specs/2026-11-04-p6-1-consumer-coroutines-design.md) · [P6-1 계획](2026-11-04-p6-1-consumer-coroutines.md) · [ADR-0012](../../adr/0012-consumer-coroutines.md). 결정 기록은 ADR-0013 — Task 9 에서 작성)

## 스펙과 다른 점 / 확인 필요

승인된 결정은 바꾸지 않는다. 아래는 코드 · jar 를 확인하다 드러난 스펙의 빈틈과 이 계획의 해결책이다 — 리뷰에서 확인해 달라.

1. **JPA 저장소 테스트(`JpaVerificationRepositoryTest`)는 `apps/api` 에 남긴다 (스펙 §7 표는 "account-infra 로 옮김").** 테이블을 만드는 Flyway 마이그레이션이 `apps/api/src/main/resources/db/migration` 에 있고, 그 테스트는 공유 통합 테스트 컨텍스트(`ApiIntegrationTest`)를 쓴다. account-infra 에 따로 컨테이너 · 스키마를 세우면 마이그레이션을 복사하거나 다른 모듈의 리소스를 읽어야 한다. 테스트는 그대로 두고 import 만 바꾼다. account-infra 의 테스트는 은행 응답 해석 단위 테스트다.
2. **`BankMaintenanceGuard` · `BankMaintenanceException` 은 `apps/api/web` → `modules/bank-gateway` 로 옮긴다 (스펙 §3-1 에 없음).** 새 앱도 같은 가드(점검이면 503 + `Retry-After`)를 써야 하는데, 가드는 `BankMaintenanceWindow`(이미 bank-gateway) 와 `Clock` 에만 의존한다. 복사하지 않는다. 패키지가 `io.github.hyeokjinon.banktransfer.gateway` 로 바뀌어 `apps/api` 의 import 를 고친다.
3. **공유 패키지의 엔티티 · 저장소는 `@AutoConfigurationPackage(basePackages = ["io.github.hyeokjinon.banktransfer.account.infra"])` 로 등록한다.** 두 앱의 `@SpringBootApplication` 패키지(`…api` · `…reactive`) 밖이라 JPA 엔티티 스캔 · 저장소 스캔에 걸리지 않는다. Boot 4 의 `AutoConfigurationPackage` 는 `spring-boot-autoconfigure-4.1.1.jar` 에 있음을 확인했다 (`EntityScan` 은 Boot 4 에서 위치가 바뀌어 쓰지 않는다).
4. **오류 계약은 기대표를 `modules/account-infra` 의 test fixtures(`java-test-fixtures` 플러그인)에 Kotlin 객체로 둔다.** 스펙은 "기대표 파일 하나를 두 앱 테스트가 읽는다". 파일 경로를 상대 경로로 읽으면 Gradle 테스트 작업 디렉터리에 기대게 된다 — test fixtures 는 두 앱의 테스트 클래스패스에 같이 올라간다. 표는 두 갈래다: ① HTTP 로 재현할 수 있는 경우(헤더 · 본문 · 계좌 없음 · 불일치 · 시간 초과 · 성공 모양), ② 예외 → 상태 · `code` (은행 거절 · 연결 없음 · 점검 · 그 밖 — 모의 은행으로 HTTP 재현이 어렵거나 시계가 필요한 경우). ②는 각 앱의 예외 처리기를 직접 불러 확인한다.
5. **정확성 규칙을 k6 시간 초과까지 포함해 다듬는다 (스펙 §6 "1원 이체가 더 많으면 무효").** MVC 블로킹이 천장을 넘는 칸(D 500 · R 400)은 Tomcat 큐 대기가 k6 시간 초과(10초)를 넘는 것이 **예상된 결과**다. 이때 k6 는 연결을 끊고 — MVC 는 끝까지 진행해 행을 저장하고(201 수 < 행 수), WebFlux 는 취소돼 1원만 나갈 수 있다(행 수 < 1원 수). 그래서 회차 판정은 이렇게 한다. **시간 초과 0 이면** `201 수 = 행 수 = 1원 수` (스펙 그대로). **시간 초과 T > 0 이면** `0 ≤ 행 수 − 201 수 ≤ T` 이고 `0 ≤ 1원 수 − 행 수 ≤ T` 일 때만 유효로 남기고, 결과 파일에 `timeouts` · `rows_minus_created` · `one_won_minus_rows` 를 적는다 (결과 문서가 "끊김 비용"으로 읽는다). 그 밖은 `.invalid`.
6. **`dropped_iterations` 는 두 원인을 나눠 적는다.** 앱이 느려 VU 가 모자란 경우(`vus_max` = `maxVUs` 에 닿음)와 k6 자체 한계. 결과 파일에 `vus_max` 를 같이 남기고, 결과 문서에서 앱 쪽 포화로 읽는다 (스펙 §6 "0 이 아니면 부하 발생기 한계로 표시" 를 세분).
7. **모의 은행의 입금 기록을 `CopyOnWriteArrayList` → `ConcurrentLinkedQueue` 로 바꾼다 (계획에서 발견).** 지금은 입금마다 계좌의 목록 전체를 복사한다(O(n)). 측정은 회차마다 계좌 세 개에 최대 수만 건이 들어와 복사가 O(n²)로 커지고, 그 복사가 모의 은행 이벤트 루프에서 돈다(추정: 측정 결과를 흔들 수 있다). 순서 · 테스트용 `depositsOf()` 동작은 같다 (Task 8).
8. **리액티브 앱 테스트의 스키마는 테스트에서만 Flyway 로 `apps/api` 의 마이그레이션을 올린다.** 앱 본체는 Flyway 의존이 없다(스키마는 `apps/api` 소유). 테스트 작업에 `spring.flyway.locations=filesystem:<apps/api 마이그레이션 절대 경로>` 를 시스템 프로퍼티로 넘긴다 (Task 1).
9. **`apps/api` 의 1원인증 컨트롤러를 셋으로 나눈다.** 지금 `AccountVerificationController` 하나가 요청 · 확인을 함께 갖는데, 판은 요청만 갈린다. `AccountVerificationController`(확인만, 늘 빈) · `AsyncVerificationRequestController`(요청, `async`, 기본) · `BlockingVerificationRequestController`(요청, `blocking`). 경로 · 응답은 그대로다.

## Global Constraints

스펙에서 그대로 옮긴 프로젝트 전체 요구 (모든 Task 의 요구사항에 암묵적으로 포함된다):

- **비교 경로 (§1 · §2)**: 1원인증 요청 `POST /api/v1/account-verifications` 하나만. 출금 접수 · `/confirm` · 그 밖의 API 는 리액티브 앱에 만들지 않는다.
- **고르는 법 (§2 · §5)**: `account.verification.web-mode` — `apps/api`: **`async`(기본) | `blocking`**, `apps/api-reactive`: **`coroutine`(기본) | `reactor`**. **소문자 그대로만, 그 밖의 값은 기동 실패.** 컨트롤러는 판마다 하나, `@ConditionalOnProperty` 로 하나만 빈 (기본 판은 `matchIfMissing = true`). 지금 값은 `/actuator/info`.
- **판정 (§2 · §3-2)**: `checkHolder(found: String?, input: String): String` · `newDraft(): VerificationDraft` · `issue(draft, command, holderName): AccountVerification` 를 네 판이 같이 쓴다. **기존 `AccountVerificationServiceTest` 는 고치지 않고 통과.** `modules/account` 에 코루틴 · Reactor · Spring 의존을 넣지 않는다.
- **account-infra (§3-1)**: `AccountVerificationEntity` · `AccountVerificationJpaRepository` · `JpaVerificationRepository` · 은행 응답 해석 `holderNameOf` · `requireOk` · `BankRejectedException` 을 `apps/api` 에서 옮긴다. 테이블 소유(Flyway)는 `apps/api` 그대로.
- **리액티브 앱 (§2 · §3-1 · §5)**: Kafka · 스케줄러 · 배치 · Flyway 를 켜지 않는다 (의존성도 넣지 않는다). 포트 **8081**. Hikari 최대 풀 **20** (두 앱 같게). 은행 점검 시간 전문 차단(`sendGate`) · 웹 점검 가드(503 + `Retry-After`)는 `apps/api` 와 같은 방식.
- **흐름 (§3-3)**: 은행 동시 호출 상한은 **어느 판에도 두지 않는다.** 게이트웨이 응답 시간 초과 3초는 모든 판에 같다. 판 사이에 일부러 남기는 차이는 블로킹 대조군의 `join()` 하나뿐.
- **스레드 (§4-2)**: JDBC 는 두 이벤트 루프(Reactor Netty HTTP 루프 · 은행 게이트웨이 루프) 어디에서도 돌지 않는다. 코루틴 `withContext(io)` (기본 `Dispatchers.IO`, 생성자로 받음), Reactor `publishOn(Schedulers.boundedElastic())`.
- **끊김 (§4-3)**: 클라이언트가 끊으면 취소되게 둔다. **`CancellationException` 은 오류 응답으로 바꾸지 않고 다시 던진다.** 1원 이체 대기 중 끊김은 **경고 로그 (단계 `transfer` · 은행코드 · 마스킹한 계좌)**. `NonCancellable` 은 쓰지 않는다. MVC 판은 지금처럼 끝까지 진행.
- **오류 응답 (§4-4)**: 400 `USER_ID_REQUIRED`(본문 검증보다 먼저) · 400 본문 검증(지금 MVC 와 같은 `code` = `INVALID_REQUEST`) · 404 `ACCOUNT_NOT_FOUND` · 422 `HOLDER_NAME_MISMATCH` · 502 `BANK_REJECTED` · 503 `BANK_UNAVAILABLE` · 503 `BANK_MAINTENANCE` + `Retry-After` · 504 `BANK_TIMEOUT` · 500 `INTERNAL_ERROR`(메시지 숨김). **응답에 인증코드를 넣지 않는다.** 두 앱이 같아야 한다 (계약 테스트).
- **진행 조건 실험 (§4-1)**: E1 ~ E4 를 구현 전에 진짜 모의 은행으로. 통과해야 다음 Task.
- **측정 (§6)**: 본 측정 **4판 × D 100 · 500ms × R 100 · 200 · 400건/s × 3회 중앙값 (72회)**, 원인 확인 **MVC 블로킹 · Tomcat 최대 스레드 400 · D 500 · R 400 · 3회**, 한계 탐색 **비동기 세 판 · D 500 · R 800 → 1600 → … (성공률 99% 미만 · 201 p99 3초 초과 · 시스템 CPU 80% 중 하나면 멈춤)**. 회차 = **워밍업 10초 + 측정 30초**. k6 요청 시간 초과 **10초**. 기록: 201 처리량 · 201 의 p50 · p95 · p99 · 실패 상태별 수 · `dropped_iterations` · `jvm.threads.live` 최댓값 · 앱 프로세스 CPU · 시스템 CPU · 정확성(위 "스펙과 다른 점" 5).
- **격리 (§6)**: 측정용 모의 은행 **9190** (`--response-delay`, 회차마다 새 `--bank-files`), 측정용 앱 포트 **18080**(`apps/api`) · **18081**(`apps/api-reactive`). `apps/api` 측정 인스턴스는 **Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존 스케줄을 끈다.** 측정 사용자 **`p62-` 접두** (`p62-u001` ~ `p62-u100`), 계좌는 모의 은행 기본 계좌. `cleanup` 은 측정 사용자의 1원인증 행만 지운다. 시작 전 읽기 전용 사전 확인. 도구 **`load/verification/`**, 결과 **`docs/perf/p6-2-web-reactive.md`** + 원자료 `docs/perf/raw/p6-2/`, 절차 **OPERATION §12**.
- **판단 기준 (§6)**: 비동기 세 판의 처리량 차이가 **5% 안이면 "같다"**. **운영 기본은 MVC 비동기 그대로**, WebFlux 앱은 비교용 (ADR-0013).
- **변이 검사 (§7)**: `withContext(io)` 제거 · `publishOn` 제거 · 취소를 오류 응답으로 · 블로킹 컨트롤러의 `join()` 을 future 반환으로 — 모두 테스트가 잡아야 한다. 커밋하지 않고 되돌린다.
- **실행 구성 (§7)**: `8. API 리액티브 (코루틴)` · `9. API 리액티브 (Reactor)` · `10. API (1원인증 블로킹)`. `http/account-verification.http` · Postman 에 리액티브 앱 주소 변수.
- **문서 (§9)**: ADR-0013 (**제안** — 머지 후 채택), 결과 문서, OPERATION §12, ARCHITECTURE(모듈 두 개), README(로드맵 P6-2 · 테스트 수), SETUP(포트 8081), HANDOVER, CHANGELOG. **용어 정리: 저장소 문서의 "퍼밋" → "은행 호출 자리(세마포어 permit)"** — ADR-0012 · P6-1 결과 문서 · HANDOVER · 코루틴 리스너 주석.
- **작업 규칙**: 브랜치 `feat/p6-2-web-reactive` 에서만. **push 하지 않는다.** 문서 · 주석 · 커밋은 한국어, 주석은 "왜" 를 쓰고 주변 코드의 밀도와 말투를 따른다 (CLAUDE.md). TDD. 설정 파일은 먼저 읽고 합친다(덮어쓰기 금지). 커밋 `type(scope): 한국어 요약`, 마지막 줄 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. 비밀(DB 비밀번호 · 텔레그램 토큰)은 저장소 · 명령행에 넣지 않는다 (`infra/.env` → 컨테이너로만). `postman/` 아래에서 **Postman 앱이 다시 쓴 파일은 커밋하지 않는다** — 커밋 전 `git status`, `git add` 는 경로를 적는다. 로그에 계좌번호 원문 · 인증코드를 남기지 않는다 (계좌는 마스킹).
- **테스트는 Testcontainers 만** (자동 테스트가 로컬 인프라를 쓰지 않는다). **로컬 인프라 MySQL(3306)은 측정 스크립트(Task 8 연기 측정 · Task 10 측정)만 쓰고, 그것도 사용자 `p62-u001 ~ u100` 의 1원인증 행만 만들고 지운다 — 다른 행은 건드리지 않는다.** `bootRun` 으로 앱을 띄워 확인하지 않는다 (손 확인은 사용자가 한다).
- **명령 (Windows, Git Bash, 저장소 루트)**: 통합 테스트 전에 Docker Desktop 을 띄우고 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`. 대상 테스트 `./gradlew :apps:api:test --tests '*이름*'` · `./gradlew :apps:api-reactive:test --tests '*이름*'`. 전체 `./gradlew build` — P6-1 끝에 465개 · 6 ~ 8분이었고, 이 계획이 컨텍스트 3개(리액티브 공유 · 리액티브 Reactor · MVC 블로킹)와 실험 컨텍스트를 더해 **8 ~ 11분으로 추정** (Bash 도구 `timeout` 600000, 넘으면 `run_in_background`).

**버전 · API 확인 결과 (계획 작성 때 jar 로 확인함 — 구현자는 다시 찾지 않아도 된다. 코드는 컴파일하지 않았다)**

1. **Reactor `Mono.fromFuture(Supplier)`** (`reactor-core-3.8.7.jar`, `javap`): `fromFuture(Supplier)` · `fromFuture(Supplier, boolean suppressCancel)` 가 있다. 구독 취소 때 `MonoCompletionStageSubscription.cancel()` 이 `Future.cancel` 을 부른다(`suppressCancel = false` 기본). 완료 예외가 `CompletionException` 이면 벗긴다(바이트코드의 `instanceof CompletionException`). → E3 · E4 의 Reactor 쪽 예상 근거. 진짜 모의 은행으로 Task 1 이 확인한다.
2. **Spring 이 suspend 핸들러를 부르는 디스패처**: P6-1 계획 확인 결과 2 — `CoroutinesUtils.invokeSuspendingFunction` 이 `Dispatchers.Unconfined` 로 `mono {}` 를 만든다 (`spring-core-7.0.9`). WebFlux 의 `InvocableHandlerMethod` 도 suspend 메서드를 이 함수로 부른다(추정 — Spring 6 부터 같은 경로). → **E1 예상: `await()` 뒤는 게이트웨이 이벤트 루프 스레드.** Task 1 이 확인한다.
3. **`kotlinx.coroutines.future.await()`**: 완료 예외를 벗겨(`CompletionException` 의 cause) 던지고, 코루틴 취소 때 받은 future 를 `cancel` 한다 (P6-1 Task 3 이 게이트웨이 대기 표가 응답 시간 초과 뒤 정리됨을 고정 — `BankGatewayClient.pendingCount`).
4. **Boot 4 모듈**: `spring-boot-starter-webflux` · `-actuator` · `-validation` · `-data-jpa` 를 쓴다. WebFlux 스타터는 로컬 캐시에 없어 **첫 빌드 때 받는다** (네트워크 필요). `org.springframework.boot.autoconfigure.AutoConfigurationPackage` 는 `spring-boot-autoconfigure-4.1.1.jar` 에 있다.
5. **WebFlux 의 오류 처리기 상속점**: `org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler` 의 `handleExceptionInternal(ex: Exception, body: Any?, headers: HttpHeaders?, status: HttpStatusCode, exchange: ServerWebExchange): Mono<ResponseEntity<Any>>` (Spring 6.0+ 시그니처, 추정 — 구현자는 컴파일로 확인하고 다르면 시그니처만 맞춘다). WebFlux ProblemDetail 켜기: `spring.webflux.problemdetails.enabled: true`.
6. **WebFlux 인자 해석기**: `org.springframework.web.reactive.result.method.HandlerMethodArgumentResolver` (`supportsParameter` · `resolveArgument(parameter, bindingContext, exchange): Mono<Any>`), 등록은 `WebFluxConfigurer.configureArgumentResolvers(configurer) { configurer.addCustomResolver(…) }`.
7. **코드 사실**: `BankGatewayClient.inquireName(bankCode, accountNo): CompletableFuture<NameInquiryResponse>` · `transfer(bankCode, accountNo, amount, memo, reference): CompletableFuture<TransferResponse>` · `isConnected` · `pendingCount`. 예외 `BankTimeoutException`(gateway) · `BankUnavailableException`(open, 하위 `BankNotSentException`). 모의 은행 기본 계좌 `1002123456789 홍길동` · `3333012345678 김카뱅` · `1101234567890 이페이` · `9990000000002 무응답`(1원 초과만 무응답). `silentAccounts` 로 준 계좌(테스트 `9990000000001`)는 성명조회부터 무응답. 모의 은행 거래 기록 줄 = `D` + 거래번호 20자 + … (1원인증 거래번호는 `V` 접두 → 줄은 `DV` 로 시작). `responseDelay` 는 띄운 뒤에도 바꿀 수 있다(`var`).

## 결정(계획) 모음

스펙이 정하지 않은 곳의 판단이다. 각 Task 안에도 다시 적는다.

- 결정(계획): 판정 클래스 이름 `VerificationRequestRules(codeGenerator, clock, ttl, memoPrefix = "BT")`, 초안 `VerificationDraft(id, codeHash, memo)`. 코드 해시 함수는 `verificationCodeHash(id, code)` 최상위 `internal` 함수로 빼 `confirm()` 과 같이 쓴다. `VerificationDraft.toString()` 은 적요(=코드)를 가린다.
- 결정(계획): account-infra 패키지 `io.github.hyeokjinon.banktransfer.account.infra`. `toDbPrecision()` 은 `apps/api` 의 다른 저장소(출금 · 원장)도 쓰므로 `apps/api/…/persistence/DbPrecision.kt` 에 남기고, account-infra 는 같은 내용의 `private` 함수를 따로 둔다 (한 줄, 두 모듈이 서로 의존하지 않게).
- 결정(계획): 리액티브 앱 패키지 `io.github.hyeokjinon.banktransfer.reactive`, 앱 클래스 `ReactiveApiApplication`. 웹 DTO(`VerificationRequest` · `VerificationResponse`) · `RequestUser` · `UserIdRequiredException` 은 리액티브 앱에 따로 둔다 (웹 계층은 공유하지 않는다 — 스펙 §2). 계약 테스트가 갈라짐을 잡는다.
- 결정(계획): 은행 설정 프로퍼티(`bank.gateway.*` · `bank.maintenance.*`)는 리액티브 앱에 같은 키 · 같은 기본값의 클래스를 따로 둔다 (`apps/api` 의 클래스는 `apps/api` 전용 설정 파일에 있고, 20줄짜리라 모듈로 빼는 이득보다 의존이 늘어나는 비용이 크다).
- 결정(계획): `/actuator/info` — `apps/api`: `info.account.verification.web-mode` · `info.server.tomcat.threads.max`, 리액티브: `info.account.verification.web-mode` · `info.datasource.maximum-pool-size`. 측정 스크립트가 띄운 조건을 확인한 뒤에만 잰다.
- 결정(계획): 경고 로그의 계좌 마스킹은 끝 4자리만 남긴다 (`****6789`). 함수 `maskAccount(accountNo)` 는 account-infra 에 둔다 (두 흐름이 쓴다).
- 결정(계획): 측정 산출물 이름 `<판>-d<지연>-r<도착률>[-t<톰캣>]-<회차>.json` (예: `coroutine-d500-r400-1.json`, `blocking-d500-r400-t400-1.json`). 회차가 숫자면 본 측정 표, `probe` 로 시작하면 한계 탐색 표, 그 밖(`smoke`)은 표에서 빠진다.
- 결정(계획): 변이 검사 4종은 Task 7 에서 돌리고 결과를 그 커밋 메시지에, Task 9 에서 ADR-0013 에 옮긴다.
- 결정(계획): 측정(Task 10)은 **컨트롤러가 직접** 돌린다 (2시간 가까이 백그라운드 프로세스를 다루고, 한계 탐색에 중간 판단이 있다).

## 파일 구조

| 파일 | 책임 |
|---|---|
| `settings.gradle.kts` (수정, Task 1 · 3) | `:apps:api-reactive` (Task 1), `:modules:account-infra` (Task 3) |
| `apps/api-reactive/build.gradle.kts` (신규, Task 1) | WebFlux · actuator · validation · data-jpa · coroutines, 테스트 Flyway · Testcontainers |
| `apps/api-reactive/…/reactive/ReactiveApiApplication.kt` · `src/main/resources/application.yml` (신규, Task 1) | 앱 골격 (Task 5 에서 채운다) |
| `apps/api-reactive/src/test/…/experiment/webflux/WebFluxExperimentTest.kt` (신규, Task 1) | 진행 조건 실험 E1 ~ E4 (WebFlux 코루틴 · Reactor) — 남겨 둔다 |
| `apps/api/src/test/…/api/experiment/MvcDisconnectExperimentTest.kt` (신규, Task 1) | E2 의 MVC 비동기 쪽 (끊겨도 끝까지 저장) |
| `modules/account/…/account/VerificationRequestRules.kt` (신규, Task 2) | 판정 순수 함수 |
| `modules/account/…/account/AccountVerificationService.kt` (수정, Task 2) | `request()` 를 판정 함수로 |
| `modules/account/src/test/…/account/VerificationRequestRulesTest.kt` (신규, Task 2) | 판정 단위 |
| `modules/account-infra/build.gradle.kts` · `…/account/infra/*.kt` (신규, Task 3) | JPA 저장소 · 은행 응답 해석 · 계좌 마스킹 |
| `modules/account-infra/src/testFixtures/…/account/infra/contract/VerificationErrorContract.kt` (신규, Task 3) | 오류 계약 기대표 (두 앱 테스트가 읽는다) |
| `modules/bank-gateway/…/gateway/BankMaintenanceGuard.kt` (이동, Task 3) | 점검 가드 · 예외 |
| `apps/api/…` (수정, Task 3) | import 이동 · `@AutoConfigurationPackage` · `persistence/DbPrecision.kt` |
| `apps/api/…/api/web/AsyncVerificationRequestController.kt` · `BlockingVerificationRequestController.kt` (신규, Task 4) | MVC 두 판 |
| `apps/api/…/api/web/AccountVerificationController.kt` (수정, Task 4) | 확인만 |
| `apps/api/…/api/config/AppConfiguration.kt` · `application.yml` (수정, Task 4) | `web-mode` 검증 · info |
| `.run/10. API (1원인증 블로킹).run.xml` (신규, Task 4) | `--account.verification.web-mode=blocking` |
| `apps/api-reactive/…/reactive/*` (신규, Task 5) | 설정 · 게이트웨이 · 점검 가드 · 사용자 해석 · 오류 처리 · 코루틴 흐름 · 컨트롤러 |
| `.run/8. API 리액티브 (코루틴).run.xml` (신규, Task 5) | `:apps:api-reactive:bootRun` |
| `apps/api-reactive/…/reactive/ReactorVerificationFlow.kt` · `ReactorVerificationController.kt` (신규, Task 6) | Reactor 판 |
| `.run/9. API 리액티브 (Reactor).run.xml` (신규, Task 6) | `--account.verification.web-mode=reactor` |
| 계약 · 끊김 테스트 (신규, Task 7) | 두 앱의 `VerificationErrorContractTest`, 리액티브 끊김 테스트 |
| `apps/mock-bank/…/MockBankServer.kt` (수정, Task 8) | 입금 기록 O(1) |
| `load/verification/run.sh` · `verification.js` · `cleanup.sql` · `stats.sql` · `summarize.sh` (신규, Task 8) | 측정 도구 |
| 문서 (Task 9) | ADR-0013, OPERATION §12, ARCHITECTURE, README, SETUP, HANDOVER, CHANGELOG, 용어 정리, `http/account-verification.http`, Postman |
| 결과 (Task 10) | `docs/perf/p6-2-web-reactive.md`, `docs/perf/raw/p6-2/*.json`, ADR-0013 결과 절 |

`…` 경로는 모두 `src/main/kotlin/io/github/hyeokjinon/banktransfer` (테스트는 `src/test/kotlin/io/github/hyeokjinon/banktransfer`) 아래다.

---

### Task 1: 실험 게이트 — 리액티브 앱 골격 · 진행 조건 실험 E1 ~ E4 (결과에 따라 진행)

스펙 §4-1: WebFlux suspend · Reactor 가 은행 게이트웨이 future 를 기다릴 때의 스레드 · 취소 · 예외를 진짜 모의 은행으로 확인하고 테스트로 남긴다. **이 Task 가 통과해야 Task 2 이후를 한다.**

**Files:**
- Modify: `settings.gradle.kts`
- Create: `apps/api-reactive/build.gradle.kts`
- Create: `apps/api-reactive/src/main/kotlin/io/github/hyeokjinon/banktransfer/reactive/ReactiveApiApplication.kt`
- Create: `apps/api-reactive/src/main/resources/application.yml`
- Create: `apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/experiment/webflux/WebFluxExperimentTest.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/MvcDisconnectExperimentTest.kt`

**Interfaces:**
- Consumes: `BankGatewayClient(BankGatewayConfig(host, port, responseTimeout))` · `.start()` · `.isConnected` · `.inquireName(bankCode, accountNo)`, `MockBankServer(silentAccounts = …).start()` · `.boundPort` · `.responseDelay` (var), `apps/api` 의 `ApiIntegrationTest` (`bank` · `port` · `newUserId()`).
- Produces: Gradle 모듈 `:apps:api-reactive` (패키지 `io.github.hyeokjinon.banktransfer.reactive`, 앱 클래스 `ReactiveApiApplication`). 실험 관찰값 — 커밋 메시지에 적어 Task 9 (ADR-0013) 가 옮긴다.

**멈춤 조건 (컨트롤러가 사용자에게 돌아간다):**
- **BLOCKED — 구현을 멈추고 보고한다**: E2 에서 클라이언트가 끊어도 WebFlux 코루틴이 취소되지 않거나(5초 안에 `CancellationException` 없음) Reactor 판에서 `doOnCancel` 이 불리지 않는다 · E3 에서 취소 뒤 게이트웨이 future 가 `isCancelled` 가 아니다. 스펙 §4-3 의 끊김 정책 · 경고 로그가 전제를 잃는다. 관찰값과 함께 보고하고, 테스트 기대값을 관찰값에 맞춰 고쳐 커밋하지 않는다.
- **DONE_WITH_CONCERNS — 진행하되 보고한다**: E1 의 `await` 뒤 스레드가 게이트웨이 루프가 아니다(예: `reactor-http-nio-…`) · E4 에서 예외가 `CompletionException` 으로 싸여 온다 · MVC 실험에서 끊긴 요청이 저장되지 않는다. 스펙 §4-1 "예상과 다르면" 칸대로 설명 · 기대값을 실제에 맞추면 되는 차이라 멈추지 않는다 — 실제 관찰값을 테스트의 기대값으로 두고, 테스트 이름 · 주석 · 커밋 메시지에 "예상과 다름" 을 적는다.

- [ ] **Step 1: 모듈 등록 · 빌드 파일**

`settings.gradle.kts` 의 실행 모듈 줄 아래에 더한다 (기존 줄은 그대로):

```kotlin
include(":apps:api-reactive")       // 1원인증 요청만 하는 WebFlux 앱 (P6-2 — MVC 와 비교용, 운영 기본 아님)
```

`apps/api-reactive/build.gradle.kts`:

```kotlin
plugins {
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

// 1원인증 요청만 하는 WebFlux 앱 (P6-2, ADR-0013). Kafka · 스케줄러 · 배치 · Flyway 는 넣지 않는다 — 테이블은 apps/api 의 Flyway 가 만든다.
// JPA(블로킹 JDBC)는 IO 디스패처 · boundedElastic 에서만 부른다 (R2DBC 는 범위 밖).
dependencies {
    implementation(project(":modules:account"))
    implementation(project(":modules:bank-gateway"))
    implementation(project(":modules:transfer"))   // RandomReferenceGenerator — 1원인증 거래번호(V 접두)를 apps/api 와 같은 생성기로

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    // 버전은 Boot BOM (kotlinx-coroutines 1.10.2). core: await · withContext. reactor: WebFlux 가 suspend 핸들러를 Mono 로 감싸 부를 때 쓴다
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation(project(":apps:mock-bank"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.awaitility:awaitility-kotlin")
    // 테스트에서만 apps/api 의 마이그레이션으로 스키마를 만든다 (앱 본체는 스키마를 바꾸지 않는다)
    testImplementation("org.springframework.boot:spring-boot-starter-flyway")
    testImplementation("org.flywaydb:flyway-mysql")
}

tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")   // apps/api 와 같다: KST 변환이 JVM 기본 시간대에 기대지 않는지 서버(UTC) 조건에서 본다
    // 스키마 소유는 apps/api — 그 마이그레이션 폴더를 그대로 쓴다 (복사하면 갈라진다)
    systemProperty(
        "spring.flyway.locations",
        "filesystem:" + rootProject.file("apps/api/src/main/resources/db/migration").absolutePath.replace('\\', '/'),
    )
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    workingDir = rootProject.projectDir
}
```

- [ ] **Step 2: 앱 골격 (Task 5 에서 채운다)**

`ReactiveApiApplication.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

/**
 * 1원인증 요청만 하는 WebFlux 앱 (P6-2, ADR-0013). 같은 요청을 apps/api(MVC)와 같은 도구로 비교하려고 둔다 — 운영 기본은 apps/api 다.
 * Spring 은 한 앱에 MVC 와 WebFlux 가 같이 있으면 MVC 를 고르므로 앱을 나눴다.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
class ReactiveApiApplication

fun main(args: Array<String>) {
    runApplication<ReactiveApiApplication>(*args)
}
```

`application.yml` (Task 5 에서 데이터소스 · 은행 · 계정 설정을 더한다):

```yaml
spring:
  application:
    name: bank-transfer-reactive
  main:
    web-application-type: reactive   # 명시: 클래스패스에 spring-web(RestClient 등)도 있어 서블릿으로 오인되지 않게

server:
  port: 8081                          # apps/api(8080)와 나란히 띄운다
```

- [ ] **Step 3: 실험 테스트 E1 ~ E4 (WebFlux)**

Spring Boot 자동 설정 없이 `@EnableWebFlux` 컨텍스트 + Reactor Netty `HttpServer` 로 띄운다 — 실험이 앱 설정(Task 5)에 기대지 않고, 패키지 `…experiment.webflux` 는 앱의 컴포넌트 스캔(`…reactive`) 밖이라 다른 테스트에 섞이지 않는다 (P6-1 계획 "스펙과 다른 점" 7 과 같은 이유).

`WebFluxExperimentTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.experiment.webflux

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.client.RestClient
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.server.adapter.WebHttpHandlerBuilder
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServer
import java.net.Socket
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * P6-2 진행 조건 실험 (설계 §4-1). 문서만으로 확신할 수 없는 네 가지를 진짜 모의 은행 · 게이트웨이로 본다:
 * E1 await 뒤 스레드, E2 클라이언트 끊김 → 취소, E3 취소가 게이트웨이 future 까지 가는지, E4 예외가 벗겨져 오는지.
 * 결과는 ADR-0013 에 옮긴다. 남겨 둔다 — 프레임워크를 올렸을 때 같은 동작인지 다시 본다.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebFluxExperimentTest {

    /** 컨트롤러가 본 것. 테스트마다 [reset]. */
    class Probe {
        val threadAfterAwait = AtomicReference<String?>()
        val error = AtomicReference<Throwable?>()
        val future = AtomicReference<CompletableFuture<*>?>()
        @Volatile var cancelled = CountDownLatch(1)
        fun reset() { threadAfterAwait.set(null); error.set(null); future.set(null); cancelled = CountDownLatch(1) }
    }

    @RestController
    class ExperimentController(private val client: BankGatewayClient, private val probe: Probe) {

        @GetMapping("/coroutine/name")
        suspend fun coroutineName(@RequestParam account: String): String {
            val f = client.inquireName("090", account).also { probe.future.set(it) }
            try {
                val res = f.await()
                probe.threadAfterAwait.set(Thread.currentThread().name)
                return res.holderName
            } catch (e: CancellationException) {
                probe.cancelled.countDown()
                throw e
            } catch (e: Exception) {
                probe.error.set(e)
                throw e
            }
        }

        @GetMapping("/reactor/name")
        fun reactorName(@RequestParam account: String): Mono<String> =
            Mono.fromFuture { client.inquireName("090", account).also { probe.future.set(it) } }
                .map { res: NameInquiryResponse -> probe.threadAfterAwait.set(Thread.currentThread().name); res.holderName }
                .doOnCancel { probe.cancelled.countDown() }
                .doOnError { probe.error.set(it) }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebFlux
    class ExperimentConfig {
        @Bean fun probe() = Probe()
        @Bean fun controller(client: BankGatewayClient, probe: Probe) = ExperimentController(client, probe)
    }

    private val bank = MockBankServer(silentAccounts = setOf(SILENT)).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val context = AnnotationConfigApplicationContext().apply {
        beanFactory.registerSingleton("bankGatewayClient", gateway)
        register(ExperimentConfig::class.java)
        refresh()
    }
    private val server = HttpServer.create().port(0).handle(ReactorHttpHandlerAdapter(WebHttpHandlerBuilder.applicationContext(context).build())).bindNow()
    private val probe = context.getBean(Probe::class.java)
    private val http = RestClient.create("http://localhost:${server.port()}")

    @BeforeEach
    fun setUp() {
        probe.reset()
        bank.responseDelay = Duration.ZERO
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { server.disposeNow(); context.close(); gateway.close(); bank.close() }

    /** 요청을 보낸 뒤 응답을 읽지 않고 [closeAfterMs] 뒤 연결을 끊는다 (브라우저 닫힘 · 클라이언트 시간 초과와 같다). */
    private fun requestAndDisconnect(path: String, closeAfterMs: Long) {
        Socket("localhost", server.port()).use { s ->
            s.getOutputStream().write("GET $path HTTP/1.1\r\nHost: localhost\r\n\r\n".toByteArray())
            s.getOutputStream().flush()
            Thread.sleep(closeAfterMs)
        }
    }

    private fun threadOnly(name: String?) = name!!.substringBefore(" @coroutine")   // 코루틴 디버그 모드(-ea)가 붙이는 꼬리를 뗀다

    @Test
    fun `E1 코루틴 - await 뒤 코드는 은행 응답을 완료한 게이트웨이 이벤트 루프에서 이어진다`() {
        assertEquals("홍길동", http.get().uri("/coroutine/name?account=1002123456789").retrieve().body(String::class.java))
        val after = threadOnly(probe.threadAfterAwait.get())
        println("[P6-2 실험 E1 코루틴] await 뒤 = $after")
        assertTrue(after.startsWith("multiThreadIoEventLoopGroup"), "게이트웨이 루프(Netty 기본 이름)여야 한다: $after")
        assertFalse(after.startsWith("reactor-http"), "HTTP 루프가 아니어야 한다: $after")
    }

    @Test
    fun `E1 Reactor - fromFuture 뒤 map 은 은행 응답을 완료한 게이트웨이 이벤트 루프에서 돈다`() {
        assertEquals("홍길동", http.get().uri("/reactor/name?account=1002123456789").retrieve().body(String::class.java))
        val after = threadOnly(probe.threadAfterAwait.get())
        println("[P6-2 실험 E1 Reactor] map = $after")
        assertTrue(after.startsWith("multiThreadIoEventLoopGroup"), "게이트웨이 루프여야 한다: $after")
    }

    @Test
    fun `E2 E3 코루틴 - 응답 전에 끊으면 코루틴이 취소되고 게이트웨이 future 도 취소된다`() {
        bank.responseDelay = Duration.ofMillis(400)
        requestAndDisconnect("/coroutine/name?account=1002123456789", closeAfterMs = 100)
        assertTrue(probe.cancelled.await(5, TimeUnit.SECONDS), "끊김이 코루틴 취소로 전파돼야 한다")
        assertTrue(probe.future.get()!!.isCancelled, "await 가 취소될 때 받은 future 도 취소해야 한다")
        println("[P6-2 실험 E2 · E3 코루틴] 취소됨, future.isCancelled = true")
    }

    @Test
    fun `E2 E3 Reactor - 응답 전에 끊으면 구독이 취소되고 게이트웨이 future 도 취소된다`() {
        bank.responseDelay = Duration.ofMillis(400)
        requestAndDisconnect("/reactor/name?account=1002123456789", closeAfterMs = 100)
        assertTrue(probe.cancelled.await(5, TimeUnit.SECONDS), "끊김이 구독 취소로 전파돼야 한다")
        assertTrue(probe.future.get()!!.isCancelled, "fromFuture 가 취소될 때 future 도 취소해야 한다 (suppressCancel = false)")
        println("[P6-2 실험 E2 · E3 Reactor] 취소됨, future.isCancelled = true")
    }

    @Test
    fun `E4 코루틴 - 은행 시간 초과는 CompletionException 에 싸이지 않고 원래 예외로 온다`() {
        runCatching { http.get().uri("/coroutine/name?account=$SILENT").retrieve().toBodilessEntity() }
        assertIs<BankTimeoutException>(probe.error.get(), "await 는 CompletionException 을 벗긴다")
    }

    @Test
    fun `E4 Reactor - 은행 시간 초과는 CompletionException 에 싸이지 않고 원래 예외로 온다`() {
        runCatching { http.get().uri("/reactor/name?account=$SILENT").retrieve().toBodilessEntity() }
        assertIs<BankTimeoutException>(probe.error.get(), "fromFuture 는 CompletionException 을 벗긴다")
    }

    private companion object {
        const val SILENT = "9990000000001"   // 성명조회부터 무응답 (silentAccounts)
    }
}
```

- [ ] **Step 4: 실험 테스트 E2 의 MVC 쪽 (apps/api)**

`MvcDisconnectExperimentTest.kt` — 서블릿 비동기는 끊겨도 future 를 끝까지 진행해 저장하는지 (스펙 §4-3 마지막 줄의 근거):

```kotlin
package io.github.hyeokjinon.banktransfer.api.experiment

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.net.Socket
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P6-2 진행 조건 실험 E2 의 MVC 쪽 (설계 §4-1 · §4-3). 서블릿 비동기(CompletableFuture 반환)는 클라이언트가 끊어도 future 를 취소하지 않아
 * 1원 이체 · 저장까지 끝난다 — WebFlux 판(끊기면 취소)과 다른 점이다. 결과는 ADR-0013 에 옮긴다.
 * 공유 모의 은행의 지연을 잠깐 바꾼다: 게이트웨이 응답 시간 초과(테스트 500ms)보다 짧게, 끝나면 되돌린다.
 */
class MvcDisconnectExperimentTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun `E2 MVC 비동기 - 응답 전에 끊어도 1원 이체와 저장은 끝까지 간다`() {
        val user = newUserId()
        val bytes = """{"bankCode":"090","accountNo":"1002123456789","holderName":"홍길동"}""".toByteArray()
        bank.responseDelay = Duration.ofMillis(300)
        try {
            Socket("localhost", port).use { s ->
                s.getOutputStream().write(
                    ("POST /api/v1/account-verifications HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n" +
                        "$USER_ID_HEADER: $user\r\nContent-Length: ${bytes.size}\r\n\r\n").toByteArray() + bytes,
                )
                s.getOutputStream().flush()
                Thread.sleep(100)   // 성명조회 응답(300ms)을 기다리는 중에 끊는다
            }
            await.atMost(Duration.ofSeconds(5)).ignoreExceptions() untilAsserted {
                assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE user_id = ?", Int::class.java, user))
            }
            println("[P6-2 실험 E2 MVC] 끊긴 요청도 저장됨 (취소되지 않음)")
        } finally {
            bank.responseDelay = Duration.ZERO
        }
    }
}
```

- [ ] **Step 5: 실험을 돌린다**

Run:
```bash
export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"
./gradlew :apps:api-reactive:test --tests '*WebFluxExperimentTest*' :apps:api:test --tests '*MvcDisconnectExperimentTest*'
```
Expected: 7개 PASS. 출력의 `[P6-2 실험 …]` 줄을 모은다 (WebFlux 스타터를 처음 받느라 첫 실행은 몇 분 걸릴 수 있다).
- E1 이 다른 스레드를 보이면: 그 이름을 기대값으로 바꾸고 테스트 이름에 "(예상과 다름: …)" 을 붙여 DONE_WITH_CONCERNS.
- E2 · E3 이 실패하면: BLOCKED (위 멈춤 조건).
- 게이트웨이 루프 이름이 `multiThreadIoEventLoopGroup-` 이 아닌 다른 Netty 기본 이름이면(Netty 버전 차이) 그것이 게이트웨이 루프인지 `BankGatewayClient` 의 `group` 생성 줄(`MultiThreadIoEventLoopGroup(1, …)`)로 확인하고 기대값을 고친다 — 이것은 "예상과 다름" 이 아니다.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts apps/api-reactive/build.gradle.kts \
  apps/api-reactive/src/main/kotlin/io/github/hyeokjinon/banktransfer/reactive/ReactiveApiApplication.kt \
  apps/api-reactive/src/main/resources/application.yml \
  apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/experiment/webflux/WebFluxExperimentTest.kt \
  apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/experiment/MvcDisconnectExperimentTest.kt
git commit -F - <<'EOF'
test(p6-2): 진행 조건 실험 E1 ~ E4 — WebFlux 코루틴 · Reactor 의 스레드 · 끊김 취소 · 예외, MVC 비동기의 끊김

관찰값:
- E1 코루틴 await 뒤 = <스레드 이름>, Reactor map = <스레드 이름>
- E2 · E3 코루틴 · Reactor 모두 끊기면 취소, 게이트웨이 future isCancelled = <true/false>
- E4 코루틴 · Reactor 모두 BankTimeoutException 그대로
- E2 MVC 비동기: 끊겨도 저장됨

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```
(`<…>` 는 Step 5 출력으로 채운다 — 관찰값 그대로.)

---

### Task 2: 도메인 — 판정 순수 함수 `VerificationRequestRules`

스펙 §3-2: 네 판이 같이 쓰는 판정. 지금의 `AccountVerificationService.request()` 를 이 함수들로 다시 짠다 — **기존 `AccountVerificationServiceTest` 는 고치지 않고 통과.**

**Files:**
- Create: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/VerificationRequestRules.kt`
- Modify: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationService.kt`
- Create: `modules/account/src/test/kotlin/io/github/hyeokjinon/banktransfer/account/VerificationRequestRulesTest.kt`

**Interfaces:**
- Consumes: `AccountVerification`, `RequestVerification`, `CodeGenerator`, `RandomCodeGenerator`, `AccountVerificationException`, `VerificationStatus` (모두 기존).
- Produces (Task 4 ~ 6 이 쓴다):
  - `class VerificationRequestRules(codeGenerator: CodeGenerator = RandomCodeGenerator(), clock: Clock = Clock.systemUTC(), ttl: Duration = Duration.ofMinutes(5), memoPrefix: String = "BT")`
  - `fun checkHolder(found: String?, input: String): String` — `null` → `AccountNotFound`, 공백을 뺀 이름이 다르면 `HolderNameMismatch`, 같으면 `found`
  - `fun newDraft(): VerificationDraft`
  - `fun issue(draft: VerificationDraft, command: RequestVerification, holderName: String): AccountVerification`
  - `class VerificationDraft(val id: UUID, val codeHash: String, val memo: String)` (`toString()` 은 id 만)
  - `internal fun verificationCodeHash(id: UUID, code: String): String` (모듈 안에서만)

- [ ] **Step 1: 실패하는 테스트**

`VerificationRequestRulesTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.account

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class VerificationRequestRulesTest {

    private val now = Instant.parse("2026-11-05T00:00:00Z")
    private val rules = VerificationRequestRules(
        codeGenerator = { "1234" },
        clock = Clock.fixed(now, ZoneOffset.UTC),
        ttl = Duration.ofMinutes(5),
    )
    private val command = RequestVerification("user-1", "090", "1002123456789", "홍 길동")

    @Test
    fun `예금주가 없으면 AccountNotFound`() {
        assertFailsWith<AccountVerificationException.AccountNotFound> { rules.checkHolder(null, "홍길동") }
    }

    @Test
    fun `공백을 빼고 비교한다 - 같으면 은행이 준 예금주명을 돌려준다`() {
        assertEquals("홍길동", rules.checkHolder("홍길동", " 홍 길 동 "))
    }

    @Test
    fun `이름이 다르면 HolderNameMismatch`() {
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { rules.checkHolder("김카뱅", "홍길동") }
    }

    @Test
    fun `초안 - 적요는 접두 + 코드, 해시는 id 와 코드로 만든다`() {
        val draft = rules.newDraft()

        assertEquals("BT1234", draft.memo)
        assertEquals(verificationCodeHash(draft.id, "1234"), draft.codeHash)
        assertEquals(64, draft.codeHash.length)
    }

    @Test
    fun `초안의 문자열 표현에는 적요(인증코드)가 없다 - 로그에 새지 않게`() {
        val draft = rules.newDraft()

        assertFalse("1234" in draft.toString(), draft.toString())
    }

    @Test
    fun `발급 - SENT 이고 만료는 발급 시각 + ttl, 요청자를 남긴다`() {
        val draft = rules.newDraft()

        val v = rules.issue(draft, command, "홍길동")

        assertEquals(draft.id, v.id)
        assertEquals("user-1", v.userId)
        assertEquals("090", v.bankCode)
        assertEquals("1002123456789", v.accountNo)
        assertEquals("홍길동", v.holderName)
        assertEquals(draft.codeHash, v.codeHash)
        assertEquals(VerificationStatus.SENT, v.status)
        assertEquals(0, v.failedAttempts)
        assertEquals(now.plus(Duration.ofMinutes(5)), v.expiresAt)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:account:test --tests '*VerificationRequestRulesTest*'`
Expected: 컴파일 실패 (`Unresolved reference: VerificationRequestRules`).

- [ ] **Step 3: 구현**

`VerificationRequestRules.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.account

import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.util.HexFormat
import java.util.UUID

/**
 * 1원인증 요청의 판정 (P6-2). 네 판(MVC 블로킹 · 비동기, WebFlux 코루틴 · Reactor)이 같이 쓴다 — 판마다 다른 것은 은행을 기다리는 방법과
 * 저장을 어느 스레드에서 하는가뿐이다. 그래서 여기는 스레드 · I/O 를 모르는 순수 함수만 둔다 (P6-1 의 begin/finish 와 같은 원칙).
 */
class VerificationRequestRules(
    private val codeGenerator: CodeGenerator = RandomCodeGenerator(),
    private val clock: Clock = Clock.systemUTC(),
    private val ttl: Duration = Duration.ofMinutes(5),
    private val memoPrefix: String = "BT",
) {

    /** 성명조회 결과 [found] 와 입력 [input] 을 비교한다. 공백은 무시한다. 같으면 은행이 준 예금주명 (저장값). */
    fun checkHolder(found: String?, input: String): String = when {
        found == null -> throw AccountVerificationException.AccountNotFound()
        normalize(found) != normalize(input) -> throw AccountVerificationException.HolderNameMismatch()
        else -> found
    }

    /** 1원에 실을 코드와 저장할 해시. 코드 원문은 적요로만 나가고 저장하지 않는다. */
    fun newDraft(): VerificationDraft {
        val id = UUID.randomUUID()
        val code = codeGenerator.generate()
        return VerificationDraft(id, verificationCodeHash(id, code), memoPrefix + code)
    }

    /** 1원을 보낸 뒤 저장할 인증 건. 만료는 지금(발급 시각)부터 ttl. */
    fun issue(draft: VerificationDraft, command: RequestVerification, holderName: String) = AccountVerification(
        id = draft.id,
        userId = command.userId,
        bankCode = command.bankCode,
        accountNo = command.accountNo,
        holderName = holderName,
        codeHash = draft.codeHash,
        status = VerificationStatus.SENT,
        failedAttempts = 0,
        expiresAt = clock.instant().plus(ttl),
    )

    private fun normalize(name: String) = name.filterNot { it.isWhitespace() }
}

/** 1원인증 초안. [memo] 는 인증코드를 담으므로 [toString] 에 넣지 않는다 (로그 · 예외 메시지로 새지 않게). */
class VerificationDraft(val id: UUID, val codeHash: String, val memo: String) {
    override fun toString() = "VerificationDraft(id=$id)"
}

/** 코드 해시. 요청(발급)과 확인이 같은 식을 써야 한다. */
internal fun verificationCodeHash(id: UUID, code: String): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("$id:$code".toByteArray()))
```

`AccountVerificationService.kt` — 생성자 인자 목록 · 순서 · 기본값은 그대로 두고 판정을 위임한다. 바뀌는 부분:

```kotlin
class AccountVerificationService(
    private val bank: BankPort,
    private val repository: VerificationRepository,
    codeGenerator: CodeGenerator = RandomCodeGenerator(),
    private val clock: Clock = Clock.systemUTC(),
    ttl: Duration = Duration.ofMinutes(5),
    private val maxAttempts: Int = 5,
    memoPrefix: String = "BT",
) {
    /** 판정은 네 판이 같이 쓰는 [VerificationRequestRules] 에 있다 (P6-2). 여기는 CompletableFuture 로 이어 붙이기만 한다. */
    private val rules = VerificationRequestRules(codeGenerator, clock, ttl, memoPrefix)

    /** 없는 계좌 · 이름 불일치는 checkHolder 가 던진다 → 이 future 가 그 예외로 끝나고 1원은 보내지 않는다. */
    fun request(command: RequestVerification): CompletableFuture<AccountVerification> =
        bank.inquireHolderName(command.bankCode, command.accountNo).thenCompose { found ->
            val holderName = rules.checkHolder(found, command.holderName)
            val draft = rules.newDraft()
            bank.transfer(command.bankCode, command.accountNo, 1, draft.memo).thenApply {
                repository.save(rules.issue(draft, command, holderName))
            }
        }
```

- `confirm()` 의 `hash(v.id, code)` 를 `verificationCodeHash(v.id, code)` 로 바꾼다.
- `sendOneWon` · `normalize` · `hash` (private) 와 쓰지 않게 된 import(`MessageDigest` · `HexFormat`)를 지운다. 클래스 KDoc 은 그대로 둔다.
- 예외 전달은 지금과 같다: `thenCompose` 안에서 던진 예외는 결과 future 를 `CompletionException(원인)` 으로 끝내고, `get()` 은 `ExecutionException(원인)`, MVC 예외 처리기는 `CompletionException` 을 벗긴다.

- [ ] **Step 4: 테스트 통과 · 기존 테스트 그대로**

Run: `./gradlew :modules:account:test`
Expected: `VerificationRequestRulesTest` 6개 + 기존 `AccountVerificationServiceTest` 전부 PASS. **`AccountVerificationServiceTest.kt` 는 고치지 않는다** (`git diff --stat` 에 나오면 안 된다).

- [ ] **Step 5: Commit**

```bash
git add modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/VerificationRequestRules.kt \
  modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationService.kt \
  modules/account/src/test/kotlin/io/github/hyeokjinon/banktransfer/account/VerificationRequestRulesTest.kt
git commit -m "refactor(p6-2): 1원인증 판정을 순수 함수 VerificationRequestRules 로 — 네 판이 같이 쓴다, 기존 서비스 테스트 그대로

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `modules/account-infra` — JPA 저장소 · 은행 응답 해석 이동, 점검 가드 이동, 오류 계약 기대표 (동작 변화 없음)

스펙 §3-1: 두 앱이 같이 쓰는 바깥 어댑터를 한 벌로. **이 Task 는 옮기기만 한다 — `apps/api` 의 기존 테스트가 고치지 않고(import 제외) 모두 통과해야 한다.** 오류 계약 기대표와 `apps/api` 쪽 계약 테스트(지금 동작의 특성 테스트)를 같이 만든다.

**Files:**
- Modify: `settings.gradle.kts`
- Create: `modules/account-infra/build.gradle.kts`
- Move: `apps/api/…/api/persistence/AccountVerificationPersistence.kt` → `modules/account-infra/…/account/infra/AccountVerificationPersistence.kt`
- Create: `modules/account-infra/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/infra/BankResponses.kt`
- Create: `modules/account-infra/src/test/kotlin/io/github/hyeokjinon/banktransfer/account/infra/BankResponsesTest.kt`
- Create: `modules/account-infra/src/testFixtures/kotlin/io/github/hyeokjinon/banktransfer/account/infra/contract/VerificationErrorContract.kt`
- Move: `apps/api/…/api/web/BankMaintenanceGuard.kt` → `modules/bank-gateway/…/gateway/BankMaintenanceGuard.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/DbPrecision.kt`
- Modify: `apps/api/build.gradle.kts`, `BankTransferApplication.kt`, `config/AppConfiguration.kt`, `web/AccountVerificationController.kt`, `web/TransferController.kt`, `web/ApiExceptionHandler.kt`
- Modify (import 만): `apps/api/src/test/…` 중 컴파일러가 가리키는 파일 (`persistence/JpaVerificationRepositoryTest.kt`, `retention/RetentionTestDates.kt`, `web/ApiExceptionHandlerTest.kt` 등)
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/VerificationErrorContractTest.kt`

**Interfaces:**
- Consumes: 없음 (Task 2 와 독립). `NameInquiryResponse(header, bankCode, accountNo, holderName)` · `TransferResponse(header, bankCode, accountNo, amount, memo, reference)` · `Header(messageClass, bizType, tranNo, tranDateTime, responseCode)` · `header.isOk` · `ResponseCodes.OK` · `ResponseCodes.ACCOUNT_NOT_FOUND` (bank-protocol).
- Produces (Task 4 ~ 7 이 쓴다), 패키지 `io.github.hyeokjinon.banktransfer.account.infra`:
  - `AccountVerificationEntity` · `interface AccountVerificationJpaRepository` · `class JpaVerificationRepository(jpa, tx: TransactionTemplate, clock: Clock) : VerificationRepository` (내용 그대로)
  - `class BankRejectedException(val responseCode: String)` · `fun holderNameOf(res: NameInquiryResponse): String?` · `fun requireOk(res: TransferResponse)` · `fun maskAccount(accountNo: String): String`
  - `const val ACCOUNT_INFRA_PACKAGE = "io.github.hyeokjinon.banktransfer.account.infra"`
  - test fixtures `…account.infra.contract.VerificationErrorContract` — `PATH` · `USER` · `SILENT_ACCOUNT = "9990000000001"` · `SUCCESS_FIELDS` · `httpCases: List<HttpCase(name, userId, body, status, code)>` · `exceptionCases: List<ExceptionCase(exception, status, code, retryAfter)>`
  - 패키지 `io.github.hyeokjinon.banktransfer.gateway`: `class BankMaintenanceGuard(window, clock)` · `class BankMaintenanceException(val retryAfterSeconds: Long)` (내용 그대로)

- [ ] **Step 1: 모듈 · 빌드 파일**

`settings.gradle.kts` 의 라이브러리 모듈 줄 아래에 더한다:

```kotlin
include(":modules:account-infra")   // account 도메인의 바깥 어댑터 (JPA 저장소 · 은행 응답 해석) — apps/api 와 apps/api-reactive 가 같이 쓴다 (P6-2)
```

`modules/account-infra/build.gradle.kts`:

```kotlin
plugins {
    `java-library`
    `java-test-fixtures`          // 오류 계약 기대표를 두 앱의 테스트가 같이 읽는다 (P6-2)
    kotlin("plugin.spring")
    kotlin("plugin.jpa")          // @Entity 에 인자 없는 생성자
}

// Hibernate 가 엔티티를 프록시로 감쌀 수 있게 연다 (apps/api 에서 옮겨 온 설정)
allOpen {
    annotation("jakarta.persistence.Entity")
}

// account 도메인 포트(VerificationRepository · 은행 응답)의 바깥 구현. 도메인 모듈(account)은 여전히 Spring · Netty 의존이 없다 (ADR-0001).
dependencies {
    api(project(":modules:account"))
    api(project(":modules:bank-gateway"))   // bank-protocol 의 응답 타입 · 게이트웨이 예외 (bank-gateway 가 api 로 내보낸다)
    api("org.springframework.boot:spring-boot-starter-data-jpa")   // 공개 시그니처에 JpaRepository · TransactionTemplate 이 있다

    testFixturesApi(project(":modules:account"))
    testFixturesApi(project(":modules:bank-gateway"))
}
```

- [ ] **Step 2: 은행 응답 해석 — 실패하는 테스트**

`BankResponsesTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.account.infra

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class BankResponsesTest {

    private fun header(biz: BizType, code: String) = Header(MessageClass.RESPONSE, biz, 1, LocalDateTime.of(2026, 11, 5, 9, 0, 0), code)

    @Test
    fun `성명조회 0000 이면 예금주명`() {
        assertEquals("홍길동", holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, ResponseCodes.OK), "090", "1002123456789", "홍길동")))
    }

    @Test
    fun `성명조회 계좌 없음이면 null`() {
        assertNull(holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, ResponseCodes.ACCOUNT_NOT_FOUND), "090", "5599000000001", "")))
    }

    @Test
    fun `성명조회 그 밖의 코드는 BankRejectedException`() {
        val e = assertFailsWith<BankRejectedException> {
            holderNameOf(NameInquiryResponse(header(BizType.NAME_INQUIRY, "9999"), "090", "1002123456789", ""))
        }
        assertEquals("9999", e.responseCode)
    }

    @Test
    fun `1원 이체가 0000 이 아니면 BankRejectedException, 0000 이면 지나간다`() {
        requireOk(TransferResponse(header(BizType.TRANSFER, ResponseCodes.OK), "090", "1002123456789", 1, "BT1234", "V2026110500000000001"))
        assertFailsWith<BankRejectedException> {
            requireOk(TransferResponse(header(BizType.TRANSFER, "1001"), "090", "1002123456789", 1, "BT1234", "V2026110500000000001"))
        }
    }

    @Test
    fun `계좌는 끝 4자리만 남긴다`() {
        assertEquals("****6789", maskAccount("1002123456789"))
    }
}
```

(`NameInquiryResponse` · `TransferResponse` 생성자 인자 순서는 `bank-protocol/…/BankMessage.kt:21 · 39` 와 맞춘다 — 다르면 인자 순서만 맞춘다.)

Run: `./gradlew :modules:account-infra:test` → Expected: 컴파일 실패 (`holderNameOf` 없음).

- [ ] **Step 3: 구현 — 은행 응답 해석 · 계좌 마스킹**

`BankResponses.kt` (`apps/api/…/config/AppConfiguration.kt` 의 `BankRejectedException` · `BankGatewayAdapter` 해석 로직을 옮긴다):

```kotlin
package io.github.hyeokjinon.banktransfer.account.infra

import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse

/** account-infra 의 엔티티 · 저장소 패키지. 두 앱이 @AutoConfigurationPackage 로 JPA 스캔에 더한다 (앱 패키지 밖이라). */
const val ACCOUNT_INFRA_PACKAGE = "io.github.hyeokjinon.banktransfer.account.infra"

/** 은행이 처리를 거절한 경우(응답코드가 0000 이 아님). */
class BankRejectedException(val responseCode: String) : RuntimeException("bank rejected with code $responseCode")

/**
 * 1원인증 성명조회 응답 해석 (P6-2 — MVC 어댑터 · WebFlux 두 흐름이 같이 쓴다). 0000 이면 예금주명, 계좌 없음이면 null, 그 밖은 거절.
 * 막는 일을 하지 않는 짧은 계산이라 어느 스레드(게이트웨이 이벤트 루프 포함)에서 불러도 된다.
 */
fun holderNameOf(res: NameInquiryResponse): String? = when (res.header.responseCode) {
    ResponseCodes.OK -> res.holderName
    ResponseCodes.ACCOUNT_NOT_FOUND -> null
    else -> throw BankRejectedException(res.header.responseCode)
}

/** 1원 이체 응답이 0000 이 아니면 거절. */
fun requireOk(res: TransferResponse) {
    if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
}

/** 로그용 계좌 표시 — 끝 4자리만 (CLAUDE.md: 로그에 계좌 원문을 남기지 않는다). */
fun maskAccount(accountNo: String): String = "****" + accountNo.takeLast(4)
```

- [ ] **Step 4: JPA 저장소 이동**

```bash
mkdir -p modules/account-infra/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/infra
git mv apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/AccountVerificationPersistence.kt \
  modules/account-infra/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/infra/AccountVerificationPersistence.kt
```

옮긴 파일에서:
- `package io.github.hyeokjinon.banktransfer.api.persistence` → `package io.github.hyeokjinon.banktransfer.account.infra`
- `internal fun Instant.toDbPrecision()` → `private fun Instant.toDbPrecision()`. KDoc 끝에 한 줄: "apps/api 의 다른 저장소는 api/persistence/DbPrecision.kt 의 같은 함수를 쓴다 — 두 모듈이 서로 의존하지 않게 한 줄을 따로 둔다."
- 클래스 · 쿼리 · 주석은 그대로.

`apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/DbPrecision.kt` (출금 · 원장 저장소가 계속 쓴다):

```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import java.time.Instant
import java.time.temporal.ChronoUnit

/** DATETIME(6) 은 마이크로초까지만 담는다. 저장 전에 맞춰 잘라야 저장한 값과 다시 읽은 값이 같다. (1원인증 저장소는 account-infra 에 같은 함수가 있다) */
internal fun Instant.toDbPrecision(): Instant = truncatedTo(ChronoUnit.MICROS)
```

- [ ] **Step 5: 점검 가드 이동**

```bash
git mv apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/BankMaintenanceGuard.kt \
  modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankMaintenanceGuard.kt
```

옮긴 파일의 `package` 를 `io.github.hyeokjinon.banktransfer.gateway` 로 바꾸고, KDoc 끝에 한 줄을 더한다: "두 앱(apps/api · apps/api-reactive)의 웹 계층이 같이 쓴다 (P6-2 에서 bank-gateway 로 옮김)." `BankMaintenanceWindow` import 는 같은 패키지라 지운다.

- [ ] **Step 6: `apps/api` 를 새 위치에 잇는다**

`apps/api/build.gradle.kts` 의 `dependencies` 에 (기존 줄은 그대로):

```kotlin
    implementation(project(":modules:account-infra"))   // 1원인증 JPA 저장소 · 은행 응답 해석 (P6-2 에서 apps/api-reactive 와 같이 쓰려고 옮김)
    testImplementation(testFixtures(project(":modules:account-infra")))   // 오류 계약 기대표
```

`BankTransferApplication.kt`:

```kotlin
@SpringBootApplication
@ConfigurationPropertiesScan
// 1원인증 엔티티 · 저장소는 modules/account-infra (앱 패키지 밖)에 있다 — JPA 엔티티 · 저장소 스캔에 그 패키지를 더한다 (P6-2)
@AutoConfigurationPackage(basePackages = [ACCOUNT_INFRA_PACKAGE])
class BankTransferApplication
```
(import `org.springframework.boot.autoconfigure.AutoConfigurationPackage`, `io.github.hyeokjinon.banktransfer.account.infra.ACCOUNT_INFRA_PACKAGE`)

`config/AppConfiguration.kt`:
- `BankRejectedException` 클래스 정의를 지우고 `io.github.hyeokjinon.banktransfer.account.infra.BankRejectedException` 을 import (다른 파일이 이 이름을 쓰면 그쪽 import 도 고친다).
- `BankGatewayAdapter` 의 해석을 공용 함수로:

```kotlin
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res -> holderNameOf(res) }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo, references.next()).thenApplyAsync({ res -> requireOk(res) }, executor)
```
  (`ResponseCodes` import 가 다른 곳에서 안 쓰이면 지운다. KDoc 의 "thenApplyAsync 로 앱 실행기에 넘긴다" 설명은 그대로.)
- `AccountVerificationJpaRepository` · `JpaVerificationRepository` import 를 `…account.infra.` 로, `BankMaintenanceGuard` import 를 `…gateway.BankMaintenanceGuard` 로.

`web/AccountVerificationController.kt` · `web/TransferController.kt`: `import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard` 를 더한다 (전에는 같은 패키지라 import 가 없었다).

`web/ApiExceptionHandler.kt`: `BankRejectedException` · `BankMaintenanceException` import 를 새 위치로.

테스트: `./gradlew :apps:api:compileTestKotlin` 이 가리키는 파일의 **import 만** 고친다. 테스트 본문은 바꾸지 않는다.

- [ ] **Step 7: 오류 계약 기대표 (test fixtures)**

`VerificationErrorContract.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.account.infra.contract

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.infra.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceException
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.time.Duration

/**
 * 1원인증 요청 오류 계약 (P6-2, 설계 §4-4). apps/api(MVC) 와 apps/api-reactive(WebFlux) 가 같은 요청에 같은 상태 · code 를 돌려줘야 한다 —
 * 웹 계층 코드는 공유하지 않으므로(API 가 다르다) 이 표 하나를 두 앱의 테스트가 같이 읽어 갈라짐을 잡는다.
 * - [httpCases]: 모의 은행으로 HTTP 재현이 되는 경우. 테스트 모의 은행은 [SILENT_ACCOUNT] 를 silentAccounts 로 받고, 게이트웨이 응답 시간 초과는 짧게(500ms) 둔다.
 * - [exceptionCases]: 모의 은행으로 재현이 어렵거나 시계가 필요한 경우 — 각 앱의 예외 처리기를 직접 불러 본다.
 */
object VerificationErrorContract {

    const val PATH = "/api/v1/account-verifications"
    const val SILENT_ACCOUNT = "9990000000001"
    const val USER = "contract-user-1"

    /** 성공 응답 본문의 키 — 인증코드는 없어야 한다. */
    val SUCCESS_FIELDS = setOf("id", "status", "expiresAt", "verifiedAt")

    data class HttpCase(val name: String, val userId: String?, val body: String, val status: Int, val code: String?)

    data class ExceptionCase(val exception: Exception, val status: Int, val code: String, val retryAfter: String? = null)

    private fun body(accountNo: String, holderName: String, bankCode: String = "090") =
        """{"bankCode":"$bankCode","accountNo":"$accountNo","holderName":"$holderName"}"""

    val httpCases: List<HttpCase> = listOf(
        HttpCase("X-User-Id 없음", null, body("1002123456789", "홍길동"), 400, "USER_ID_REQUIRED"),
        HttpCase("X-User-Id 형식 틀림", "bad id!", body("1002123456789", "홍길동"), 400, "USER_ID_REQUIRED"),
        HttpCase("헤더 없음 + 본문 틀림 → 헤더 오류가 먼저", null, body("1002123456789", "홍길동", bankCode = "9"), 400, "USER_ID_REQUIRED"),
        HttpCase("은행코드 형식 틀림", USER, body("1002123456789", "홍길동", bankCode = "9"), 400, "INVALID_REQUEST"),
        HttpCase("JSON 깨짐", USER, "{", 400, "INVALID_REQUEST"),
        HttpCase("없는 계좌", USER, body("5599000000001", "홍길동"), 404, "ACCOUNT_NOT_FOUND"),
        HttpCase("예금주 불일치", USER, body("3333012345678", "홍길동"), 422, "HOLDER_NAME_MISMATCH"),
        HttpCase("은행 무응답", USER, body(SILENT_ACCOUNT, "무응답"), 504, "BANK_TIMEOUT"),
        HttpCase("성공 (공백 무시)", USER, body("1002123456789", "홍 길동"), 201, null),
    )

    val exceptionCases: List<ExceptionCase> = listOf(
        ExceptionCase(AccountVerificationException.AccountNotFound(), 404, "ACCOUNT_NOT_FOUND"),
        ExceptionCase(AccountVerificationException.HolderNameMismatch(), 422, "HOLDER_NAME_MISMATCH"),
        ExceptionCase(BankRejectedException("9999"), 502, "BANK_REJECTED"),
        ExceptionCase(BankUnavailableException("bank connection is not active"), 503, "BANK_UNAVAILABLE"),
        ExceptionCase(BankNotSentException("bank maintenance window"), 503, "BANK_UNAVAILABLE"),
        ExceptionCase(BankMaintenanceException(120), 503, "BANK_MAINTENANCE", retryAfter = "120"),
        ExceptionCase(BankTimeoutException(1, Duration.ofSeconds(3)), 504, "BANK_TIMEOUT"),
        // 500 은 내부 메시지를 응답에 싣지 않는다 (detail = "internal error")
        ExceptionCase(IllegalStateException("jdbc:mysql://secret-host/db"), 500, "INTERNAL_ERROR"),
    )
}
```

- [ ] **Step 8: `apps/api` 계약 테스트 (지금 동작의 특성 테스트 — 바로 통과해야 한다)**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/VerificationErrorContractTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/** 1원인증 오류 계약 — MVC 쪽 (P6-2, 설계 §4-4). 같은 표를 apps/api-reactive 의 계약 테스트가 읽는다. */
class VerificationErrorContractTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `HTTP 로 재현되는 경우 - 상태와 code 가 표와 같다`() {
        for (case in VerificationErrorContract.httpCases) {
            val (status, body) = client.post().uri(VerificationErrorContract.PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers { h -> case.userId?.let { h.set(USER_ID_HEADER, it) } }
                .body(case.body)
                .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
            assertEquals(case.status, status, "${case.name}: $body")
            if (case.code == null) {
                assertEquals(VerificationErrorContract.SUCCESS_FIELDS, body.keys, "${case.name}: 성공 응답 키 (인증코드 없음)")
                assertEquals("SENT", body["status"])
            } else {
                assertEquals(case.code, body["code"], "${case.name}: $body")
            }
        }
    }

    @Test
    fun `예외에서 상태 · code · Retry-After 로 - 표와 같다`() {
        val handler = ApiExceptionHandler()
        for (case in VerificationErrorContract.exceptionCases) {
            val res = handler.handle(case.exception)
            assertEquals(case.status, res.statusCode.value(), case.exception.toString())
            assertEquals(case.code, res.body!!.properties?.get("code"), case.exception.toString())
            assertEquals(case.retryAfter, res.headers.getFirst(HttpHeaders.RETRY_AFTER), case.exception.toString())
            if (case.status == 500) assertEquals("internal error", res.body!!.detail)
        }
    }
}
```

- 지금 동작이 계약이다: 이 테스트가 실패하면 **앱을 고치지 말고** 표를 실제 MVC 응답에 맞춘 뒤(예: 성공 본문에 `verifiedAt` 키가 없으면 `SUCCESS_FIELDS` 에서 뺀다, 본문 검증 실패의 code 가 다르면 그 값으로) 커밋 메시지에 "표를 MVC 실제 응답에 맞춤: …" 으로 적는다. 단, **응답에 인증코드가 보이면** 앱 버그다 — 멈추고 보고한다.
- `ApiExceptionHandler.handle` 이 `private`/다른 시그니처면 테스트가 부를 수 있게 `internal` 로 넓히지 말고, HTTP 로만 볼 수 있는 경우는 표에서 HTTP 케이스로 옮긴다 (지금 `fun handle(e: Exception)` 은 public 이다 — 코드 확인됨).

- [ ] **Step 9: 전체 테스트 (옮기기만 했으므로 모두 통과)**

Run: `./gradlew :modules:account-infra:test :modules:bank-gateway:test :apps:api:test` (`timeout` 600000)
Expected: 모두 PASS. `JpaVerificationRepositoryTest` 가 통과하면 엔티티 스캔(`@AutoConfigurationPackage`)이 붙은 것이다 — 실패 로그에 `Not a managed type: class …AccountVerificationEntity` 가 보이면 그 애너테이션이 빠진 것.

- [ ] **Step 10: Commit**

```bash
git add settings.gradle.kts modules/account-infra modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankMaintenanceGuard.kt apps/api
git status   # postman/ 이 섞이지 않았는지
git commit -m "refactor(p6-2): 1원인증 JPA 저장소 · 은행 응답 해석을 modules/account-infra 로, 점검 가드를 bank-gateway 로 — 오류 계약 기대표 · MVC 계약 테스트

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `apps/api` — `web-mode` (`async` | `blocking`), 컨트롤러 셋으로, actuator info, 실행 구성 10

스펙 §2 · §3-3 · §5: MVC 두 판. 비동기 판은 지금 코드 그대로 옮기기만 하고, 블로킹 판(대조군)을 더한다. "스펙과 다른 점" 9.

**Files:**
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (`VerificationProperties`)
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/AccountVerificationController.kt` (확인만 남김 — DTO 는 이 파일에 그대로)
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/AsyncVerificationRequestController.kt`
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/BlockingVerificationRequestController.kt`
- Modify: `apps/api/src/main/resources/application.yml`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/VerificationPropertiesTest.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/VerificationWebModeWiringTest.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/BlockingVerificationRequestTest.kt`
- Create: `.run/10. API (1원인증 블로킹).run.xml`

**Interfaces:**
- Consumes: `AccountVerificationService.request(RequestVerification): CompletableFuture<AccountVerification>` (Task 2 뒤에도 같은 시그니처), `BankMaintenanceGuard` (Task 3, gateway 패키지), `VerificationRequest` · `VerificationResponse` (같은 파일에 있음).
- Produces: `VerificationProperties.webMode: String` · 상수 `VerificationProperties.ASYNC = "async"` · `BLOCKING = "blocking"` · `WEB_MODES`. `/actuator/info` 의 `account.verification.web-mode` · `server.tomcat.threads.max` (Task 8 측정 스크립트가 읽는다).

- [ ] **Step 1: 실패하는 테스트 — 설정 검증**

`VerificationPropertiesTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class VerificationPropertiesTest {

    @Test
    fun `기본은 async`() {
        assertEquals("async", VerificationProperties().webMode)
    }

    @Test
    fun `소문자 그대로만 받는다 - 그 밖은 기동 실패`() {
        // @ConditionalOnProperty 는 대소문자를 무시하고 비교한다 — "Blocking" 을 받아 주면 판 이름과 info 표시가 어긋난다 (P6-1 과 같은 이유)
        for (bad in listOf("Blocking", "ASYNC", "coroutine", "")) {
            assertFailsWith<IllegalArgumentException>(bad) { VerificationProperties(webMode = bad) }
        }
        assertEquals("blocking", VerificationProperties(webMode = "blocking").webMode)
    }
}
```

Run: `./gradlew :apps:api:test --tests '*VerificationPropertiesTest*'` → Expected: 컴파일 실패 (`webMode` 없음).

- [ ] **Step 2: 구현 — 설정**

`AppConfiguration.kt` 의 `VerificationProperties` 를 바꾼다:

```kotlin
@ConfigurationProperties("account.verification")
data class VerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val maxAttempts: Int = 5,
    /**
     * 1원인증 요청 판 (P6-2, ADR-0013): async(기본, 서블릿 비동기) | blocking(대조군 — 요청 스레드가 은행을 끝까지 기다린다, 측정 전용).
     * 소문자 그대로만 받는다: @ConditionalOnProperty 는 대소문자를 무시해 "Blocking" 도 블로킹 컨트롤러를 띄우지만, info · 측정 이름과 어긋난다.
     */
    val webMode: String = ASYNC,
) {
    init {
        require(webMode in WEB_MODES) { "account.verification.web-mode must be one of $WEB_MODES (lower case): '$webMode'" }
    }

    companion object {
        const val ASYNC = "async"
        const val BLOCKING = "blocking"
        val WEB_MODES = listOf(ASYNC, BLOCKING)
    }
}
```

Run: 같은 명령 → Expected: PASS.

- [ ] **Step 3: 실패하는 테스트 — 조립 · info (공유 컨텍스트, 기본 async)**

`VerificationWebModeWiringTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 기본 설정의 1원인증 요청 판 (P6-2). 측정 스크립트(load/verification/run.sh)는 /actuator/info 로 띄운 판을 확인한 뒤에만 잰다. */
class VerificationWebModeWiringTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext

    @Test
    fun `기본 설정이면 비동기 컨트롤러 하나만 뜨고 actuator info 에 판 · Tomcat 최대 스레드가 보인다`() {
        assertEquals(1, context.getBeanNamesForType(AsyncVerificationRequestController::class.java).size)
        assertEquals(0, context.getBeanNamesForType(BlockingVerificationRequestController::class.java).size)
        assertEquals(1, context.getBeanNamesForType(AccountVerificationController::class.java).size)   // 확인은 판과 상관없이 늘 뜬다

        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!

        assertTrue("\"web-mode\":\"async\"" in info, info)
        assertTrue(Regex("\"max\":\"?200\"?").containsMatchIn(info), info)
    }
}
```

Run: `./gradlew :apps:api:test --tests '*VerificationWebModeWiringTest*'` → Expected: 컴파일 실패 (`AsyncVerificationRequestController` 없음).

- [ ] **Step 4: 구현 — 컨트롤러 셋**

`AccountVerificationController.kt` 에서 `request` 메서드와 그 KDoc 을 지우고(DTO · `confirm` 은 그대로), 클래스 KDoc 을 단다:

```kotlin
/** 1원인증 확인 (P1 · P4-1 · P5-1). 요청(POST)은 판(account.verification.web-mode)에 따라 [AsyncVerificationRequestController] 또는 [BlockingVerificationRequestController] 가 받는다 (P6-2). */
@RestController
@RequestMapping("/api/v1/account-verifications")
class AccountVerificationController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {
    // confirm(...) 그대로
}
```

`AsyncVerificationRequestController.kt` (지금의 `request` 를 그대로 옮긴다):

```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.api.config.VerificationProperties
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture

/** 1원인증 요청 — 기본 판 (account.verification.web-mode = async). */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = VerificationProperties.ASYNC, matchIfMissing = true)
class AsyncVerificationRequestController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /**
     * CompletableFuture 를 돌려주면 Spring MVC 가 서블릿 비동기 모드로 처리한다.
     * 은행 응답을 기다리는 동안 Tomcat 요청 스레드는 풀로 돌아간다. 인증 건에는 요청자(X-User-Id)를 남긴다 (P4-1).
     * 클라이언트가 응답 전에 끊어도 future 는 끝까지 간다 — 1원 이체 · 저장까지 (P6-2 실험 E2, WebFlux 판은 취소된다).
     */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): CompletableFuture<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()   // 점검 중에는 성명조회 · 1원 이체를 보내지 않는다 (P5-1) — 비동기로 넘기기 전에 거절한다
        return service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .thenApply { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }
}
```

`BlockingVerificationRequestController.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.api.config.VerificationProperties
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * 1원인증 요청 — 대조군 (P6-2, account.verification.web-mode = blocking, 측정 전용 · 운영에서 쓰지 않는다).
 * 같은 서비스를 부르고 요청 스레드(Tomcat)가 join() 으로 은행 두 번(성명조회 · 1원 이체)을 끝까지 기다린다 —
 * 동시에 기다릴 수 있는 요청 수가 Tomcat 최대 스레드 수(기본 200)에 묶인다. 비동기 판과 다른 것은 이 join 하나뿐이다.
 */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = VerificationProperties.BLOCKING)
class BlockingVerificationRequestController(private val service: AccountVerificationService, private val maintenance: BankMaintenanceGuard) {

    /** join 이 던지는 CompletionException 은 ApiExceptionHandler 가 벗긴다 — 오류 응답은 비동기 판과 같다. */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): ResponseEntity<VerificationResponse> {
        maintenance.ensureOpen()
        val verification = service.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName)).join()
        return ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(verification))
    }
}
```

`application.yml` — 먼저 읽고 합친다. `account.verification` 아래에 한 줄, `info` 아래에 두 묶음을 더한다:

```yaml
account:
  verification:
    ttl: 5m
    max-attempts: 5
    web-mode: async            # 1원인증 요청 판 (P6-2): async(기본) | blocking(대조군, 측정 전용). 소문자만, 그 밖은 기동 실패

info:
  # (기존 wallet · transfer · datasource 는 그대로)
  account:
    verification:
      web-mode: ${account.verification.web-mode}   # 지금 1원인증 요청 판 (P6-2, load/verification/run.sh 가 측정 전에 확인)
  server:
    tomcat:
      threads:
        max: ${server.tomcat.threads.max:200}       # 블로킹 판의 천장 = 이 수 (원인 확인 실험에서 400 으로 띄운다)
```

Run: `./gradlew :apps:api:test --tests '*VerificationWebModeWiringTest*' --tests '*AccountVerificationApiTest*' --tests '*VerificationErrorContractTest*' --tests '*BankMaintenanceApiTest*'`
Expected: PASS (요청 · 확인 · 오류 · 점검이 컨트롤러를 나눈 뒤에도 같다).

- [ ] **Step 5: 실패하는 테스트 — 블로킹 판 (별도 컨텍스트)**

`BlockingVerificationRequestTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 1원인증 요청 블로킹 판 (P6-2 대조군). 판이 설정으로 갈려 이 클래스만 별도 컨텍스트다 — 끝나면 닫는다.
 * Tomcat 최대 스레드를 2로 줄여 "요청 스레드가 은행을 끝까지 기다린다" 를 시간으로 본다: 은행이 한 번에 250ms 걸리면 요청 하나 ≈ 500ms,
 * 동시 4건은 스레드 2개에 두 번 나뉘어 ≈ 1초. 비동기 판이면 스레드를 놓아 ≈ 0.5초에 다 끝난다 (변이 검사: join → future 반환이면 실패해야 한다).
 * 공유 모의 은행의 지연은 게이트웨이 응답 시간 초과(테스트 500ms)보다 짧게 두고 끝나면 되돌린다.
 */
@TestPropertySource(properties = ["account.verification.web-mode=blocking", "server.tomcat.threads.max=2"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BlockingVerificationRequestTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun request(userId: String): Int =
        client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() }

    @Test
    fun `blocking 이면 블로킹 컨트롤러 하나만 뜨고 info 에 보인다`() {
        assertEquals(1, context.getBeanNamesForType(BlockingVerificationRequestController::class.java).size)
        assertEquals(0, context.getBeanNamesForType(AsyncVerificationRequestController::class.java).size)
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"web-mode\":\"blocking\"" in info, info)
        assertTrue(Regex("\"max\":\"?2\"?").containsMatchIn(info), info)
    }

    @Test
    fun `응답 · 저장 · 확인은 비동기 판과 같다`() {
        val user = newUserId()
        val (status, created) = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
        assertEquals(201, status)
        assertEquals("SENT", created["status"])

        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        val confirmed = client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("code" to code))
            .exchange { _, res -> res.bodyTo(mapType) ?: emptyMap() }
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `Tomcat 스레드 2개면 은행을 기다리는 4건이 두 번에 나뉘어 끝난다 - 요청 스레드를 붙잡는다`() {
        request(newUserId())   // 데우기 (첫 요청의 클래스 로딩 · 커넥션이 재는 시간에 섞이지 않게)
        bank.responseDelay = Duration.ofMillis(250)
        val pool = Executors.newFixedThreadPool(4)
        try {
            val started = System.nanoTime()
            val results = (1..4).map { pool.submit<Int> { request(newUserId()) } }.map { it.get(10, TimeUnit.SECONDS) }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertEquals(listOf(201, 201, 201, 201), results)
            println("[P6-2 블로킹 판] Tomcat 스레드 2 · 은행 250ms × 2 · 동시 4건 → ${elapsedMs}ms")
            assertTrue(elapsedMs >= 900, "스레드 2개가 두 번에 나눠 처리해야 한다 (≈ 1000ms): ${elapsedMs}ms")
        } finally {
            bank.responseDelay = Duration.ZERO
            pool.shutdownNow()
        }
    }
}
```

Run: `./gradlew :apps:api:test --tests '*BlockingVerificationRequestTest*'`
Expected: 3개 PASS (컨트롤러가 이미 있으므로 바로 통과한다 — **변이로 확인**: `BlockingVerificationRequestController.request` 를 비동기 판처럼 `CompletableFuture` 를 돌려주게 잠깐 바꾸면 세 번째 테스트가 `elapsedMs >= 900` 에서 실패해야 한다. 확인 뒤 되돌린다. 결과(밀리초 두 값)를 커밋 메시지에 적는다.)

- [ ] **Step 6: 실행 구성 10**

`.run/10. API (1원인증 블로킹).run.xml` — `.run/7. API (코루틴 컨슈머).run.xml` 을 복사해 두 곳만 바꾼다:
- `name="10. API (1원인증 블로킹)"`
- `scriptParameters` 값 `--args=&quot;--account.verification.web-mode=blocking&quot;`

- [ ] **Step 7: 테스트 · Commit**

Run: `./gradlew :apps:api:test` (`timeout` 600000) → Expected: 모두 PASS.

```bash
git add apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt \
  apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/AccountVerificationController.kt \
  apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/AsyncVerificationRequestController.kt \
  apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/BlockingVerificationRequestController.kt \
  apps/api/src/main/resources/application.yml \
  apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/VerificationPropertiesTest.kt \
  apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/VerificationWebModeWiringTest.kt \
  apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/web/BlockingVerificationRequestTest.kt \
  ".run/10. API (1원인증 블로킹).run.xml"
git commit -m "feat(p6-2): apps/api 1원인증 요청 판 web-mode = async | blocking — 블로킹 대조군 컨트롤러, info 에 판 · Tomcat 최대 스레드, 실행 구성 10

변이 확인: join → future 반환이면 동시 4건 <비동기 ms>ms (블로킹 <블로킹 ms>ms) 로 테스트 실패.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `apps/api-reactive` — 설정 · 게이트웨이 · 웹 계층 · 코루틴 판 (`CoroutineVerificationFlow`), 계약 테스트

스펙 §3 · §4 · §5: 리액티브 앱을 실제로 뜨게 하고 기본 판(코루틴)을 만든다. 오류 응답은 계약 기대표(Task 3)로 MVC 와 같은지 본다.

**Files (모두 `apps/api-reactive/` 아래):**
- Modify: `build.gradle.kts` (account-infra · test fixtures 의존)
- Modify: `src/main/kotlin/…/reactive/ReactiveApiApplication.kt` (`@AutoConfigurationPackage`)
- Modify: `src/main/resources/application.yml`
- Create: `src/main/kotlin/…/reactive/config/ReactiveProperties.kt`
- Create: `src/main/kotlin/…/reactive/config/ReactiveConfiguration.kt`
- Create: `src/main/kotlin/…/reactive/flow/CoroutineVerificationFlow.kt`
- Create: `src/main/kotlin/…/reactive/web/RequestUser.kt`
- Create: `src/main/kotlin/…/reactive/web/VerificationDtos.kt`
- Create: `src/main/kotlin/…/reactive/web/ReactiveExceptionHandler.kt`
- Create: `src/main/kotlin/…/reactive/web/CoroutineVerificationController.kt`
- Create (test support): `src/test/kotlin/…/reactive/support/MySqlTestContainer.kt` · `TestClock.kt` · `LogCapture.kt` · `ReactiveIntegrationTest.kt` · `VerificationSuite.kt`
- Create (tests): `src/test/kotlin/…/reactive/config/ReactiveVerificationPropertiesTest.kt` · `flow/CoroutineVerificationFlowTest.kt` · `web/CoroutineModeTest.kt`
- Create: `.run/8. API 리액티브 (코루틴).run.xml` (저장소 루트)

**Interfaces:**
- Consumes: `VerificationRequestRules` · `VerificationDraft` (Task 2), `JpaVerificationRepository` · `AccountVerificationJpaRepository` · `holderNameOf` · `requireOk` · `maskAccount` · `BankRejectedException` · `ACCOUNT_INFRA_PACKAGE` · test fixtures `VerificationErrorContract` (Task 3), `BankMaintenanceGuard` · `BankMaintenanceException` · `BankMaintenanceWindow` · `BankGatewayClient` · `BankGatewayConfig` · `BankTimeoutException` · `BankUnavailableException` (bank-gateway), `RandomReferenceGenerator(prefix = 'V', clock)` · `ReferenceGenerator` (transfer).
- Produces (Task 6 · 7 이 쓴다):
  - `ReactiveVerificationProperties(ttl, webMode)` · `COROUTINE = "coroutine"` · `REACTOR = "reactor"` · `WEB_MODES`
  - `class CoroutineVerificationFlow(client: BankGatewayClient, rules: VerificationRequestRules, repository: VerificationRepository, references: ReferenceGenerator, io: CoroutineDispatcher = Dispatchers.IO)` · `suspend fun request(command: RequestVerification): AccountVerification` · 상수 `DISCONNECTED_DURING_TRANSFER` (경고 로그 형식)
  - 빈: `clock` · `bankMaintenanceWindow` · `bankMaintenanceGuard` · `bankGatewayClient` · `verificationRepository` · `verificationRequestRules` · `verificationReferences` · `coroutineVerificationFlow`
  - 웹: `RequestUser` · `USER_ID_HEADER` · `UserIdRequiredException` · `VerificationRequest` · `VerificationResponse` · `ReactiveExceptionHandler.handle(e: Exception): ResponseEntity<ProblemDetail>`
  - 테스트 지원: `ReactiveIntegrationTest` (`bank` · `port` · `client` · `gateway` · `jdbc` · `testClock` · `newUserId()` · `rowsOf(userId)`), 추상 `VerificationSuite` (계약 테스트 2개 — Task 6 의 Reactor 판이 상속하고 Task 7 이 끊김 테스트를 더한다)

- [ ] **Step 1: 의존 · 앱 · 설정 파일**

`apps/api-reactive/build.gradle.kts` 의 `dependencies` 에 (Task 1 줄은 그대로):

```kotlin
    implementation(project(":modules:account-infra"))   // 1원인증 JPA 저장소 · 은행 응답 해석 — apps/api 와 같은 코드 (P6-2)
    testImplementation(testFixtures(project(":modules:account-infra")))   // 오류 계약 기대표
```

`ReactiveApiApplication.kt` 의 애너테이션:

```kotlin
@SpringBootApplication
@ConfigurationPropertiesScan
// 1원인증 엔티티 · 저장소는 modules/account-infra (앱 패키지 밖) — JPA 스캔에 더한다 (apps/api 와 같다)
@AutoConfigurationPackage(basePackages = [ACCOUNT_INFRA_PACKAGE])
class ReactiveApiApplication
```

`application.yml` (Task 1 내용과 합친다 — 값은 `apps/api/src/main/resources/application.yml` 의 같은 키와 같게):

```yaml
spring:
  application:
    name: bank-transfer-reactive
  main:
    web-application-type: reactive   # 명시: 클래스패스에 spring-web(RestClient 등)도 있어 서블릿으로 오인되지 않게
  webflux:
    problemdetails:
      enabled: true                  # 표준 오류도 ProblemDetail + code (ReactiveExceptionHandler) — apps/api 와 같은 응답 모양
  datasource:
    # apps/api 와 같은 DB · 같은 시간대 설정 (docs/database.md §2). 스키마는 apps/api 의 Flyway 가 만든다
    url: jdbc:mysql://${MYSQL_HOST:localhost}:${MYSQL_PORT:3306}/${MYSQL_DATABASE:bank_transfer}?connectionTimeZone=Asia/Seoul&forceConnectionTimeZoneToSession=true
    username: ${MYSQL_USER:bank}
    password: ${MYSQL_PASSWORD:local-dev-only}
    hikari:
      maximum-pool-size: 20          # apps/api 와 같게 (P6-2 — 풀 크기가 다르면 측정이 풀을 재게 된다)
      connection-init-sql: SET SESSION innodb_lock_wait_timeout = 5
  jpa:
    hibernate:
      ddl-auto: validate             # 스키마는 바꾸지 않는다. 엔티티와 다르면 기동 실패
    properties:
      hibernate.jdbc.time_zone: Asia/Seoul
      hibernate.type.preferred_instant_jdbc_type: TIMESTAMP

server:
  port: 8081                          # apps/api(8080)와 나란히 띄운다

bank:
  gateway:
    host: localhost
    port: 9090
    response-timeout: 3s
    poll-interval: 30s
  maintenance:                        # apps/api 와 같은 점검 시간 (P5-1, KST)
    enabled: true
    start: "23:50"
    end: "00:10"

account:
  verification:
    ttl: 5m
    web-mode: coroutine               # 1원인증 요청 판 (P6-2): coroutine(기본) | reactor. 소문자만, 그 밖은 기동 실패

management:
  endpoints:
    web:
      exposure:
        include: health,info
  info:
    env:
      enabled: true

info:
  account:
    verification:
      web-mode: ${account.verification.web-mode}   # load/verification/run.sh 가 측정 전에 확인
  datasource:
    maximum-pool-size: ${spring.datasource.hikari.maximum-pool-size}
```

- [ ] **Step 2: 실패하는 테스트 — 설정 검증**

`ReactiveVerificationPropertiesTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ReactiveVerificationPropertiesTest {

    @Test
    fun `기본은 coroutine, 소문자 그대로만 받는다`() {
        assertEquals("coroutine", ReactiveVerificationProperties().webMode)
        assertEquals("reactor", ReactiveVerificationProperties(webMode = "reactor").webMode)
        for (bad in listOf("Reactor", "COROUTINE", "async", "")) {
            assertFailsWith<IllegalArgumentException>(bad) { ReactiveVerificationProperties(webMode = bad) }
        }
    }
}
```

Run: `./gradlew :apps:api-reactive:test --tests '*ReactiveVerificationPropertiesTest*'` → Expected: 컴파일 실패.

- [ ] **Step 3: 구현 — 설정 · 조립**

`config/ReactiveProperties.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.config

import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

/** 1원인증 (P6-2). [webMode] = coroutine(기본) | reactor — 소문자 그대로만 (apps/api 의 VerificationProperties 와 같은 이유). */
@ConfigurationProperties("account.verification")
data class ReactiveVerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val webMode: String = COROUTINE,
) {
    init {
        require(webMode in WEB_MODES) { "account.verification.web-mode must be one of $WEB_MODES (lower case): '$webMode'" }
    }

    companion object {
        const val COROUTINE = "coroutine"
        const val REACTOR = "reactor"
        val WEB_MODES = listOf(COROUTINE, REACTOR)
    }
}

/** apps/api 의 같은 키 · 같은 기본값 (결정(계획): 20줄짜리라 모듈로 빼지 않고 앱마다 둔다). */
@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** apps/api 와 같다 — KST 벽시계 "HH:mm", 시작 포함 · 끝 제외 (YAML 에서는 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class BankMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}
```

`config/ReactiveConfiguration.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.config

import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.account.infra.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.account.infra.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.reactive.flow.CoroutineVerificationFlow
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock

/** 조립은 여기서만 (apps/api 의 AppConfiguration 과 같은 원칙). 은행 · 점검 · 저장소는 apps/api 와 같은 클래스를 같은 설정으로 쓴다. */
@Configuration(proxyBeanMethods = false)
class ReactiveConfiguration {

    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun bankMaintenanceWindow(props: BankMaintenanceProperties): BankMaintenanceWindow = props.toWindow()

    @Bean
    fun bankMaintenanceGuard(window: BankMaintenanceWindow, clock: Clock) = BankMaintenanceGuard(window, clock)

    /** 점검 중에는 전문을 보내지 않는다 (P5-1) — 웹 가드가 먼저 막아도 여기가 마지막 보장이다 (apps/api 와 같다). */
    @Bean(destroyMethod = "close")
    fun bankGatewayClient(props: BankGatewayProperties, maintenance: BankMaintenanceWindow, clock: Clock): BankGatewayClient =
        BankGatewayClient(
            BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval),
            sendGate = { if (maintenance.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate, clock: Clock): VerificationRepository =
        JpaVerificationRepository(jpa, tx, clock)

    @Bean
    fun verificationRequestRules(props: ReactiveVerificationProperties, clock: Clock) =
        VerificationRequestRules(codeGenerator = RandomCodeGenerator(), clock = clock, ttl = props.ttl)

    /** 1원인증 이체의 이용기관 거래번호 (V 접두, 저장 안 함) — apps/api 와 같은 생성기. */
    @Bean
    fun verificationReferences(clock: Clock): ReferenceGenerator = RandomReferenceGenerator(prefix = 'V', clock = clock)

    @Bean
    fun coroutineVerificationFlow(
        client: BankGatewayClient,
        rules: VerificationRequestRules,
        repository: VerificationRepository,
        references: ReferenceGenerator,
    ) = CoroutineVerificationFlow(client, rules, repository, references)
}
```

(`TransactionTemplate` 빈은 Boot 의 트랜잭션 자동 설정이 만든다 — apps/api 도 같은 빈을 받는다. 리액티브 앱에서 없다고 나오면 `@Bean fun transactionTemplate(tm: PlatformTransactionManager) = TransactionTemplate(tm)` 을 더하고 커밋 메시지에 적는다.)

Run: Step 2 명령 → Expected: PASS.

- [ ] **Step 4: 실패하는 테스트 — 코루틴 흐름 (진짜 모의 은행 · 게이트웨이, 가짜 저장소)**

`flow/CoroutineVerificationFlowTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.InMemoryVerificationRepository
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 코루틴 판 흐름 (P6-2, 설계 §3-3 · §4-2 · §4-3). 은행은 진짜 모의 은행 · 게이트웨이, 저장소는 저장한 스레드를 기록하는 가짜. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CoroutineVerificationFlowTest {

    private class RecordingRepository(private val inner: InMemoryVerificationRepository = InMemoryVerificationRepository()) :
        VerificationRepository by inner {
        val saveThreads = CopyOnWriteArrayList<String>()
        override fun save(verification: AccountVerification): AccountVerification =
            inner.save(verification).also { saveThreads += Thread.currentThread().name }
    }

    private val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val ioExecutor = Executors.newSingleThreadExecutor { Thread(it, "test-io") }
    private val io = ioExecutor.asCoroutineDispatcher()
    private lateinit var repository: RecordingRepository
    private lateinit var flow: CoroutineVerificationFlow

    @BeforeEach
    fun setUp() {
        bank.responseDelay = Duration.ZERO
        repository = RecordingRepository()
        flow = CoroutineVerificationFlow(gateway, VerificationRequestRules(codeGenerator = { "1234" }), repository, RandomReferenceGenerator(prefix = 'V'), io)
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { gateway.close(); bank.close(); io.close() }

    private fun command(accountNo: String, name: String) = RequestVerification("user-1", "090", accountNo, name)

    @Test
    fun `예금주가 맞으면 1원을 보내고 IO 디스패처에서 저장한다 - 이벤트 루프에서 JDBC 를 기다리지 않는다`() = runBlocking {
        val before = bank.depositsOf("1002123456789").size

        val v = flow.request(command("1002123456789", "홍 길동"))

        assertEquals("홍길동", v.holderName)
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertEquals("BT1234", bank.depositsOf("1002123456789").last().memo)
        assertTrue(bank.depositsOf("1002123456789").last().reference.startsWith("V"), "1원인증 거래번호는 V 접두")
        assertEquals(listOf("test-io"), repository.saveThreads.map { it.substringBefore(" @coroutine") })
    }

    @Test
    fun `없는 계좌면 AccountNotFound, 1원을 보내지 않는다`() = runBlocking {
        assertFailsWith<AccountVerificationException.AccountNotFound> { flow.request(command("5599000000001", "홍길동")) }
        assertTrue(repository.saveThreads.isEmpty())
    }

    @Test
    fun `예금주가 다르면 HolderNameMismatch, 1원을 보내지 않는다`() = runBlocking {
        val before = bank.depositsOf("3333012345678").size
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { flow.request(command("3333012345678", "홍길동")) }
        assertEquals(before, bank.depositsOf("3333012345678").size)
    }

    @Test
    fun `은행 무응답이면 BankTimeoutException 그대로`() = runBlocking {
        assertFailsWith<BankTimeoutException> { flow.request(command("9990000000001", "무응답")) }
        Unit
    }

    @Test
    fun `1원 이체를 기다리는 중 취소되면 CancellationException 그대로 - 오류로 바꾸지 않고 저장하지 않는다`() = runBlocking {
        bank.responseDelay = Duration.ofMillis(300)
        val before = bank.depositsOf("1002123456789").size
        val job = async { flow.request(command("1002123456789", "홍길동")) }
        while (bank.depositsOf("1002123456789").size == before) Thread.sleep(5)   // 은행이 1원 이체를 받았다 = 응답 대기 중
        job.cancel()

        assertFailsWith<CancellationException> { job.await() }
        Thread.sleep(500)   // 응답(300ms)이 지나도
        assertTrue(repository.saveThreads.isEmpty(), "취소된 요청은 저장하지 않는다")
    }
}
```

Run: `./gradlew :apps:api-reactive:test --tests '*CoroutineVerificationFlowTest*'` → Expected: 컴파일 실패 (`CoroutineVerificationFlow` 없음).

(`runBlocking` 블록의 마지막 식이 `Unit` 이 아니면 JUnit 이 테스트로 보지 않을 수 있다 — 위처럼 `Unit` 을 두거나 `: Unit = runBlocking { … }` 로 쓴다. `while (…) Thread.sleep` 은 `runBlocking` 스레드를 막지만 `async` 는 같은 스레드의 이벤트 루프라 첫 `await()` 지점까지 먼저 가야 한다 — 막히면 `async(Dispatchers.Default)` 로 바꾼다.)

- [ ] **Step 5: 구현 — 코루틴 흐름**

`flow/CoroutineVerificationFlow.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.account.infra.holderNameOf
import io.github.hyeokjinon.banktransfer.account.infra.maskAccount
import io.github.hyeokjinon.banktransfer.account.infra.requireOk
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * 1원인증 요청 — WebFlux 코루틴 판 (P6-2, ADR-0013). 판정은 [VerificationRequestRules] (네 판 공용), 여기는 기다리는 방법만 정한다.
 * - 은행 두 번(성명조회 · 1원 이체)은 await — 기다리는 동안 스레드를 잡지 않는다.
 * - await 뒤는 은행 응답을 완료한 게이트웨이 이벤트 루프에서 이어진다 (실험 E1). 거기서 JDBC 를 기다리면 그 루프의 모든 은행 응답이 멈추므로
 *   저장만 [io] 로 넘긴다. 해석 · 비교는 짧은 계산이라 루프에서 해도 된다.
 * - 클라이언트가 끊으면 WebFlux 가 코루틴을 취소한다 (실험 E2). 취소는 다시 던진다 — 오류 응답으로 바꾸지 않는다(보낼 곳이 없다).
 *   1원 이체를 기다리는 중 취소되면 1원이 나갔을 수 있어 경고를 남긴다. 사용자는 다시 요청한다 (설계 §4-3: 대가는 최악에 1원 두 번).
 */
class CoroutineVerificationFlow(
    private val client: BankGatewayClient,
    private val rules: VerificationRequestRules,
    private val repository: VerificationRepository,
    private val references: ReferenceGenerator,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun request(command: RequestVerification): AccountVerification {
        val found = holderNameOf(client.inquireName(command.bankCode, command.accountNo).await())
        val holderName = rules.checkHolder(found, command.holderName)
        val draft = rules.newDraft()
        try {
            requireOk(client.transfer(command.bankCode, command.accountNo, 1, draft.memo, references.next()).await())
        } catch (e: CancellationException) {
            log.warn(DISCONNECTED_DURING_TRANSFER, command.bankCode, maskAccount(command.accountNo))
            throw e
        }
        return withContext(io) { repository.save(rules.issue(draft, command, holderName)) }
    }

    companion object {
        /** 1원 이체 응답을 기다리는 중 취소 (클라이언트 끊김 등). 계좌는 마스킹 — Task 7 끊김 테스트가 이 형식을 본다. */
        const val DISCONNECTED_DURING_TRANSFER =
            "1원인증 요청 취소 (stage=transfer) — 1원 이체 응답을 기다리는 중이라 1원이 나갔을 수 있다. 사용자는 다시 요청한다: bank={} account={}"
    }
}
```

Run: Step 4 명령 → Expected: 5개 PASS.

- [ ] **Step 6: 웹 계층 — 사용자 · DTO · 오류 처리 · 컨트롤러**

`web/RequestUser.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import org.springframework.context.annotation.Configuration
import org.springframework.core.MethodParameter
import org.springframework.web.reactive.BindingContext
import org.springframework.web.reactive.config.WebFluxConfigurer
import org.springframework.web.reactive.result.method.HandlerMethodArgumentResolver
import org.springframework.web.reactive.result.method.annotation.ArgumentResolverConfigurer
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono

const val USER_ID_HEADER = "X-User-Id"

/** 요청한 사용자 — apps/api 의 RequestUser 와 같은 규칙(영숫자 · 하이픈 1~36자). 웹 계층은 앱마다 따로 두고 계약 테스트로 맞춘다 (P6-2). */
data class RequestUser(val id: String)

class UserIdRequiredException : RuntimeException("$USER_ID_HEADER header is required (1-36 letters, digits or hyphens)")

/**
 * WebFlux 판 인자 해석기. 컨트롤러는 [RequestUser] 를 **첫 인자**로 받는다 — WebFlux 는 인자들을 차례로 구독해 묶으므로(zip) 먼저 실패한 헤더 오류가
 * 본문 검증보다 먼저 응답이 된다 (apps/api 와 같은 순서, 계약 테스트 "헤더 없음 + 본문 틀림" 이 지킨다).
 */
class RequestUserResolver : HandlerMethodArgumentResolver {

    override fun supportsParameter(parameter: MethodParameter): Boolean = parameter.parameterType == RequestUser::class.java

    override fun resolveArgument(parameter: MethodParameter, bindingContext: BindingContext, exchange: ServerWebExchange): Mono<Any> {
        val value = exchange.request.headers.getFirst(USER_ID_HEADER)
        return if (value == null || !USER_ID.matches(value)) Mono.error(UserIdRequiredException()) else Mono.just(RequestUser(value))
    }

    private companion object {
        val USER_ID = Regex("[A-Za-z0-9-]{1,36}")
    }
}

@Configuration(proxyBeanMethods = false)
class WebConfiguration : WebFluxConfigurer {
    override fun configureArgumentResolvers(configurer: ArgumentResolverConfigurer) {
        configurer.addCustomResolver(RequestUserResolver())
    }
}
```

`web/VerificationDtos.kt` — `apps/api/…/web/AccountVerificationController.kt` 의 `VerificationRequest` · `VerificationResponse` 를 그대로 옮겨 적는다 (애너테이션 · 메시지 · `from()` 같게, 패키지만 `…reactive.web`). 파일 머리에 한 줄: `// apps/api 의 같은 DTO — 웹 계층은 공유하지 않고 계약 테스트(VerificationErrorContract)로 맞춘다 (P6-2)`.

`web/ReactiveExceptionHandler.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.infra.BankRejectedException
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.util.concurrent.CompletionException

/**
 * 예외 → 상태 + ProblemDetail (P6-2). apps/api 의 ApiExceptionHandler 에서 1원인증 요청에 해당하는 부분만 같은 규칙으로 옮겼다 —
 * 같은 요청에 같은 상태 · code 인지는 계약 테스트(VerificationErrorContract)가 본다.
 * CancellationException(클라이언트 끊김)은 여기서 다루지 않는다: 응답을 보낼 곳이 없고, 오류로 바꾸면 취소가 사라진다.
 */
@RestControllerAdvice
class ReactiveExceptionHandler : ResponseEntityExceptionHandler() {
    private val log = LoggerFactory.getLogger(javaClass)

    /** WebFlux 표준 예외(본문 깨짐 · 검증 실패 …)에도 code — 400 = INVALID_REQUEST, 그 밖 4xx = 상태 이름, 5xx = INTERNAL_ERROR (apps/api 와 같다). */
    override fun handleExceptionInternal(
        ex: Exception,
        body: Any?,
        headers: HttpHeaders?,
        status: HttpStatusCode,
        exchange: ServerWebExchange,
    ): Mono<ResponseEntity<Any>> =
        super.handleExceptionInternal(ex, body, headers, status, exchange).map { response ->
            val problem = response.body as? ProblemDetail
            if (problem != null && problem.properties?.containsKey("code") != true) {
                problem.setProperty(
                    "code",
                    when {
                        status.is5xxServerError -> "INTERNAL_ERROR"
                        status.value() == 400 -> "INVALID_REQUEST"
                        else -> HttpStatus.resolve(status.value())?.name ?: "ERROR"
                    },
                )
            }
            response
        }

    @ExceptionHandler(
        CompletionException::class,
        AccountVerificationException::class,
        BankTimeoutException::class,
        BankUnavailableException::class,
        BankRejectedException::class,
        BankMaintenanceException::class,
        UserIdRequiredException::class,
    )
    fun handle(e: Exception): ResponseEntity<ProblemDetail> {
        val cause = if (e is CompletionException) e.cause ?: e else e
        val (status, code) = when (cause) {
            is AccountVerificationException.AccountNotFound -> HttpStatus.NOT_FOUND to "ACCOUNT_NOT_FOUND"
            is AccountVerificationException.HolderNameMismatch -> HttpStatus.UNPROCESSABLE_CONTENT to "HOLDER_NAME_MISMATCH"
            is BankTimeoutException -> HttpStatus.GATEWAY_TIMEOUT to "BANK_TIMEOUT"
            is BankUnavailableException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_UNAVAILABLE"
            is BankRejectedException -> HttpStatus.BAD_GATEWAY to "BANK_REJECTED"
            is BankMaintenanceException -> HttpStatus.SERVICE_UNAVAILABLE to "BANK_MAINTENANCE"
            is UserIdRequiredException -> HttpStatus.BAD_REQUEST to "USER_ID_REQUIRED"
            else -> {
                log.error("unexpected error", cause)
                HttpStatus.INTERNAL_SERVER_ERROR to "INTERNAL_ERROR"
            }
        }
        // 500 은 내부 메시지(SQL · 경로 등)를 응답에 싣지 않는다
        val detail = if (status == HttpStatus.INTERNAL_SERVER_ERROR) "internal error" else cause.message ?: code
        val problem = ProblemDetail.forStatusAndDetail(status, detail).apply { setProperty("code", code) }
        return ResponseEntity.status(status)
            .headers { h -> (cause as? BankMaintenanceException)?.let { h.set(HttpHeaders.RETRY_AFTER, it.retryAfterSeconds.toString()) } }
            .body(problem)
    }
}
```

(`handleExceptionInternal` 의 Kotlin 시그니처(널 가능 여부)가 컴파일러와 다르면 시그니처만 맞춘다 — 확인 결과 5. `HttpStatus.UNPROCESSABLE_CONTENT` 는 apps/api 가 이미 쓴다.)

`web/CoroutineVerificationController.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.reactive.config.ReactiveVerificationProperties
import io.github.hyeokjinon.banktransfer.reactive.flow.CoroutineVerificationFlow
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/** 1원인증 요청 — WebFlux 코루틴 판 (기본, account.verification.web-mode = coroutine). 응답 · 오류는 apps/api 와 같다. */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = ReactiveVerificationProperties.COROUTINE, matchIfMissing = true)
class CoroutineVerificationController(private val flow: CoroutineVerificationFlow, private val maintenance: BankMaintenanceGuard) {

    @PostMapping
    suspend fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): ResponseEntity<VerificationResponse> {
        maintenance.ensureOpen()   // 점검 중에는 은행에 보내지 않는다 (P5-1)
        val verification = flow.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
        return ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(verification))
    }
}
```

- [ ] **Step 7: 테스트 지원 (복사 + 기반 클래스)**

`support/MySqlTestContainer.kt` · `support/LogCapture.kt` · `support/TestClock.kt`: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/` 의 같은 이름 파일을 **그대로 복사하고 패키지만** `io.github.hyeokjinon.banktransfer.reactive.support` 로 바꾼다 (`TestClock.kt` 는 `TestClock` · `TestClockConfiguration` 둘 다). 파일마다 KDoc 첫 줄 뒤에 "apps/api 의 같은 파일을 복사 (P6-2 — 테스트 지원은 앱마다 둔다)" 한 줄을 더한다.

`support/ReactiveIntegrationTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.support

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.UUID

/**
 * apps/api-reactive 통합 테스트의 부모 (apps/api 의 ApiIntegrationTest 와 같은 원칙): 설정이 같으면 컨텍스트 · MySQL 컨테이너 · 모의 은행이 한 번만 뜬다.
 * 스키마는 apps/api 의 마이그레이션으로 Flyway 가 만든다 (build.gradle.kts 의 spring.flyway.locations). 테스트끼리 DB 를 공유하므로 사용자는 테스트마다 새로.
 * 모의 은행 지연을 바꾼 테스트가 다음 테스트에 남기지 않게 끝날 때 되돌린다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlTestContainer::class, TestClockConfiguration::class)
abstract class ReactiveIntegrationTest {

    companion object {
        val bank: MockBankServer = MockBankServer(silentAccounts = setOf(VerificationErrorContract.SILENT_ACCOUNT)).start()

        @JvmStatic
        @DynamicPropertySource
        fun bankProperties(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.port") { bank.boundPort }
            registry.add("bank.gateway.response-timeout") { "500ms" }
        }
    }

    @Value("\${local.server.port}")
    protected var port: Int = 0

    @Autowired protected lateinit var gateway: BankGatewayClient
    @Autowired protected lateinit var jdbc: JdbcTemplate
    @Autowired protected lateinit var testClock: TestClock

    protected val client: RestClient by lazy { RestClient.create("http://localhost:$port") }

    protected fun newUserId(): String = UUID.randomUUID().toString()

    protected fun rowsOf(userId: String): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE user_id = ?", Int::class.java, userId)!!

    @BeforeEach
    fun waitForBankConnection() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterEach
    fun resetShared() {
        testClock.release()
        bank.responseDelay = Duration.ZERO
    }
}
```

`support/VerificationSuite.kt` — 두 판(코루틴 · Reactor)이 상속하는 공통 테스트 (Task 7 이 끊김 테스트를 여기에 더한다):

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.support

import io.github.hyeokjinon.banktransfer.account.infra.contract.VerificationErrorContract
import io.github.hyeokjinon.banktransfer.reactive.web.ReactiveExceptionHandler
import io.github.hyeokjinon.banktransfer.reactive.web.USER_ID_HEADER
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import kotlin.test.Test
import kotlin.test.assertEquals

/** WebFlux 두 판이 같이 지켜야 하는 것 (P6-2). 판마다 이 클래스를 상속한 테스트 클래스가 하나씩 있다 (CoroutineModeTest · ReactorModeTest). */
abstract class VerificationSuite : ReactiveIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `오류 계약 - HTTP 로 재현되는 경우 상태와 code 가 apps-api 와 같은 표를 따른다`() {
        for (case in VerificationErrorContract.httpCases) {
            val (status, body) = client.post().uri(VerificationErrorContract.PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers { h -> case.userId?.let { h.set(USER_ID_HEADER, it) } }
                .body(case.body)
                .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
            assertEquals(case.status, status, "${case.name}: $body")
            if (case.code == null) {
                assertEquals(VerificationErrorContract.SUCCESS_FIELDS, body.keys, "${case.name}: 성공 응답 키 (인증코드 없음)")
                assertEquals("SENT", body["status"])
            } else {
                assertEquals(case.code, body["code"], "${case.name}: $body")
            }
        }
    }

    @Test
    fun `오류 계약 - 예외에서 상태 · code · Retry-After 로`() {
        val handler = ReactiveExceptionHandler()
        for (case in VerificationErrorContract.exceptionCases) {
            val res = handler.handle(case.exception)
            assertEquals(case.status, res.statusCode.value(), case.exception.toString())
            assertEquals(case.code, res.body!!.properties?.get("code"), case.exception.toString())
            assertEquals(case.retryAfter, res.headers.getFirst(HttpHeaders.RETRY_AFTER), case.exception.toString())
            if (case.status == 500) assertEquals("internal error", res.body!!.detail)
        }
    }
}
```

- [ ] **Step 8: 판 테스트 — 코루틴 (공유 기본 컨텍스트)**

`web/CoroutineModeTest.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.reactive.support.VerificationSuite
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 기본 판(코루틴)의 조립 · 요청 · 점검 + 공통(VerificationSuite). */
class CoroutineModeTest : VerificationSuite() {

    @Autowired private lateinit var context: ApplicationContext

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun post(userId: String) = client.post().uri("/api/v1/account-verifications")
        .contentType(MediaType.APPLICATION_JSON)
        .header(USER_ID_HEADER, userId)
        .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
        .exchange { _, res -> Triple(res.statusCode.value(), res.headers, res.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `기본 설정이면 코루틴 컨트롤러 하나만 뜨고 info 에 판 · 커넥션 풀이 보인다`() {
        assertEquals(1, context.getBeanNamesForType(CoroutineVerificationController::class.java).size)
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"web-mode\":\"coroutine\"" in info, info)
        assertTrue(Regex("\"maximum-pool-size\":\"?20\"?").containsMatchIn(info), info)
    }

    @Test
    fun `요청하면 201 SENT, DB 에 요청자의 행, 은행에 코드를 실은 1원`() {
        val user = newUserId()
        val before = bank.depositsOf("1002123456789").size

        val (status, _, body) = post(user)

        assertEquals(201, status)
        assertEquals("SENT", body["status"])
        assertEquals(1, rowsOf(user))
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertTrue(bank.depositsOf("1002123456789").last().memo.startsWith("BT"))
    }

    @Test
    fun `점검 시간이면 503 BANK_MAINTENANCE + Retry-After, 은행에 보내지 않는다`() {
        val seoul = ZoneId.of("Asia/Seoul")
        testClock.fix(LocalDate.now(seoul).atTime(23, 55).atZone(seoul).toInstant())
        val before = bank.depositsOf("1002123456789").size

        val (status, headers, body) = post(newUserId())

        assertEquals(503, status)
        assertEquals("BANK_MAINTENANCE", body["code"])
        assertEquals((15 * 60).toString(), headers.getFirst(HttpHeaders.RETRY_AFTER))
        assertEquals(before, bank.depositsOf("1002123456789").size)
    }
}
```

(Retry-After 값은 23:55 → 00:10 = 900초다. `BankMaintenanceWindow.remainingSeconds` 가 올림이라 고정 시각이면 정확히 900 — 다르면 apps/api `BankMaintenanceApiTest` 의 단언 방식을 따른다.)

Run: `./gradlew :apps:api-reactive:test` (`timeout` 600000)
Expected: 실험 7개 · 설정 1 · 흐름 5 · 코루틴 판 3 + 계약 2 모두 PASS. 계약 테스트가 실패하면 **표가 아니라 리액티브 앱을 고친다** (표는 MVC 실제 응답 — Task 3).

- [ ] **Step 9: 실행 구성 8 · Commit**

`.run/8. API 리액티브 (코루틴).run.xml` — `.run/7. API (코루틴 컨슈머).run.xml` 을 복사해 바꾼다:
- `name="8. API 리액티브 (코루틴)"`
- `scriptParameters` 값을 빈 문자열(`value=""`)로
- `taskNames` 의 `:apps:api:bootRun` → `:apps:api-reactive:bootRun`

```bash
git add apps/api-reactive ".run/8. API 리액티브 (코루틴).run.xml"
git status
git commit -m "feat(p6-2): apps/api-reactive — WebFlux 코루틴 판 1원인증 요청 (await · IO 디스패처 저장 · 취소는 다시 던짐), 오류 계약 테스트, 실행 구성 8

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Reactor 판 — `ReactorVerificationFlow` · 컨트롤러, 별도 컨텍스트 판 테스트, 실행 구성 9

스펙 §3-3: 같은 흐름을 Reactor 연산자로. 사용자가 실무 Reactor 사용법을 같이 보려고 고른 판이다 — 코드 주석에 코루틴 판과의 대응(`await` ↔ `fromFuture`, `withContext(io)` ↔ `publishOn`)을 적는다.

**Files (모두 `apps/api-reactive/` 아래):**
- Create: `src/main/kotlin/…/reactive/flow/ReactorVerificationFlow.kt`
- Create: `src/main/kotlin/…/reactive/web/ReactorVerificationController.kt`
- Modify: `src/main/kotlin/…/reactive/config/ReactiveConfiguration.kt` (빈 하나)
- Create: `src/test/kotlin/…/reactive/flow/ReactorVerificationFlowTest.kt`
- Create: `src/test/kotlin/…/reactive/web/ReactorModeTest.kt`
- Create: `.run/9. API 리액티브 (Reactor).run.xml` (저장소 루트)

**Interfaces:**
- Consumes: Task 5 의 빈 · 웹 타입 · `VerificationSuite`, Task 3 의 `holderNameOf` · `requireOk` · `maskAccount`.
- Produces: `class ReactorVerificationFlow(client, rules, repository, references, scheduler: Scheduler = Schedulers.boundedElastic())` · `fun request(command: RequestVerification): Mono<AccountVerification>` · 상수 `DISCONNECTED_DURING_TRANSFER` (코루틴 판과 같은 형식). 빈 `reactorVerificationFlow`.

- [ ] **Step 1: 실패하는 테스트 — Reactor 흐름**

`flow/ReactorVerificationFlowTest.kt` — `CoroutineVerificationFlowTest` 와 같은 짜임(진짜 모의 은행 · 게이트웨이, 저장 스레드를 기록하는 가짜 저장소):

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.InMemoryVerificationRepository
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Reactor 판 흐름 (P6-2, 설계 §3-3 · §4-2 · §4-3). CoroutineVerificationFlowTest 와 같은 행동을 같은 방법으로 본다. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactorVerificationFlowTest {

    private class RecordingRepository(private val inner: InMemoryVerificationRepository = InMemoryVerificationRepository()) :
        VerificationRepository by inner {
        val saveThreads = CopyOnWriteArrayList<String>()
        override fun save(verification: AccountVerification): AccountVerification =
            inner.save(verification).also { saveThreads += Thread.currentThread().name }
    }

    private val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val elasticExecutor = Executors.newSingleThreadExecutor { Thread(it, "test-elastic") }
    private val scheduler = Schedulers.fromExecutorService(elasticExecutor)
    private lateinit var repository: RecordingRepository
    private lateinit var flow: ReactorVerificationFlow

    @BeforeEach
    fun setUp() {
        bank.responseDelay = Duration.ZERO
        repository = RecordingRepository()
        flow = ReactorVerificationFlow(gateway, VerificationRequestRules(codeGenerator = { "1234" }), repository, RandomReferenceGenerator(prefix = 'V'), scheduler)
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { gateway.close(); bank.close(); scheduler.dispose() }

    @Test
    fun `예금주가 맞으면 1원을 보내고 publishOn 스케줄러에서 저장한다 - 이벤트 루프에서 JDBC 를 기다리지 않는다`() {
        val before = bank.depositsOf("1002123456789").size

        val v = flow.request(command("1002123456789", "홍 길동")).block(Duration.ofSeconds(5))!!

        assertEquals("홍길동", v.holderName)
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertEquals(listOf("test-elastic"), repository.saveThreads)
    }

    @Test
    fun `없는 계좌 · 불일치 · 무응답은 같은 예외로 끝난다`() {
        assertFailsWith<AccountVerificationException.AccountNotFound> { flow.request(command("5599000000001", "홍길동")).block(Duration.ofSeconds(5)) }
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { flow.request(command("3333012345678", "홍길동")).block(Duration.ofSeconds(5)) }
        assertFailsWith<BankTimeoutException> { flow.request(command("9990000000001", "무응답")).block(Duration.ofSeconds(5)) }
        assertTrue(repository.saveThreads.isEmpty())
    }

    @Test
    fun `구독을 1원 이체 대기 중에 끊으면 저장하지 않는다`() {
        bank.responseDelay = Duration.ofMillis(300)
        val before = bank.depositsOf("1002123456789").size
        val subscription = flow.request(command("1002123456789", "홍길동")).subscribe()
        while (bank.depositsOf("1002123456789").size == before) Thread.sleep(5)

        subscription.dispose()

        Thread.sleep(500)
        assertTrue(repository.saveThreads.isEmpty(), "취소된 요청은 저장하지 않는다")
    }

    private fun command(accountNo: String, name: String) = RequestVerification("user-1", "090", accountNo, name)
}
```

(`block()` 은 `Mono` 의 오류가 `RuntimeException` 이면 그대로 던진다. 다르면 `reactor.core.Exceptions.unwrap` 으로 벗겨 단언한다.)

Run: `./gradlew :apps:api-reactive:test --tests '*ReactorVerificationFlowTest*'` → Expected: 컴파일 실패.

- [ ] **Step 2: 구현 — Reactor 흐름 · 빈 · 컨트롤러**

`flow/ReactorVerificationFlow.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.account.infra.holderNameOf
import io.github.hyeokjinon.banktransfer.account.infra.maskAccount
import io.github.hyeokjinon.banktransfer.account.infra.requireOk
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

/**
 * 1원인증 요청 — WebFlux Reactor 판 (P6-2, ADR-0013). 판정 · 해석 · 저장소는 코루틴 판과 같고, 기다리는 방법만 연산자로 쓴다.
 * 코루틴 판과의 대응:
 * - `client.xxx().await()`        ↔ `Mono.fromFuture { client.xxx() }` — 구독할 때 호출, 구독 취소 때 future 도 취소 (실험 E3)
 * - 다음 단계로 값 넘기기          ↔ `map` (짧은 계산) · `flatMap` (또 기다리는 것)
 * - `withContext(io) { save }`    ↔ `publishOn(boundedElastic())` 뒤 `map { save }` — 이 아래 연산자는 블로킹을 허용하는 스레드에서 돈다
 * - `catch (CancellationException)` ↔ `doOnCancel` — 클라이언트 끊김이 구독 취소로 온다 (실험 E2)
 * fromFuture 뒤 map 은 게이트웨이 이벤트 루프에서 돈다(실험 E1) — 그래서 저장 전에 publishOn 이 꼭 있어야 한다.
 */
class ReactorVerificationFlow(
    private val client: BankGatewayClient,
    private val rules: VerificationRequestRules,
    private val repository: VerificationRepository,
    private val references: ReferenceGenerator,
    private val scheduler: Scheduler = Schedulers.boundedElastic(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun request(command: RequestVerification): Mono<AccountVerification> =
        Mono.fromFuture { client.inquireName(command.bankCode, command.accountNo) }
            .map { res -> rules.checkHolder(holderNameOf(res), command.holderName) to rules.newDraft() }
            .flatMap { (holderName, draft) ->
                Mono.fromFuture { client.transfer(command.bankCode, command.accountNo, 1, draft.memo, references.next()) }
                    .map { res -> requireOk(res) }
                    .doOnCancel { log.warn(DISCONNECTED_DURING_TRANSFER, command.bankCode, maskAccount(command.accountNo)) }
                    .thenReturn(holderName to draft)
            }
            .publishOn(scheduler)
            .map { (holderName, draft) -> repository.save(rules.issue(draft, command, holderName)) }

    companion object {
        /** 코루틴 판(CoroutineVerificationFlow.DISCONNECTED_DURING_TRANSFER)과 같은 형식 — 끊김 테스트가 두 판에 같은 단언을 쓴다. */
        const val DISCONNECTED_DURING_TRANSFER = CoroutineVerificationFlow.DISCONNECTED_DURING_TRANSFER
    }
}
```

`ReactiveConfiguration.kt` 에 빈 하나:

```kotlin
    @Bean
    fun reactorVerificationFlow(
        client: BankGatewayClient,
        rules: VerificationRequestRules,
        repository: VerificationRepository,
        references: ReferenceGenerator,
    ) = ReactorVerificationFlow(client, rules, repository, references)
```

`web/ReactorVerificationController.kt`:

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.reactive.config.ReactiveVerificationProperties
import io.github.hyeokjinon.banktransfer.reactive.flow.ReactorVerificationFlow
import jakarta.validation.Valid
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

/** 1원인증 요청 — WebFlux Reactor 판 (account.verification.web-mode = reactor). 응답 · 오류는 코루틴 판 · apps/api 와 같다. */
@RestController
@RequestMapping("/api/v1/account-verifications")
@ConditionalOnProperty(prefix = "account.verification", name = ["web-mode"], havingValue = ReactiveVerificationProperties.REACTOR)
class ReactorVerificationController(private val flow: ReactorVerificationFlow, private val maintenance: BankMaintenanceGuard) {

    /** 점검 검사는 Mono 를 만들기 전에 던진다 — 처리기가 받아 503 (구독 뒤 오류로 흘려도 같은 응답이지만, 은행 호출을 만들지도 않게). */
    @PostMapping
    fun request(user: RequestUser, @Valid @RequestBody body: VerificationRequest): Mono<ResponseEntity<VerificationResponse>> {
        maintenance.ensureOpen()
        return flow.request(RequestVerification(user.id, body.bankCode, body.accountNo, body.holderName))
            .map { ResponseEntity.status(HttpStatus.CREATED).body(VerificationResponse.from(it)) }
    }
}
```

Run: Step 1 명령 → Expected: 3개 PASS.

- [ ] **Step 3: 판 테스트 — Reactor (별도 컨텍스트)**

`web/ReactorModeTest.kt` — `CoroutineModeTest` 와 같은 세 테스트를 Reactor 판으로 (조립 단언만 다르다. 판마다 같은 행동을 지키는 테스트라 본문을 같게 둔다):

```kotlin
package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.reactive.support.VerificationSuite
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Reactor 판 + 공통(VerificationSuite). 판이 설정으로 갈려 이 클래스만 별도 컨텍스트다 — 끝나면 닫는다. */
@TestPropertySource(properties = ["account.verification.web-mode=reactor"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReactorModeTest : VerificationSuite() {

    @Autowired private lateinit var context: ApplicationContext

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun post(userId: String) = client.post().uri("/api/v1/account-verifications")
        .contentType(MediaType.APPLICATION_JSON)
        .header(USER_ID_HEADER, userId)
        .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
        .exchange { _, res -> Triple(res.statusCode.value(), res.headers, res.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `reactor 면 Reactor 컨트롤러 하나만 뜨고 info 에 보인다`() {
        assertEquals(1, context.getBeanNamesForType(ReactorVerificationController::class.java).size)
        assertEquals(0, context.getBeanNamesForType(CoroutineVerificationController::class.java).size)
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"web-mode\":\"reactor\"" in info, info)
    }

    @Test
    fun `요청하면 201 SENT, DB 에 요청자의 행, 은행에 코드를 실은 1원`() {
        val user = newUserId()
        val before = bank.depositsOf("1002123456789").size

        val (status, _, body) = post(user)

        assertEquals(201, status)
        assertEquals("SENT", body["status"])
        assertEquals(1, rowsOf(user))
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertTrue(bank.depositsOf("1002123456789").last().memo.startsWith("BT"))
    }

    @Test
    fun `점검 시간이면 503 BANK_MAINTENANCE + Retry-After, 은행에 보내지 않는다`() {
        val seoul = ZoneId.of("Asia/Seoul")
        testClock.fix(LocalDate.now(seoul).atTime(23, 55).atZone(seoul).toInstant())
        val before = bank.depositsOf("1002123456789").size

        val (status, headers, body) = post(newUserId())

        assertEquals(503, status)
        assertEquals("BANK_MAINTENANCE", body["code"])
        assertEquals((15 * 60).toString(), headers.getFirst(HttpHeaders.RETRY_AFTER))
        assertEquals(before, bank.depositsOf("1002123456789").size)
    }
}
```

Run: `./gradlew :apps:api-reactive:test` (`timeout` 600000) → Expected: 모두 PASS (Reactor 판도 계약 2개를 지킨다).

- [ ] **Step 4: 실행 구성 9 · Commit**

`.run/9. API 리액티브 (Reactor).run.xml` — `8. API 리액티브 (코루틴)` 을 복사해 `name="9. API 리액티브 (Reactor)"`, `scriptParameters` 값 `--args=&quot;--account.verification.web-mode=reactor&quot;`.

```bash
git add apps/api-reactive ".run/9. API 리액티브 (Reactor).run.xml"
git commit -m "feat(p6-2): WebFlux Reactor 판 1원인증 요청 (fromFuture · flatMap · publishOn(boundedElastic) · doOnCancel), 판 테스트 · 계약, 실행 구성 9

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 끊김 테스트 (두 판) · 변이 검사 4종 · 전체 빌드

스펙 §4-3 · §7: HTTP 연결을 실제로 끊어 두 판의 동작(행 없음 · 경고 로그 · 게이트웨이 대기 표 정리)을 고정하고, 변이 검사로 테스트가 실제로 지키는지 본다.

**Files:**
- Modify: `apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/reactive/support/VerificationSuite.kt` (끊김 테스트 2개 + 추상 `flowLoggerName`)
- Modify: `apps/api-reactive/src/test/kotlin/…/reactive/web/CoroutineModeTest.kt` · `ReactorModeTest.kt` (`flowLoggerName` 구현 한 줄씩)

**Interfaces:**
- Consumes: `ReactiveIntegrationTest` (`bank` · `port` · `gateway.pendingCount` · `rowsOf` · `newUserId`), `LogCapture.kt` 의 logback `ListAppender` 방식, `CoroutineVerificationFlow` · `ReactorVerificationFlow` (로거 이름 = 클래스 이름).
- Produces: 없음 (테스트 · 검증만).

- [ ] **Step 1: 끊김 테스트 (공통 — 두 판이 상속)**

`VerificationSuite.kt` 에 더한다:

```kotlin
    /** 이 판의 흐름 클래스 로거 이름 (끊김 경고를 거기서 찾는다). */
    protected abstract val flowLoggerName: String

    /** 요청을 보내고 [until] 이 참이 될 때까지 응답을 읽지 않고 기다렸다가 연결을 끊는다. */
    private fun postAndDisconnect(userId: String, accountNo: String, holderName: String, until: () -> Boolean) {
        val bytes = """{"bankCode":"090","accountNo":"$accountNo","holderName":"$holderName"}""".toByteArray()
        java.net.Socket("localhost", port).use { s ->
            s.getOutputStream().write(
                ("POST /api/v1/account-verifications HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n" +
                    "$USER_ID_HEADER: $userId\r\nContent-Length: ${bytes.size}\r\n\r\n").toByteArray() + bytes,
            )
            s.getOutputStream().flush()
            val deadline = System.currentTimeMillis() + 5_000
            while (!until()) { check(System.currentTimeMillis() < deadline) { "기다리던 상태가 오지 않았다" }; Thread.sleep(5) }
        }
    }

    @Test
    fun `끊김 - 1원 이체 응답을 기다리는 중 끊으면 행이 없고, 마스킹한 경고가 남고, 대기 표가 비워진다`() {
        val logger = org.slf4j.LoggerFactory.getLogger(flowLoggerName) as ch.qos.logback.classic.Logger
        val appender = ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>().also { it.start() }
        logger.addAppender(appender)
        try {
            val user = newUserId()
            val before = bank.depositsOf("1002123456789").size
            bank.responseDelay = java.time.Duration.ofMillis(300)

            // 모의 은행은 이체를 받자마자 입금을 기록하고 응답만 늦게 보낸다 → 입금이 늘었다 = 1원 이체 응답을 기다리는 중
            postAndDisconnect(user, "1002123456789", "홍길동") { bank.depositsOf("1002123456789").size > before }

            org.awaitility.kotlin.await.atMost(java.time.Duration.ofSeconds(5)) untilAsserted {
                val warns = appender.list.filter { it.level == ch.qos.logback.classic.Level.WARN }
                assertEquals(1, warns.size, warns.toString())
                val line = warns.single().formattedMessage
                assertTrue("stage=transfer" in line && "****6789" in line, line)
                assertTrue("1002123456789" !in line, "계좌 원문을 남기지 않는다: $line")
            }
            Thread.sleep(500)   // 은행 응답(300ms)이 지난 뒤에도
            assertEquals(0, rowsOf(user), "끊긴 요청은 저장하지 않는다 (1원은 나갔다 — 설계 §4-3 의 대가)")
            assertEquals(before + 1, bank.depositsOf("1002123456789").size)
            org.awaitility.kotlin.await.atMost(java.time.Duration.ofSeconds(5)) untilAsserted { assertEquals(0, gateway.pendingCount) }
        } finally {
            logger.detachAppender(appender)
        }
    }

    @Test
    fun `끊김 - 성명조회를 기다리는 중 끊으면 1원을 보내지 않고 행도 없다`() {
        val user = newUserId()
        val before = bank.depositsOf("1002123456789").size
        bank.responseDelay = java.time.Duration.ofMillis(300)
        val started = System.currentTimeMillis()

        postAndDisconnect(user, "1002123456789", "홍길동") { System.currentTimeMillis() - started > 100 }   // 성명조회 응답(300ms) 전

        Thread.sleep(1_000)   // 성명조회 응답 · (보냈다면) 1원 이체가 끝났을 시간
        assertEquals(before, bank.depositsOf("1002123456789").size, "1원을 보내지 않는다")
        assertEquals(0, rowsOf(user))
    }
```

(위 코드는 이해를 돕게 전체 이름을 썼다 — 파일에서는 import 로 정리한다. `untilAsserted` 는 `org.awaitility.kotlin.untilAsserted` 중위 함수다. `assertTrue` · `assertEquals` 는 `kotlin.test`.)

`CoroutineModeTest` 에: `override val flowLoggerName: String = CoroutineVerificationFlow::class.java.name`
`ReactorModeTest` 에: `override val flowLoggerName: String = ReactorVerificationFlow::class.java.name`

Run: `./gradlew :apps:api-reactive:test --tests '*ModeTest*'` → Expected: 판마다 끊김 2개 포함 모두 PASS.

- 첫 테스트가 "경고 0개" 로 실패하면: 코루틴 판은 취소가 `await` 지점에서 `CancellationException` 으로 오는지(실험 E2), Reactor 판은 `doOnCancel` 이 `flatMap` 안쪽 `Mono` 에 붙어 있는지 본다. 경고가 2개면 바깥 연산자에도 `doOnCancel` 이 붙은 것.

- [ ] **Step 2: 변이 검사 4종 (커밋하지 않고 되돌린다)**

하나씩 바꾸고 → 지정한 테스트를 돌려 **실패하는지** 확인 → `git checkout -- <파일>` 로 되돌린다. 결과(실패한 테스트 이름)를 적어 둔다.

| # | 변이 | 돌릴 테스트 | 기대 |
|---|---|---|---|
| 1 | `CoroutineVerificationFlow.request` 의 `withContext(io) { … }` 를 지우고 `repository.save(…)` 만 남긴다 | `CoroutineVerificationFlowTest` | `IO 디스패처에서 저장한다` 실패 (저장 스레드가 게이트웨이 루프) |
| 2 | `ReactorVerificationFlow.request` 의 `.publishOn(scheduler)` 줄을 지운다 | `ReactorVerificationFlowTest` | `publishOn 스케줄러에서 저장한다` 실패 |
| 3 | `CoroutineVerificationFlow` 의 `catch (e: CancellationException)` 에서 `throw e` 대신 `throw BankUnavailableException("cancelled")` (취소를 오류로 바꿈) | `CoroutineVerificationFlowTest` | `취소되면 CancellationException 그대로` 실패 |
| 4 | `BlockingVerificationRequestController.request` 가 `join()` 대신 `CompletableFuture<ResponseEntity<…>>` 를 돌려주게 (비동기 판처럼) | `:apps:api:test --tests '*BlockingVerificationRequestTest*'` | `Tomcat 스레드 2개면 … 두 번에 나뉘어` 실패 |

넷 중 하나라도 테스트가 통과해 버리면(변이를 못 잡음) 그 테스트를 고쳐 잡게 만든 뒤 이 Step 을 다시 한다 — 변이를 잡는 이유를 테스트 KDoc 에 한 줄 적는다.

- [ ] **Step 3: 전체 빌드**

Run: `./gradlew build` (`timeout` 600000 — 넘으면 `run_in_background` 로 돌리고 끝나면 결과를 읽는다)
Expected: BUILD SUCCESSFUL, 실패 · 건너뜀 0. 테스트 수를 센다:

```bash
find . -path '*/build/test-results/test/*.xml' -newer build.gradle.kts -print0 | xargs -0 grep -ho 'tests="[0-9]*"' | grep -o '[0-9]*' | paste -sd+ | bc
```
(465 에서 늘어난 수 — Task 9 가 README · SETUP · HANDOVER 에 적는다. 위 명령이 이 PC 에서 안 되면 `build/reports/tests/test/index.html` 의 모듈별 합을 더한다.)

- [ ] **Step 4: Commit**

```bash
git add apps/api-reactive/src/test
git commit -F - <<'EOF'
test(p6-2): 끊김 테스트 (두 판 — 1원 이체 대기 중 끊김은 행 없음 · 마스킹 경고 · 대기 표 정리, 성명조회 중 끊김은 1원 없음) · 변이 검사 4종

변이 검사 (커밋하지 않고 되돌림) — 모두 테스트가 잡음:
1. withContext(io) 제거 → <테스트 이름>
2. publishOn 제거 → <테스트 이름>
3. 취소를 BankUnavailableException 으로 → <테스트 이름>
4. 블로킹 join → future 반환 → <테스트 이름>
전체 빌드: 테스트 <N>개, 실패 · 건너뜀 0

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 8: 측정 도구 — 모의 은행 입금 기록 O(1), `load/verification/` (k6 열린 모델 · 실행 · 정리 · 요약), 연기 측정

스펙 §6 · "스펙과 다른 점" 5 · 6 · 7. 실행자가 로컬 인프라를 쓰는 Task 다 — **측정 사용자 `p62-u001 ~ u100` 의 1원인증 행만 만들고 지운다.**

**Files:**
- Modify: `apps/mock-bank/src/main/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServer.kt`
- Modify: `apps/mock-bank/src/test/kotlin/io/github/hyeokjinon/banktransfer/mockbank/MockBankServerTest.kt` (없으면 그 모듈의 서버 테스트 파일 — `depositsOf` 를 부르는 테스트를 찾는다)
- Create: `load/verification/verification.js`
- Create: `load/verification/run.sh`
- Create: `load/verification/cleanup.sql`
- Create: `load/verification/summarize.sh`

**Interfaces:**
- Consumes: `/actuator/info` 의 `account.verification.web-mode` · `datasource.maximum-pool-size` (두 앱) · `server.tomcat.threads.max` (apps/api) — Task 4 · 5. `/actuator/metrics/{system.cpu.usage, process.cpu.usage, jvm.threads.live}`. 모의 은행 실행 인자 `<포트> --bank-files=<디렉터리> --response-delay=<n>ms`, 기동 로그 `mock bank listening on`, 거래 기록 `<bank-files>/journal/*.log` (1원인증 줄은 `DV` 로 시작). `infra/docker-compose.yml` 의 `k6` 서비스(`profiles: [load]`, `../load:/scripts:ro`, `../docs/perf/raw:/out`).
- Produces: `docs/perf/raw/p6-2/<이름>.json` (이름 = `<판>-d<지연>-r<도착률>[-t<톰캣>]-<회차>`), 키: `run mode app delay_ms rate tomcat_threads round rps_201 p50_201 p95_201 p99_201 status_201 status_4xx status_5xx timeouts dropped_iterations vus_max created_all timeouts_all rows one_won rows_minus_created one_won_minus_rows threads_max sys_cpu_avg sys_cpu_max app_cpu_avg app_cpu_max samples code`. Task 10 · `summarize.sh` 가 읽는다.

- [ ] **Step 1: 모의 은행 입금 기록 — O(1) 추가**

`MockBankServer.kt`:

```kotlin
    /**
     * 계좌별 입금 기록 (테스트가 적요의 코드를 꺼내 본다). 받은 순서대로 쌓는다.
     * ConcurrentLinkedQueue: 추가가 O(1) — CopyOnWriteArrayList 는 입금마다 목록 전체를 복사해, 측정처럼 한 계좌에 수만 건이 오면
     * 모의 은행 이벤트 루프가 그 복사에 시간을 쓴다 (P6-2 계획 "스펙과 다른 점" 7).
     */
    private val deposits = ConcurrentHashMap<String, ConcurrentLinkedQueue<TransferRequest>>()
```

`deposit()` 의 `computeIfAbsent(message.accountNo) { CopyOnWriteArrayList() }` 를 `{ ConcurrentLinkedQueue() }` 로. `depositsOf()` 는 그대로(`?.toList() ?: emptyList()` — 그때까지의 복사본, 받은 순서). 쓰지 않게 된 `CopyOnWriteArrayList` import 는 지운다(다른 곳에서 쓰면 둔다).

테스트 — 순서를 지키는지 (바꾸기 전에도 통과하는 특성 테스트, 바꾼 뒤에도 통과해야 한다). 모의 은행 테스트 파일에 더한다:

```kotlin
    @Test
    fun `입금 기록은 받은 순서대로다`() {
        MockBankServer().start().use { bank ->
            BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort)).start().use { client ->
                val deadline = System.currentTimeMillis() + 5_000
                while (!client.isConnected) { check(System.currentTimeMillis() < deadline) { "not connected" }; Thread.sleep(20) }
                for (i in 1..3) client.transfer("090", "1101234567890", 1, "ORDER$i", "V20261105ORDER00000$i").get(5, TimeUnit.SECONDS)
                assertEquals(listOf("ORDER1", "ORDER2", "ORDER3"), bank.depositsOf("1101234567890").map { it.memo })
            }
        }
    }
```

(모의 은행 모듈의 테스트가 게이트웨이를 쓸 수 없다면(의존 방향) 이 테스트는 `modules/bank-gateway/src/test` 의 게이트웨이 테스트 파일(이미 `testImplementation(project(":apps:mock-bank"))`)에 둔다. 거래번호는 20자 — 길이를 맞춘다.)

Run: `./gradlew :apps:mock-bank:test :modules:bank-gateway:test` → Expected: PASS.

- [ ] **Step 2: k6 스크립트**

`load/verification/verification.js`:

```javascript
// P6-2 웹 1원인증 측정 (설계 §6): 열린 모델 — 초당 RATE 건이 일정하게 도착한다 (constant-arrival-rate). 응답이 느려도 도착률은 그대로다.
// 시나리오 하나(워밍업 + 측정)로 돌리고, 요청마다 경과 시간으로 phase 태그를 붙여 측정 구간만 모은다 — 시나리오를 둘로 나누면 VU 풀이 둘이라 메모리가 두 배다.
// 환경변수는 load/verification/run.sh 가 넘긴다: RUN, BASE_URL, RATE, WARMUP_S(10), MEASURE_S(30), MAX_VUS(2000)
import http from 'k6/http';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL;
const RUN = __ENV.RUN;
const RATE = Number(__ENV.RATE);
const WARMUP_S = Number(__ENV.WARMUP_S || 10);
const MEASURE_S = Number(__ENV.MEASURE_S || 30);
const MAX_VUS = Number(__ENV.MAX_VUS || 2000);
// 모의 은행 기본 계좌 — 예금주가 맞아 모두 201 이 된다. 셋에 나눠 한 계좌의 입금 기록이 너무 길어지지 않게
const ACCOUNTS = [['1002123456789', '홍길동'], ['3333012345678', '김카뱅'], ['1101234567890', '이페이']];
const STATUSES = ['201', '400', '404', '422', '500', '502', '503', '504', '0'];   // 0 = k6 시간 초과 · 연결 오류

function thresholds() {
  // 판정용이 아니다: 하위 지표를 요약에 남기게 하는 장치 (load/common.js 와 같다)
  const t = {
    'http_req_duration{phase:measure,status:201}': ['max>=0'],
    'http_reqs{phase:measure}': ['count>=0'],
    'http_reqs{status:201}': ['count>=0'],   // 워밍업 포함 201 수 = DB 행 수와 맞춰 본다
    'http_reqs{status:0}': ['count>=0'],     // 워밍업 포함 시간 초과 수 (스펙과 다른 점 5)
  };
  for (const s of STATUSES) t[`http_reqs{phase:measure,status:${s}}`] = ['count>=0'];
  return t;
}

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  scenarios: {
    open: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${WARMUP_S + MEASURE_S}s`,
      preAllocatedVUs: Math.min(MAX_VUS, Math.max(50, RATE)),
      maxVUs: MAX_VUS,
      gracefulStop: '15s',   // 끝난 뒤 진행 중 요청을 기다린다 (k6 시간 초과 10초보다 길게) — 201 수와 DB 행 수를 맞춰 보기 때문
    },
  },
  thresholds: thresholds(),
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const user = `p62-u${String((i % 100) + 1).padStart(3, '0')}`;
  const [accountNo, holderName] = ACCOUNTS[i % ACCOUNTS.length];
  const phase = exec.instance.currentTestRunDuration < WARMUP_S * 1000 ? 'warmup' : 'measure';
  http.post(`${BASE_URL}/api/v1/account-verifications`, JSON.stringify({ bankCode: '090', accountNo, holderName }), {
    headers: { 'Content-Type': 'application/json', 'X-User-Id': user },
    timeout: '10s',
    tags: { phase },
  });
}

function values(data, metric, tags) {
  const want = tags.slice().sort().join(',');
  for (const name of Object.keys(data.metrics)) {
    if (!name.startsWith(`${metric}{`)) continue;
    const got = name.slice(metric.length + 1, -1).split(',').map((s) => s.trim()).sort().join(',');
    if (got === want) return data.metrics[name].values;
  }
  return null;
}

function count(data, tags) {
  const v = values(data, 'http_reqs', tags);
  return v ? v.count : 0;
}

export function handleSummary(data) {
  const ok = values(data, 'http_req_duration', ['phase:measure', 'status:201']) || {};
  const by = {};
  for (const s of STATUSES) by[s] = count(data, ['phase:measure', `status:${s}`]);
  const result = {
    rps_201: by['201'] / MEASURE_S,
    p50_201: ok.med, p95_201: ok['p(95)'], p99_201: ok['p(99)'],
    status_201: by['201'],
    status_4xx: by['400'] + by['404'] + by['422'],
    status_5xx: by['500'] + by['502'] + by['503'] + by['504'],
    timeouts: by['0'],
    dropped_iterations: data.metrics.dropped_iterations ? data.metrics.dropped_iterations.values.count : 0,
    vus_max: data.metrics.vus_max ? data.metrics.vus_max.values.max : 0,
    created_all: count(data, ['status:201']),
    timeouts_all: count(data, ['status:0']),
  };
  const f = (x) => (x === undefined ? '-' : x.toFixed(1));
  return {
    stdout: `[${RUN}] 201 ${f(result.rps_201)}/s · p50 ${f(result.p50_201)} · p95 ${f(result.p95_201)} · p99 ${f(result.p99_201)} ms · ` +
      `시간초과 ${result.timeouts} · dropped ${result.dropped_iterations} · VU 최대 ${result.vus_max}\n`,
    [`/out/p6-2/${RUN}.k6.json`]: JSON.stringify(result, null, 2),
  };
}
```

- [ ] **Step 3: 정리 SQL**

`load/verification/cleanup.sql`:

```sql
-- P6-2 1원인증 측정 데이터 지우기 (측정 사용자 100명만: p62-u001 ~ p62-u100). load/verification/run.sh 가 회차 앞에 부르고, 측정이 끝나면 한 번 더 부른다.
-- 사용자 조건: 접두사(LIKE)로 인덱스 범위를 줄이고 REGEXP 가 정확한 100명만 남긴다 (load/consumer/cleanup.sql 과 같은 방식 — 다른 행은 지우지 않는다).
-- 1원인증만 만든다 (지갑 · 출금 · 아웃박스 없음).
DELETE FROM account_verification
 WHERE user_id LIKE 'p62-u%' AND user_id REGEXP '^p62-u(00[1-9]|0[1-9][0-9]|100)$';
```

- [ ] **Step 4: 실행 스크립트**

`load/verification/run.sh`:

```bash
#!/usr/bin/env bash
# P6-2 웹 1원인증 측정 1회 (설계 §6): 정리 → 측정용 모의 은행(지연) → 앱(판) → k6 열린 모델(워밍업 10초 + 측정 30초) → 정확성.
# 결과: docs/perf/raw/p6-2/<이름>.json (이름 = <판>-d<지연>-r<도착률>[-t<톰캣>]-<회차>).
#       정확성 확인을 통과하기 전에 끝나면(어긋남 · 오류 · Ctrl+C) .json.invalid 로 바꾼다 — summarize.sh 는 .json 만 읽는다.
#       k6 요약 · 로그 · 표본은 build/load-p62/<이름>/ (git 제외).
#
# 사용 (Git Bash, 저장소 루트):
#   bash load/verification/run.sh <blocking|async|coroutine|reactor> <지연ms 0~2999> <도착률 1~5000> <회차> [톰캣 최대 스레드=200]
#   bash load/verification/run.sh cleanup      # 측정이 끝나면 반드시
# 먼저 (OPERATION §12): infra up (MySQL · Kafka — apps/api 는 리스너를 꺼도 기동 때 Kafka 관리 클라이언트가 붙는다),
#   ./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist
#   측정 포트 18080 · 18081 · 9190 은 비어 있어야 한다. 개발용 API(8080) · 개발용 은행(9090)은 켜 둬도 된다 — 측정 앱은 다른 포트 · 다른 은행을 쓰고,
#   apps/api 측정 인스턴스는 Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존 스케줄을 끈다 (개발용 메시지 · 행을 건드리지 않는다).
# 경고: 측정 데이터는 사용자 p62-u001 ~ u100 의 1원인증 행뿐이다 (SENT, 30일 뒤 보존 배치 대상). 끝나면 cleanup.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 가 /scripts/... 를 Windows 경로로 바꾸지 않게
cd "$(dirname "$0")/../.."

ENV_FILE=infra/.env
[ -f "$ENV_FILE" ] || ENV_FILE=infra/.env.example
dc() { docker compose -f infra/docker-compose.yml --env-file "$ENV_FILE" "$@"; }
# 비밀번호는 mysql 컨테이너가 가진 환경변수를 컨테이너 안 셸이 읽는다 — PC 쪽 명령행에 나타나지 않는다 (load/run.sh 와 같다)
sql() { dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -u"$MYSQL_USER" "$MYSQL_DATABASE" --default-character-set=utf8mb4 -N'; }
RUN=verification
die() { echo "[$RUN] $*" >&2; exit 1; }
port_busy() { netstat -ano 2>/dev/null | grep -E "TCP +[^ ]+:$1 +[^ ]+ +LISTENING" >/dev/null; }
USERS="user_id LIKE 'p62-u%' AND user_id REGEXP '^p62-u(00[1-9]|0[1-9][0-9]|100)\$'"   # cleanup.sql 과 같은 조건

if [ "${1:-}" = cleanup ]; then
  sql < load/verification/cleanup.sql
  echo "cleanup 완료 (측정 사용자 p62-u001 ~ u100 의 1원인증 행)"
  exit 0
fi

MODE=${1:?판: blocking | async | coroutine | reactor}
DELAY=${2:?은행 지연 ms: 0 ~ 2999}
RATE=${3:?도착률 (건/s): 1 ~ 5000}
ROUND=${4:?회차: 1, 2, 3, probe1, smoke …}
TOMCAT=${5:-200}
case "$MODE" in
  blocking|async) APP=api; PORT=18080 ;;
  coroutine|reactor) APP=api-reactive; PORT=18081 ;;
  *) echo "모르는 판: $MODE" >&2; exit 2 ;;
esac
{ [[ "$DELAY" =~ ^[0-9]{1,4}$ ]] && [ "$DELAY" -le 2999 ]; } || { echo "지연은 0 ~ 2999 (ms): $DELAY" >&2; exit 2; }
{ [[ "$RATE" =~ ^[0-9]{1,4}$ ]] && [ "$RATE" -ge 1 ] && [ "$RATE" -le 5000 ]; } || { echo "도착률은 1 ~ 5000: $RATE" >&2; exit 2; }
[[ "$ROUND" =~ ^[A-Za-z0-9_-]{1,16}$ ]] || { echo "회차는 영문 · 숫자 · - _ 16자 이내: $ROUND" >&2; exit 2; }
{ [[ "$TOMCAT" =~ ^[0-9]{1,4}$ ]] && [ "$TOMCAT" -ge 1 ]; } || { echo "톰캣 최대 스레드는 1 이상: $TOMCAT" >&2; exit 2; }
[ "$APP" = api ] || [ "$TOMCAT" = 200 ] || { echo "톰캣 스레드는 MVC 판(blocking · async)에만 준다" >&2; exit 2; }
NAME="$MODE-d$DELAY-r$RATE"
[ "$TOMCAT" != 200 ] && NAME="$NAME-t$TOMCAT"
NAME="$NAME-$ROUND"
RUN=$NAME
RAW=docs/perf/raw/p6-2
OUT="$RAW/$NAME.json"
WORK="build/load-p62/$NAME"
BANK_FILES="$WORK/bank-files"   # 회차마다 새로 — 1원 이체 수(DV 줄)를 이 회차만 센다. 개발용 ./bank-files 와 따로
BANK_PORT=9190
API="http://localhost:$PORT"
JAR="apps/$APP/build/libs/$APP-0.1.0-SNAPSHOT.jar"
BANK_CP="apps/mock-bank/build/install/mock-bank/lib/*"
WARMUP_S=10
MEASURE_S=30
[ -f "$JAR" ] || die "jar 가 없다 — ./gradlew :apps:$APP:bootJar"
ls apps/mock-bank/build/install/mock-bank/lib/*.jar >/dev/null 2>&1 || die "모의 은행 배포본이 없다 — ./gradlew :apps:mock-bank:installDist"

# --- 프로세스 · 정리 ---
APP_PID=; BANK_PID=; SAMPLER=
OK=0
STARTED=0   # 이전 결과를 지우는 지점부터 1 — 그 앞에서 멈추면 이전 회차 결과를 .invalid 로 바꾸지 않는다
stop_pid() { [ -n "${1:-}" ] || return 0; kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }
free_port() {   # 그 포트를 쥔 프로세스가 끝날 때까지 (최대 15초). kill 이 듣지 않았으면 Windows PID 로 강제 종료 (load/consumer/run.sh 와 같다)
  for _ in $(seq 15); do port_busy "$1" || return 0; sleep 1; done
  local wpid; wpid=$(netstat -ano | grep -E "TCP +[^ ]+:$1 +[^ ]+ +LISTENING" | awk '{ print $NF }' | head -1)
  [ -n "$wpid" ] && taskkill /F /PID "$wpid" >/dev/null 2>&1 || true
  sleep 1; ! port_busy "$1"
}
finish() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  stop_pid "$APP_PID"; stop_pid "$BANK_PID"
  free_port "$PORT" >/dev/null 2>&1 || true; free_port "$BANK_PORT" >/dev/null 2>&1 || true
  if [ "$OK" != 1 ] && [ "$STARTED" = 1 ] && [ -f "$OUT" ]; then mv -f "$OUT" "$OUT.invalid"; echo "[$RUN] 확인을 통과하지 못해 $OUT.invalid 로 바꿨다 — 이 회차 숫자는 쓰지 않는다" >&2; fi
  return 0
}
trap finish EXIT
trap 'exit 130' INT TERM

# --- 0. 사전 확인 (읽기만) ---
for p in 18080 18081 "$BANK_PORT"; do
  port_busy "$p" && die "포트 $p 를 이미 쓰고 있다 — 측정용 앱 · 모의 은행을 끈다 (아무것도 건드리지 않았다)"
done
mkdir -p "$RAW" "$WORK"
rm -f "$OUT" "$OUT.invalid" "$RAW/$NAME.k6.json"
STARTED=1
rm -rf "$BANK_FILES"

# --- 1. 정리 ---
echo "[$RUN] 1/6 정리 (측정 사용자 1원인증 행)"
sql < load/verification/cleanup.sql
LEFT=$(echo "SELECT COUNT(*) FROM account_verification WHERE $USERS" | sql)
[ "$LEFT" = 0 ] || die "정리 뒤에도 측정 사용자 행이 $LEFT 건 남았다"

# --- 2. 측정용 모의 은행 ---
echo "[$RUN] 2/6 모의 은행 :$BANK_PORT 지연 ${DELAY}ms"
java -Dstdout.encoding=UTF-8 -cp "$BANK_CP" io.github.hyeokjinon.banktransfer.mockbank.MockBankServerKt "$BANK_PORT" \
  --bank-files="$BANK_FILES" --response-delay="${DELAY}ms" > "$WORK/mock-bank.log" 2>&1 &
BANK_PID=$!
for _ in $(seq 30); do
  grep -q "mock bank listening on" "$WORK/mock-bank.log" && break
  kill -0 "$BANK_PID" 2>/dev/null || die "모의 은행이 기동 중에 끝났다 — $WORK/mock-bank.log"
  sleep 1
done
grep -q "mock bank listening on" "$WORK/mock-bank.log" || die "모의 은행이 30초 안에 뜨지 않았다"

# --- 3. 앱 ---
echo "[$RUN] 3/6 앱 $APP :$PORT 판 $MODE (톰캣 $TOMCAT)"
ARGS=(--server.port="$PORT" --bank.gateway.port="$BANK_PORT" --bank.maintenance.enabled=false
  --account.verification.web-mode="$MODE"
  --management.endpoints.web.exposure.include=health,info,metrics --logging.level.io.github.hyeokjinon.banktransfer=WARN)
if [ "$APP" = api ]; then
  # 개발용 컨슈머 그룹 메시지를 나눠 갖지 않게 · 개발 행을 건드리지 않게 (설계 §6 격리)
  ARGS+=(--spring.kafka.listener.auto-startup=false --transfer.outbox.relay.enabled=false --transfer.reconcile.enabled=false
    --daily-recon.schedule.enabled=false --data-retention.schedule.enabled=false --server.tomcat.threads.max="$TOMCAT")
fi
java -Xms1g -Xmx1g -Dstdout.encoding=UTF-8 -jar "$JAR" "${ARGS[@]}" > "$WORK/app.log" 2>&1 &
APP_PID=$!
for _ in $(seq 120); do
  curl -sf "$API/actuator/health" >/dev/null 2>&1 && break
  kill -0 "$APP_PID" 2>/dev/null || die "앱이 기동 중에 끝났다 — $WORK/app.log"
  sleep 1
done
curl -sf "$API/actuator/health" >/dev/null 2>&1 || die "앱이 120초 안에 뜨지 않았다 — $WORK/app.log"
INFO=$(curl -sf "$API/actuator/info") || die "actuator info 를 읽지 못했다"
grep -q "\"web-mode\":\"$MODE\"" <<<"$INFO" || die "앱의 판이 $MODE 가 아니다: $INFO"
grep -qE "\"maximum-pool-size\":\"?20\"?[,}]" <<<"$INFO" || die "앱의 Hikari 가 20 이 아니다: $INFO"
if [ "$APP" = api ]; then grep -qE "\"max\":\"?$TOMCAT\"?[,}]" <<<"$INFO" || die "앱의 톰캣 최대 스레드가 $TOMCAT 이 아니다: $INFO"; fi
sleep 3   # 게이트웨이가 측정 은행에 붙을 시간 (첫 접속 · 재접속 1초) — 워밍업 첫 요청의 503 을 줄인다 (워밍업은 집계하지 않는다)

# --- 4. 표본 (1초마다) ---
metric() {   # 읽지 못하면 '-' — 빈 값이면 열이 밀린다
  local x; x=$(curl -sf "$API/actuator/metrics/$1" | grep -o '"value":[-0-9.eE+]*' | head -1 | cut -d: -f2 || true)
  echo "${x:--}"
}
( while :; do echo "$(date +%s%3N) $(metric system.cpu.usage) $(metric process.cpu.usage) $(metric jvm.threads.live)"; sleep 1; done ) > "$WORK/samples.txt" 2>/dev/null &
SAMPLER=$!

# --- 5. k6 ---
echo "[$RUN] 4/6 k6 열린 모델 ${RATE}건/s (워밍업 ${WARMUP_S}s + 측정 ${MEASURE_S}s, 요청 시간 초과 10s)"
K6_START=$(date +%s%3N)
dc --profile load run --rm \
  -e RUN="$NAME" -e BASE_URL="http://host.docker.internal:$PORT" -e RATE="$RATE" \
  -e WARMUP_S="$WARMUP_S" -e MEASURE_S="$MEASURE_S" \
  k6 run --quiet /scripts/verification/verification.js
kill "$SAMPLER" 2>/dev/null || true; SAMPLER=
K6_JSON="$RAW/$NAME.k6.json"
[ -f "$K6_JSON" ] || die "k6 요약이 없다: $K6_JSON"
mv -f "$K6_JSON" "$WORK/k6.json"
K6_JSON="$WORK/k6.json"
WARNS=$(grep -cE " (WARN|ERROR) " "$WORK/app.log" || true)
stop_pid "$APP_PID"; APP_PID=; free_port "$PORT" || die "앱이 포트 $PORT 를 놓지 않는다"
stop_pid "$BANK_PID"; BANK_PID=; free_port "$BANK_PORT" || die "모의 은행이 포트 $BANK_PORT 를 놓지 않는다"

# --- 6. 계산 ---
echo "[$RUN] 5/6 계산"
k() { grep -o "\"$1\": *[-0-9.eE+]*" "$K6_JSON" | head -1 | sed 's/.*: *//'; }
# 측정 구간 표본만 (k6 시작 + 워밍업 ~ + 측정). docker run 의 컨테이너 기동만큼(1 ~ 2초, 추정) 앞당겨 잡힌다 — 결과 문서 조건 표에 적는다
S=$(( K6_START + WARMUP_S * 1000 )); E=$(( S + MEASURE_S * 1000 ))
stat() { awk -v s="$S" -v e="$E" -v c="$1" -v mul="$2" '$1 >= s && $1 <= e && $c ~ /^[0-9]/ { x = $c * mul; t += x; n++; if (x > m) m = x } END { if (n) printf "%.1f %.1f %d\n", t / n, m, n; else print "null null 0" }' "$WORK/samples.txt"; }
read -r SYS_AVG SYS_MAX SAMPLES < <(stat 2 100)
read -r APP_AVG APP_MAX _ < <(stat 3 100)
read -r _ THREADS_MAX _ < <(stat 4 1)
[ "${SAMPLES:-0}" -ge 20 ] || echo "[$RUN] 경고: 측정 구간 표본이 ${SAMPLES:-0}개 (< 20)" >&2

# --- 7. 정확성 (스펙과 다른 점 5) ---
echo "[$RUN] 6/6 정확성"
ROWS=$(echo "SELECT COUNT(*) FROM account_verification WHERE $USERS" | sql)
journal() { cat "$BANK_FILES"/journal/*.log 2>/dev/null || true; }
ONE_WON=$(journal | grep -c '^DV' || true)
CREATED=$(k created_all); TIMEOUTS=$(k timeouts_all)
RMC=$(( ROWS - CREATED )); OMR=$(( ${ONE_WON:-0} - ROWS ))
echo "[$RUN] 201 $CREATED · 행 $ROWS · 1원 ${ONE_WON:-0} · 시간 초과 $TIMEOUTS (행−201 $RMC, 1원−행 $OMR)"
cat > "$OUT" <<EOF
{
  "run": "$NAME", "mode": "$MODE", "app": "$APP", "delay_ms": $DELAY, "rate": $RATE, "tomcat_threads": $TOMCAT, "round": "$ROUND",
  "rps_201": $(k rps_201), "p50_201": $(k p50_201), "p95_201": $(k p95_201), "p99_201": $(k p99_201),
  "status_201": $(k status_201), "status_4xx": $(k status_4xx), "status_5xx": $(k status_5xx), "timeouts": $(k timeouts),
  "dropped_iterations": $(k dropped_iterations), "vus_max": $(k vus_max),
  "created_all": $CREATED, "timeouts_all": $TIMEOUTS, "rows": $ROWS, "one_won": ${ONE_WON:-0},
  "rows_minus_created": $RMC, "one_won_minus_rows": $OMR,
  "threads_max": $THREADS_MAX, "sys_cpu_avg": $SYS_AVG, "sys_cpu_max": $SYS_MAX, "app_cpu_avg": $APP_AVG, "app_cpu_max": $APP_MAX,
  "samples": $SAMPLES, "app_warn_error_lines": ${WARNS:-0}, "code": "$(git rev-parse --short HEAD)"
}
EOF
if [ "$TIMEOUTS" = 0 ]; then
  { [ "$RMC" = 0 ] && [ "$OMR" = 0 ]; } || die "정확성 확인 실패 — 시간 초과 0 인데 201 $CREATED · 행 $ROWS · 1원 ${ONE_WON:-0} 이 다르다"
else
  { [ "$RMC" -ge 0 ] && [ "$RMC" -le "$TIMEOUTS" ] && [ "$OMR" -ge 0 ] && [ "$OMR" -le "$TIMEOUTS" ]; } \
    || die "정확성 확인 실패 — 시간 초과 $TIMEOUTS 으로 설명되지 않는다 (행−201 $RMC, 1원−행 $OMR)"
  echo "[$RUN] 주의: k6 시간 초과 $TIMEOUTS 건 — 끊김 비용으로 기록 (행−201 $RMC: 끊겨도 저장 / 1원−행 $OMR: 끊겨 1원만)" >&2
fi
OK=1
echo "[$RUN] 끝 — 201 $(k rps_201)건/s · p99 $(k p99_201) ms · 스레드 최대 $THREADS_MAX · 시스템 CPU 평균 $SYS_AVG% → $OUT"
```

(`MockBankServerKt` 의 실행 클래스 이름 · 기동 로그 문구는 `load/consumer/run.sh` 와 같다 — 코드 확인됨. 실행 권한: `git update-index --chmod=+x load/verification/run.sh load/verification/summarize.sh`.)

- [ ] **Step 5: 요약 스크립트**

`load/verification/summarize.sh`:

```bash
#!/usr/bin/env bash
# P6-2 측정 결과를 표(마크다운)로 만든다 → docs/perf/p6-2-web-reactive.md 에 붙인다.
# 사용: bash load/verification/summarize.sh   (docs/perf/raw/p6-2/*.json — .invalid 는 제외)
# 회차가 숫자면 본 측정 · 원인 확인 표, probe 로 시작하면 한계 탐색 표, 그 밖(smoke)은 빠진다. 지표마다 따로 중앙값.
set -euo pipefail
cd "$(dirname "$0")/../.."
RAW=docs/perf/raw/p6-2

val() { grep -o "\"$2\": *[-0-9.eE+a-z]*" "$1" | head -1 | sed 's/.*: *//'; }
median() { sort -g | awk '{ v[NR] = $1 } END { if (NR == 0) print "nan"; else print v[int((NR + 1) / 2)] }'; }
row() {   # $1 = 파일 목록 (줄바꿈), 나머지 = 앞 칸들
  local files=$1; shift
  local n; n=$(wc -l <<<"$files" | tr -d ' ')
  local line="| $* | $n"
  for key in rps_201 p50_201 p95_201 p99_201 threads_max sys_cpu_avg app_cpu_avg; do
    line="$line | $(printf '%.1f' "$(for f in $files; do val "$f" "$key"; done | median)")"
  done
  for key in timeouts dropped_iterations status_5xx; do
    line="$line | $(printf '%.0f' "$(for f in $files; do val "$f" "$key"; done | median)")"
  done
  echo "$line |"
}
header() {
  echo "| $1 | 회차 | 201 처리량 (건/s) | p50 (ms) | p95 (ms) | p99 (ms) | 스레드 최대 | 시스템 CPU 평균 (%) | 앱 CPU 평균 (%) | 시간 초과 | dropped | 5xx |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
}

for d in 100 500; do
  echo; echo "### 은행 지연 ${d}ms (요청 하나 ≈ $(( d * 2 ))ms + α)"; echo
  header "판 · 도착률"
  for r in 100 200 400; do
    for m in blocking async coroutine reactor; do
      files=$(ls "$RAW/$m-d$d-r$r-"[0-9]*.json 2>/dev/null || true)
      [ -n "$files" ] && row "$files" "$m · $r"
    done
  done
done

echo; echo "### 원인 확인 — MVC 블로킹, Tomcat 최대 스레드 200 vs 400 (지연 500ms · 도착률 400)"; echo
header "톰캣 스레드"
f200=$(ls "$RAW/blocking-d500-r400-"[0-9]*.json 2>/dev/null || true); [ -n "$f200" ] && row "$f200" "200"
f400=$(ls "$RAW/blocking-d500-r400-t400-"[0-9]*.json 2>/dev/null || true); [ -n "$f400" ] && row "$f400" "400"

echo; echo "### 한계 탐색 — 비동기 세 판, 지연 500ms (회차마다 한 번)"; echo
header "판 · 도착률"
for m in async coroutine reactor; do
  for f in $(ls "$RAW/$m-d500-r"*"-probe"*.json 2>/dev/null | sort -t r -k2 -n || true); do
    row "$f" "$m · $(val "$f" rate)"
  done
done
```

- [ ] **Step 6: 연기 측정 (도구가 도는지만 — 숫자는 쓰지 않는다)**

```bash
export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"
docker compose -f infra/docker-compose.yml --env-file infra/.env ps   # mysql · kafka healthy 여야 한다 (없으면 사용자에게 infra up 을 부탁하고 멈춘다)
./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist
bash load/verification/run.sh async 100 50 smoke
bash load/verification/run.sh coroutine 100 50 smoke
bash load/verification/run.sh reactor 100 50 smoke
bash load/verification/run.sh blocking 100 50 smoke
bash load/verification/run.sh cleanup
bash load/verification/summarize.sh   # smoke 는 표에서 빠진다 — 머리줄만 나오면 정상
```
Expected: 네 회차 모두 `끝 — 201 ≈50건/s …` 와 `시간 초과 0`, `docs/perf/raw/p6-2/<판>-d100-r50-smoke.json` 이 생긴다. **연기 측정 결과 파일은 커밋하지 않는다** — 확인 뒤 `rm docs/perf/raw/p6-2/*-smoke.json`.

- 인프라(Docker · MySQL · Kafka)가 꺼져 있으면 띄우지 말고 BLOCKED 로 보고한다 (사용자의 개발 환경이다).
- 실패하면 `build/load-p62/<이름>/` 의 `app.log` · `mock-bank.log` · `k6.json` 을 읽고 고친다. 고친 내용을 커밋 메시지에 적는다.

- [ ] **Step 7: Commit**

```bash
git add apps/mock-bank load/verification modules/bank-gateway/src/test
git status   # docs/perf/raw/p6-2/*-smoke.json · postman/ 이 섞이지 않았는지
git commit -m "feat(p6-2): 측정 도구 load/verification — k6 열린 모델 · 측정용 포트 · 정확성(시간 초과 포함), 모의 은행 입금 기록 O(1)

연기 측정: async · coroutine · reactor · blocking 지연 100ms · 50건/s 각 1회 통과 (결과는 커밋하지 않음).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: 결정 기록 · 문서 — ADR-0013(제안) · OPERATION §12 · ARCHITECTURE · README · SETUP · HANDOVER · CHANGELOG · 용어 정리 · http · Postman

스펙 §9. 측정 결과(Task 10)가 들어갈 자리는 Task 10 이 채운다 — 여기서는 "측정 전" 으로 적는다.

**Files:**
- Create: `docs/adr/0013-web-verification-webflux-comparison.md`
- Modify: `OPERATION.md` (새 §12), `ARCHITECTURE.md`, `README.md`, `SETUP.md`, `HANDOVER.md`, `CHANGELOG.md`
- Modify (용어): `docs/adr/0012-consumer-coroutines.md`, `docs/perf/p6-1-consumer-coroutines.md`, `apps/api/…/messaging/SuspendingTransferRequestedListener.kt`, `apps/api/src/test/…/messaging/SuspendingTransferRequestedListenerTest.kt` (주석 · 테스트 이름의 "퍼밋"), `ARCHITECTURE.md` · `HANDOVER.md` · `CHANGELOG.md` 의 같은 단어
- Modify: `http/account-verification.http`
- Modify: `postman/collections/bank-transfer/.resources/definition.yaml` (변수), Create: `postman/collections/bank-transfer/1-account-verification/request-verification-reactive.request.yaml`

**Interfaces:**
- Consumes: Task 1 · 4 · 7 커밋 메시지의 관찰값 (실험 E1 ~ E4, 블로킹 판 ms, 변이 검사 4종, 테스트 수).
- Produces: ADR-0013 (Task 10 이 "결과" 절을 채우고, 머지 뒤 정리 PR 이 상태를 "채택" 으로).

- [ ] **Step 1: ADR-0013**

`docs/adr/0013-web-verification-webflux-comparison.md` — ADR-0012 의 짜임(상태 · 맥락 · 결정 · 결과 · 검토한 대안)을 따른다:

```markdown
# ADR-0013: 웹 1원인증 경로의 WebFlux 비교 — 네 판 · 별도 앱 · 끊기면 취소

- 상태: 제안 (2026-11-05)
- 관련: [설계](../superpowers/specs/2026-11-05-p6-2-web-reactive-design.md) · [계획](../superpowers/plans/2026-11-05-p6-2-web-reactive.md) · [ADR-0012](0012-consumer-coroutines.md) (컨슈머 코루틴) · [측정 결과](../perf/p6-2-web-reactive.md)

## 맥락
- 웹 요청 중 은행을 기다리는 곳은 1원인증 요청 하나다 (성명조회 → 1원 이체, 은행 두 번). 출금 접수는 DB 트랜잭션 하나로 202 를 돌려주고 은행을 기다리지 않는다 — WebFlux 로 옮겨도 JDBC 를 기다리는 것은 같다.
- 지금의 1원인증은 이미 CompletableFuture 를 돌려주는 서블릿 비동기다. 기다리는 동안 Tomcat 스레드를 놓아 준다.
- P6-1 은 "빠른 것은 코루틴이 아니라 동시에 기다릴 수 있는 수" 를 컨슈머에서 확인했다. 웹에서도 같은지, 그리고 WebFlux 의 실제 차이(스레드 · 지연 분포 · 끊김)를 본다.

## 결정
1. **네 판을 같은 도구로 비교한다**: MVC 블로킹(대조군, join) · MVC 비동기(지금 기본) · WebFlux 코루틴 · WebFlux Reactor. `account.verification.web-mode` 로 기동 때 하나만 (소문자 그대로, 그 밖은 기동 실패).
2. **WebFlux 는 별도 앱 `apps/api-reactive`** — 1원인증 요청 하나만. Spring 은 한 앱에 MVC 와 WebFlux 가 같이 있으면 MVC 를 고른다. Kafka · 스케줄러 · 배치 · Flyway 는 넣지 않는다.
3. **판정 · 바깥 어댑터는 한 벌**: 판정 순수 함수 `VerificationRequestRules`(account), JPA 저장소 · 은행 응답 해석(새 모듈 `account-infra`). 판마다 다른 것은 기다리는 방법과 블로킹 코드를 도는 스레드뿐.
4. **JDBC 는 이벤트 루프에서 돌지 않는다**: 코루틴 `withContext(Dispatchers.IO)`, Reactor `publishOn(boundedElastic())`. 실험 E1: <Task 1 관찰값 — await · fromFuture 뒤는 게이트웨이 이벤트 루프>.
5. **클라이언트가 끊으면 취소되게 둔다**: 실험 E2 · E3 — <관찰값: 코루틴 취소 · Reactor 구독 취소, 게이트웨이 future 취소>. 1원 이체 응답을 기다리는 중 끊기면 1원이 나갔을 수 있어 마스킹한 경고를 남기고, 사용자는 다시 요청한다(대가는 최악에 1원 두 번 — 돈을 잃지 않는다). `NonCancellable` 은 쓰지 않는다: 응답을 못 받은 사용자는 인증 건 id 를 몰라 저장해도 확인할 수 없다. MVC 비동기는 끊겨도 끝까지 간다 (실험 E2 MVC).
6. **오류 응답은 계약 테스트로 맞춘다**: 웹 계층 코드는 공유하지 않고(API 가 다르다), 기대표 하나(`VerificationErrorContract`, account-infra test fixtures)를 두 앱 테스트가 읽는다.
7. **운영 기본은 MVC 비동기 그대로** — WebFlux 앱은 비교용이다. (측정 뒤 결과 절에서 다시 확인)

## 결과
- (Task 10 에서 채운다: 측정 요약 · 판단 기준 "비동기 세 판 차이 5% 안이면 같다" 의 결과 · 운영 기본 판단)
- 검증: 변이 검사 4종 <Task 7 관찰값>. 테스트 <N>개.

## 검토한 대안
- **출금 접수도 WebFlux 로**: 은행을 기다리지 않아 차이가 작을 것으로 추정 — 범위에서 뺐다.
- **R2DBC**: 저장 계층(차감 전략 4종 · 아웃박스)을 다시 써야 한다. 이번 비교의 질문("은행을 기다리는 동안 스레드")과 다르다 — 범위 밖.
- **한 앱에 MVC 와 WebFlux**: Spring 이 MVC 를 고른다 (불가).
- **얇은 판(지금 CompletableFuture 서비스를 await 만)**: 코드는 가장 적지만 서버 모델 하나만 비교되고 코루틴다운 코드가 없다.
- **`NonCancellable` 로 1원 이체부터 저장까지 묶기**: 위 결정 5.
- **1원인증 동시 호출 상한**: 지금 없다. 한 판에만 두면 비교가 공정하지 않다 — 범위 밖.
```

`<…>` 는 커밋 메시지의 관찰값으로 채운다 (`git log --format=%B` 로 Task 1 · 7 커밋을 읽는다). 결과 절의 괄호 줄은 Task 10 이 바꾼다.

- [ ] **Step 2: OPERATION §12 · 그 밖의 문서**

`OPERATION.md` 끝에 `## 12. 웹 1원인증 판 · WebFlux 측정 (P6-2)` — §11 의 짜임을 따른다:
- 12-1 판 고르기: `apps/api` `--account.verification.web-mode=async|blocking`, `apps/api-reactive` `--account.verification.web-mode=coroutine|reactor` (포트 8081), 지금 값은 `/actuator/info`. 블로킹은 측정 전용 — 운영에서 쓰지 않는다. 리액티브 앱은 스키마를 만들지 않는다 (apps/api 를 한 번 띄워 Flyway 가 돈 뒤에).
- 12-2 측정 한 회: 준비(infra up, `./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist`), 명령(`bash load/verification/run.sh <판> <지연> <도착률> <회차> [톰캣]`), 무엇을 끄는지(apps/api 측정 인스턴스의 Kafka 리스너 · 발행기 · 정리 작업 · 대사 · 보존), 측정 포트(18080 · 18081 · 9190), 결과 파일 · `.invalid` 규칙, 정확성 규칙(시간 초과 포함 — 계획 "스펙과 다른 점" 5 를 운영자 말로), 끝나면 `cleanup`.
- 12-3 순서: 본 측정 72회 · 원인 확인 3회 · 한계 탐색 규칙(스펙 §6 그대로), `summarize.sh`.
- 12-4 끊김: 리액티브 앱 로그의 `1원인증 요청 취소 (stage=transfer)` 경고가 뜻하는 것, 할 일(없음 — 사용자 재요청. 자주 보이면 클라이언트 시간 초과가 은행 응답보다 짧은지 본다).

`ARCHITECTURE.md`: 모듈 그림 · 표에 `modules/account-infra` · `apps/api-reactive` 를 더하고(두 앱이 account-infra 를 쓴다), 1원인증 요청 흐름에 "판 넷 — ADR-0013" 한 줄. 기존 표현을 따른다.

`README.md`: 로드맵 P6 아래 `- [ ] P6-2 웹 1원인증 WebFlux 비교: 네 판 · 별도 앱 apps/api-reactive · 끊기면 취소 → ADR-0013 · 측정 결과` (체크는 머지 뒤 정리 PR), 구조 절의 모듈 목록, 테스트 수(Task 7), 실행 구성 8 · 9 · 10.

`SETUP.md`: 포트 표에 8081(리액티브 앱) · 18080 · 18081(측정 전용), 테스트 수.

`HANDOVER.md`: 마지막 갱신 날짜, P6-2 행 `🔄 구현 · 측정 중 (브랜치 feat/p6-2-web-reactive)` + 한 줄 요약, "P6-2 설계 (합의된 것)" 표(스펙 §2 결정 요약을 옮긴다), 다음 할 일.

`CHANGELOG.md`: `## [P6-2] 웹 1원인증 WebFlux 비교 — (머지 전)` 아래 Added · Changed(account-infra 이동 · 점검 가드 이동 · 1원인증 컨트롤러 셋 · 모의 은행 입금 기록) · Fixed 없음. P6-1 항목의 짜임을 따른다.

- [ ] **Step 3: 용어 정리 — "퍼밋" → "은행 호출 자리(세마포어 permit)"**

```bash
grep -rn "퍼밋" --include=*.md --include=*.kt . | grep -v "/build/" | grep -v "docs/superpowers/"
```
나오는 곳마다 문장을 읽고 바꾼다. 규칙:
- 처음 나오는 곳(파일마다): "은행 호출 자리(세마포어 permit)" — 그 뒤는 "자리".
- "퍼밋을 받는다 / 기다린다 / 반납" → "자리를 잡는다 / 기다린다 / 비운다".
- 테스트 이름(백틱 이름)에 "퍼밋" 이 있으면 같은 규칙으로 바꾼다 (동작은 그대로).
- `docs/superpowers/` 아래의 과거 스펙 · 계획은 기록이라 바꾸지 않는다.
- ADR-0012 는 채택된 문서지만 용어만 바꾸는 것이라 내용 결정은 그대로 둔다 — ADR 끝에 "2026-11-05: 용어 '퍼밋' 을 '은행 호출 자리(세마포어 permit)' 로 바꿈 (P6-2)" 한 줄을 남긴다.

Run: `./gradlew :apps:api:test --tests '*SuspendingTransferRequestedListenerTest*'` → Expected: PASS (이름만 바뀜).

- [ ] **Step 4: http · Postman**

`http/account-verification.http` 끝에:

```http
### P6-2 — 리액티브 앱(8081)에 같은 요청 (실행 구성 8 · 9). 확인(/confirm)은 apps/api(8080)로 — 같은 DB 다
POST http://localhost:8081/api/v1/account-verifications
Content-Type: application/json
X-User-Id: user-1

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "holderName": "홍길동"
}

> {% client.global.set("verificationId", response.body.id); %}
```
(파일의 다른 요청이 응답 저장에 쓰는 문법을 그대로 따른다 — 다르면 그것에 맞춘다.)

`postman/collections/bank-transfer/.resources/definition.yaml` 의 `variables:` 에 `reactiveBaseUrl: http://localhost:8081` 한 줄 (기존 줄 그대로).

`postman/collections/bank-transfer/1-account-verification/request-verification-reactive.request.yaml` — `request-verification.request.yaml` 을 복사해 바꾼다:
- `name: 1-1R 인증 요청 — 리액티브 앱 (8081, P6-2)`
- `url: '{{reactiveBaseUrl}}/api/v1/account-verifications'`
- `order: 1500`
- 스크립트 그대로 (`verificationId` 저장 → 1-2 확인은 8080 으로 간다).

**커밋 전 `git status` — Postman 앱이 다시 쓴 다른 파일이 보이면 커밋하지 않는다** (이 Step 이 만든 두 파일만 `git add`).

- [ ] **Step 5: Commit**

```bash
git add docs/adr/0013-web-verification-webflux-comparison.md OPERATION.md ARCHITECTURE.md README.md SETUP.md HANDOVER.md CHANGELOG.md \
  docs/adr/0012-consumer-coroutines.md docs/perf/p6-1-consumer-coroutines.md \
  apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/SuspendingTransferRequestedListener.kt \
  apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/messaging/SuspendingTransferRequestedListenerTest.kt \
  http/account-verification.http \
  postman/collections/bank-transfer/.resources/definition.yaml \
  postman/collections/bank-transfer/1-account-verification/request-verification-reactive.request.yaml
git status
git commit -m "docs(p6-2): ADR-0013(제안) · OPERATION §12 · 구조 · 로드맵 · 포트 · HANDOVER · CHANGELOG, 용어 '퍼밋' → '은행 호출 자리', http · Postman 리액티브 요청

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: 측정 · 결과 문서 (컨트롤러가 직접 실행 — 서브에이전트에 맡기지 않는다)

스펙 §6. 2시간 가까이 백그라운드 프로세스를 다루고 한계 탐색에 중간 판단이 있다.

**Files:**
- Create: `docs/perf/p6-2-web-reactive.md`, `docs/perf/raw/p6-2/*.json`
- Modify: `docs/adr/0013-web-verification-webflux-comparison.md` (결과 절), `HANDOVER.md` · `README.md` · `CHANGELOG.md` (한 줄씩)

- [ ] **Step 1: 준비**

- 사용자에게 알린다: 측정 동안 PC 를 무겁게 쓰지 않는다 (시스템 CPU 가 측정에 섞인다), 측정 포트 18080 · 18081 · 9190 을 쓴다. 인프라가 떠 있는지 확인한다 (꺼져 있으면 사용자에게 부탁).
- `./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:mock-bank:installDist` (측정 코드 = 지금 HEAD — 결과 파일의 `code` 에 남는다).

- [ ] **Step 2: 본 측정 72회 + 원인 확인 3회**

판 순서를 회차마다 돌려 시간에 따른 PC 상태 변화가 한 판에 몰리지 않게 한다:

```bash
for round in 1 2 3; do
  for d in 100 500; do
    for r in 100 200 400; do
      for m in blocking async coroutine reactor; do
        bash load/verification/run.sh "$m" "$d" "$r" "$round" || echo "실패: $m $d $r $round" >> build/load-p62/failed.txt
      done
    done
  done
  bash load/verification/run.sh blocking 500 400 "$round" 400 || echo "실패: blocking 500 400 $round t400" >> build/load-p62/failed.txt
done
```
(`run_in_background` 로 돌리고 끝나면 알림을 받는다 — 회차 하나 ≈ 40초 + 기동 ≈ 20초 → 75회 ≈ 75 ~ 90분, 추정. 폴링하지 않는다.)

- 끝나면 `build/load-p62/failed.txt` 를 본다. `.invalid` 회차는 원인(`app.log` · `mock-bank.log` · `k6.json`)을 읽고 — 도구 문제면 고쳐 그 칸만 다시, 앱 문제면 멈추고 사용자에게 보고한다.
- `bash load/verification/summarize.sh` 로 표를 본다. 편차(같은 칸 세 회차의 처리량이 중앙값에서 20% 넘게 벗어남)가 있으면 그 칸을 한 번 더 재고 결과 문서에 적는다.

- [ ] **Step 3: 한계 탐색 (비동기 세 판, 지연 500ms)**

판마다 R = 800 → 1600 → 3200 순서로 한 번씩 (`<판> 500 <R> probe1`). 그 판의 회차가 아래 중 하나에 닿으면 그 판의 탐색을 멈춘다:
- 성공률(201 ÷ 측정 구간 요청) 99% 미만 — 결과 파일 `status_201` 과 `status_4xx + status_5xx + timeouts` 로 계산
- 201 의 p99 3초 초과
- 시스템 CPU 평균 80% 이상
- `dropped_iterations > 0` 이고 `vus_max` 가 2000(최대)이 아닌 경우 — k6 자체 한계일 수 있어 멈추고 결과 문서에 "부하 발생기 한계" 로 적는다

- [ ] **Step 4: 결과 문서**

`docs/perf/p6-2-web-reactive.md` — P6-1 결과 문서(`docs/perf/p6-1-consumer-coroutines.md`)의 짜임을 따른다:

1. **한 줄 결론** — 숫자 하나와 함께 (예: "지연 500ms · 400건/s 에서 MVC 블로킹 X건/s, 비동기 세 판 Y ~ Z건/s — 차이는 기다리는 동안 스레드를 놓는가에서 난다").
2. **무엇을 왜 재는가** — 웹에서 은행을 기다리는 곳이 1원인증뿐인 이유, 열린 모델을 고른 이유.
3. **그림으로 보기** (Mermaid) — ① 네 판의 스레드 점유(은행 대기 중 누가 스레드를 잡는가), ② 코루틴 판 한 요청의 스레드 이동(HTTP 루프 → 대기 → 게이트웨이 루프 → IO → 응답), ③ 끊김 시점별 결과(성명조회 · 1원 이체 · 저장), ④ 열린 모델에서 블로킹이 무너지는 모양(도착 > 처리 → 큐 → 시간 초과).
4. **조건** — PC · 버전 · 측정 코드(`code`) · 회차 시간 · k6 설정 · 표본 간격 · "이 PC 기준 상대 비교이며 운영 수치가 아니다" (표 위에도).
5. **결과** — `summarize.sh` 표 그대로 + 칸마다 읽는 법.
6. **예상과 비교** — 블로킹 천장 ≈ 스레드 ÷ 2D (D 100 → 1,000건/s, D 500 → 200건/s), 리틀의 법칙으로 비동기 판의 동시 대기 수 ≈ R × 2D. 확인 · 추정 구분.
7. **원인 확인** — 톰캣 200 vs 400 표, 천장이 스레드를 따라 움직였는지.
8. **나타날 수 있는 현상과 읽는 법** — 시간 초과 · dropped · 5xx · 스레드 수 · CPU, 끊김 비용(`rows_minus_created` · `one_won_minus_rows`).
9. **한계 탐색** — 판별 멈춘 지점과 이유.
10. **해석 · 판단** — 판단 기준(5%) 적용, 운영 기본 판단 (스펙 §6).
11. **한계** — 한 PC · 인스턴스 하나 · 고정 지연 · JDBC 블로킹 · 표본 시각 오차 (계획 Task 8 Step 4 주석).
12. **원자료** — `docs/perf/raw/p6-2/` 파일 목록 규칙, 다시 재는 법(OPERATION §12).

확인한 것과 추정한 것을 문장마다 구분한다 (CLAUDE.md). 숫자는 결과 파일에서만 가져온다.

- [ ] **Step 5: ADR · 요약 문서 · Commit**

- ADR-0013 결과 절: 측정 요약 3 ~ 5줄, 판단 기준 결과, 운영 기본 판단(스펙 §6 — 예상과 크게 다르면 그 이유와 함께 다시 판단).
- `HANDOVER.md` P6-2 행에 측정 요약 한 줄, `README.md` 의 P6-2 줄에 결과 문서 링크와 대표 숫자, `CHANGELOG.md` 에 측정 항목.
- `bash load/verification/run.sh cleanup`

```bash
git add docs/perf/p6-2-web-reactive.md docs/perf/raw/p6-2 docs/adr/0013-web-verification-webflux-comparison.md HANDOVER.md README.md CHANGELOG.md
git status
git commit -m "docs(p6-2): 측정 결과 — 네 판 × 지연 2 × 도착률 3 × 3회, 원인 확인(톰캣 200 vs 400), 한계 탐색, ADR-0013 결과 절

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## 스펙 대응표 (계획 자체 점검)

| 스펙 | 계획 |
|---|---|
| §1 범위 (1원인증 요청만 · 범위 밖 목록) | Global Constraints, Task 5 · 6 (리액티브 앱은 요청 하나) |
| §2 결정: 별도 앱 · account-infra · 판정 공유 · web-mode · 끊김 · 계약 · 열린 모델 | Task 1 · 3 · 2 · 4 · 5 · 6 · 7 · 8 |
| §3-1 모듈 · 포트 8081 · Hikari 20 · sendGate · 점검 가드 | Task 1 · 3 · 5 ("스펙과 다른 점" 2 · 3) |
| §3-2 판정 함수 세 개 | Task 2 |
| §3-3 네 판의 흐름 · 상한 없음 · 3초 같음 | Task 4 · 5 · 6 |
| §4-1 진행 조건 실험 E1 ~ E4 | Task 1 |
| §4-2 스레드 규칙 · 스레드 이름 테스트 · 변이 | Task 5 · 6 (흐름 테스트), Task 7 (변이 1 · 2) |
| §4-3 끊김 표 · 경고 로그 · 취소 다시 던짐 · MVC 끝까지 | Task 1 (E2 MVC), Task 5 (취소 단위), Task 7 (HTTP 끊김 두 판) |
| §4-4 오류 응답 표 · 계약 테스트 | Task 3 (기대표 · MVC), Task 5 · 6 (두 판) |
| §5 설정 · info | Task 4 · 5 |
| §6 측정 칸 · 기록 · 격리 · 결과 문서 · 판단 기준 | Task 8 · 10 ("스펙과 다른 점" 5 · 6 · 7) |
| §7 테스트 표 · 변이 4종 · 실행 구성 8 · 9 · 10 · http · Postman · 손 확인 | Task 2 ~ 7 · 9 (손 확인은 사용자가 머지 전에 — PR 본문) |
| §8 위험과 한계 | Task 10 Step 4 (결과 문서 한계 절), ADR-0013 |
| §9 ADR-0013 · 문서 · 용어 정리 · PR 하나 | Task 9 · 10 |
