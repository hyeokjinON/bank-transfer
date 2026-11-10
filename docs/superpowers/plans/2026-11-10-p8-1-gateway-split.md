# P8-1 은행 게이트웨이 서비스 분리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 은행 FEP 연결을 혼자 가진 게이트웨이 앱(`apps/bank-gateway`)을 떼어내고, api · api-reactive 가 설정 하나(`bank.gateway.mode=http`)로 HTTP 로 부르게 한다. "안 보냄만 다시 보낸다, 모르면 묻는다" 는 그대로 지키고, 서버 2판 × 클라이언트 2판을 같은 계약 테스트 · 같은 측정 도구로 비교한다.

**Architecture:** `modules/bank-gateway` 에 인터페이스 `BankGateway`(결과는 `CompletableFuture`)를 두고 기존 Netty `BankGatewayClient` 가 구현한다. 새 모듈 `modules/bank-gateway-http` 가 HTTP 계약(DTO · 오류 코드) · 실패 분류 · 클라이언트(WebClient / JDK)를 가진다. 새 앱 `apps/bank-gateway` 가 내부 HTTP API 3개(WebFlux / MVC 비동기)로 은행 호출을 노출한다. 처리 코드(어댑터 · 처리기 · 정리 작업 · 리액티브 흐름)는 `BankGateway` 만 본다.

**Tech Stack:** Kotlin 2.3.21 · Java 21 · Spring Boot 4.1.1 (WebFlux · WebMVC · Actuator) · Reactor Netty 1.3.7 · JDK `java.net.http.HttpClient` · Jackson 3 (`tools.jackson`) · Netty 4.2 · Testcontainers 2.0.5 (+ toxiproxy) · JUnit 5 · Gradle 9 · Git Bash 측정 스크립트 · k6

**Spec:** `docs/superpowers/specs/2026-11-10-p8-1-gateway-split-design.md` (커밋 07b2b6d)

## Global Constraints

- 기본 `bank.gateway.mode` 는 `direct` — 기존 테스트 524개 · 실행 구성 · 손 확인 절차가 그대로 돌아야 한다. 각 Task 끝에 `./gradlew build` 전체 통과.
- 실패 분류 규칙: **`BankNotSentException` 만 "안 보냄"(다시 보내도 안전)**. HTTP 클라이언트는 은행에 확실히 안 갔다고 증명할 수 있을 때만(503 + `BANK_NOT_SENT`, 연결 단계 실패, 연결 풀 대기 시간 초과) 그것을 만든다. 나머지는 전부 "모름"(`BankTimeoutException` 또는 `BankUnavailableException`).
- 503 이라도 본문 `code` 가 `BANK_NOT_SENT` 일 때만 안 보냄.
- HTTP 클라이언트 자동 재시도 금지 (WebClient `disableRetry(true)`, JDK 는 POST 를 재시도하지 않는 기본값 — 테스트로 고정).
- 게이트웨이는 api 가 끊어도 은행 송신을 취소하지 않는다 (WebFlux `Mono.fromFuture(f, true)`, MVC 는 의존 future 를 돌려준다 — 취소가 원본으로 번지지 않음).
- 시간 순서: 연결 1s · 연결 풀 대기 1s < 게이트웨이 은행 응답 3s < HTTP 응답 5s. api 의 `bank.gateway.http.response-timeout` 이 5s 미만이면 기동 경고만(실패 아님).
- `modules/bank-gateway-http` 는 Spring Boot 에 의존하지 않는다 (`spring-webflux` · `reactor-netty-http` · Jackson 만).
- 경로: `POST /internal/v1/bank/name-inquiries` · `/internal/v1/bank/transfers` · `/internal/v1/bank/inquiries`. 오류 본문 `{"code", "message", "tranNo"?, "timeoutMs"?}`, 코드 `BANK_NOT_SENT`(503) · `BANK_TIMEOUT`(504) · `BANK_UNAVAILABLE`(502) · `INVALID_REQUEST`(400) · `INTERNAL`(500).
- 포트: 게이트웨이 개발 8082, 측정 18082, Toxiproxy 제어 8474 · 프록시 28082. 기존: api 8080 · api-reactive 8081 · kafka-ui 8089 · 모의 은행 9090 / 측정 9190 · 웹 측정 18080 · 18081.
- 로그에 계좌번호는 마스킹(뒤 4자리), 요청 · 응답 본문 전체를 남기지 않는다.
- 주석 · 문서 · 커밋 메시지는 한국어. 커밋 끝 줄 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- 커밋하지 말 것: `.run/2. API (8080).run.xml` 의 기존 변경(줄바꿈 하나), `postman/` 의 앱 자동 수정분, `infra/.env`.
- 개발 DB 는 측정 행 외에는 읽기만. 측정 데이터는 측정 스크립트의 사용자 범위(`load-p61-u*`, `p62-u*`)로만.
- Reactor Netty 1.3.7 기본값 (바이트코드로 확인함): 최대 연결 `max(코어 수, 8) × 2`, 연결 풀 대기 45,000ms.

---

## 파일 구조

| 파일 | 책임 | Task |
|---|---|---|
| `modules/bank-gateway/src/main/kotlin/.../gateway/BankGateway.kt` (신규) | 은행 호출 3가지 인터페이스 | 1 |
| `modules/bank-gateway/src/main/kotlin/.../gateway/BankGatewayClient.kt` | `: BankGateway`, `BankTimeoutException.timeout` 을 `val` 로 | 1 |
| `apps/api/.../config/{AppConfiguration,BankTransferAdapter,SuspendBankTransferAdapter,BankInquiryAdapter}.kt`, `messaging/{MessagingConfiguration,ConsumerConfiguration}.kt`, `reconcile/ReconcileConfiguration.kt` | `BankGatewayClient` → `BankGateway` | 1 |
| `apps/api-reactive/.../config/ReactiveConfiguration.kt`, `flow/{Coroutine,Reactor}VerificationFlow.kt` | 같은 교체 | 1 |
| `apps/api/src/test/.../support/ApiIntegrationTest.kt`, `apps/api-reactive/src/test/.../support/ReactiveIntegrationTest.kt` | `gateway: BankGateway`, 연결 대기를 `bankConnected()` 로 | 1 |
| `modules/bank-gateway-http/build.gradle.kts` (신규) | 모듈 · testFixtures | 2 |
| `modules/bank-gateway-http/src/main/kotlin/.../gateway/http/GatewayContract.kt` | 경로 · 오류 코드 · DTO · 전문 ↔ DTO 변환 | 2 |
| `.../gateway/http/GatewayJson.kt` | Jackson 3 매퍼 하나 | 2 |
| `.../gateway/http/HttpFailures.kt` | 실패 분류 한 곳 (§4-3) | 2 |
| `.../gateway/http/HttpGatewaySettings.kt` | 연결 · 시간 설정 값 객체 | 2 |
| `.../gateway/http/HttpBankGateway.kt` | `interface HttpBankGateway : BankGateway, AutoCloseable` + 팩토리 | 2 (팩토리 jdk 분기는 5) |
| `.../gateway/http/WebClientBankGateway.kt` | WebClient 판 | 2 |
| `.../gateway/http/JdkHttpBankGateway.kt` | JDK 판 | 5 |
| `modules/bank-gateway-http/src/testFixtures/kotlin/.../gateway/http/fixture/{TcpCutProxy,GatewayProcess}.kt` | 끊는 프록시, 게이트웨이 별도 프로세스 기동 | 2 (GatewayProcess 는 4) |
| `apps/bank-gateway/**` (신규) | 앱 · 설정 · 컨트롤러 2판 · 오류 변환 · health | 3 (MVC 판은 5) |
| `apps/bank-gateway/src/test/.../GatewayContractTest.kt` | 계약 테스트 (조합 매개변수화) | 3, 5 |
| `apps/bank-gateway/src/test/.../ToxiproxyGatewayTest.kt` | 망 지연 · 끊김 | 6 |
| `apps/api/.../config/AppConfiguration.kt` · `apps/api-reactive/.../config/*` | `mode` · `http.*` 설정, 빈 선택 | 4 (client 선택은 5) |
| `apps/api/src/test/.../gateway/HttpGatewayFlowTest.kt` · `HttpGatewayCoroutineFlowTest.kt`, `apps/api-reactive/src/test/.../HttpGatewayReactiveFlowTest.kt` | http 모드 흐름 | 4 |
| `infra/docker-compose.yml` · `infra/README.md` | `chaos` 프로필 Toxiproxy | 6 |
| `load/lib/gateway.sh` (신규), `load/consumer/run.sh`, `load/verification/run.sh`, `load/consumer/summarize.sh`, `load/verification/summarize.sh` | 측정 도구 | 7 |
| `docs/perf/p8-1-gateway-split.md`, `docs/perf/raw/p8-1/` | 측정 결과 | 8 |
| `docs/adr/0015-bank-gateway-service.md`, ARCHITECTURE · README · SETUP · OPERATION · HANDOVER · CHANGELOG, `.run/*.run.xml` | 문서 · 실행 구성 | 9 |

패키지: 기존 `io.github.hyeokjinon.banktransfer.gateway` (Netty), 신규 `io.github.hyeokjinon.banktransfer.gateway.http` (HTTP 모듈), `io.github.hyeokjinon.banktransfer.gateway.http.fixture` (testFixtures), `io.github.hyeokjinon.banktransfer.gatewayapp` (게이트웨이 앱). 아래 `...` 는 `src/main/kotlin/io/github/hyeokjinon/banktransfer` 를 줄인 것이다.

---

### Task 1: `BankGateway` 인터페이스 — 호출부가 구현이 아니라 인터페이스를 보게 한다 (동작 그대로)

**Files:**
- Create: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGateway.kt`
- Modify: `modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayClient.kt` (클래스 선언 87행 근처, `inquireName` · `transfer` · `inquire` 134 ~ 142행, `BankTimeoutException` 57행)
- Modify: `apps/api/.../api/config/AppConfiguration.kt` (`BankGatewayAdapter` 89 ~ 99행, `bankPort` 133행)
- Modify: `apps/api/.../api/config/BankTransferAdapter.kt:16`, `SuspendBankTransferAdapter.kt:17`, `BankInquiryAdapter.kt:18`
- Modify: `apps/api/.../api/messaging/MessagingConfiguration.kt:75`, `ConsumerConfiguration.kt:66`, `apps/api/.../api/reconcile/ReconcileConfiguration.kt:75`
- Modify: `apps/api-reactive/.../reactive/config/ReactiveConfiguration.kt` (`coroutineVerificationFlow` · `reactorVerificationFlow`), `flow/CoroutineVerificationFlow.kt:29`, `flow/ReactorVerificationFlow.kt:27`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Modify: `apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/reactive/support/ReactiveIntegrationTest.kt`
- Test: `modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayInterfaceTest.kt`

**Interfaces:**
- Produces:
  ```kotlin
  interface BankGateway {
      fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse>
      fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse>
      fun inquire(reference: String): CompletableFuture<InquiryResponse>
  }
  class BankTimeoutException(val tranNo: Long, val timeout: Duration)   // timeout 이 val 이 된다
  // ApiIntegrationTest: protected lateinit var gateway: BankGateway ; protected open fun bankConnected(): Boolean
  // ReactiveIntegrationTest: protected lateinit var gateway: BankGateway ; protected open fun bankConnected(): Boolean
  ```

- [ ] **Step 1: 실패하는 테스트**

`modules/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGatewayInterfaceTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** 처리 코드는 BankGateway 만 본다 (P8-1). 직결 구현(Netty)이 그 계약을 그대로 지키는지. */
class BankGatewayInterfaceTest {
    private val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
    private val client = BankGatewayClient(
        BankGatewayConfig(host = "127.0.0.1", port = bank.boundPort, responseTimeout = Duration.ofMillis(300)),
    ).start()
    private val gateway: BankGateway = client

    @AfterTest
    fun tearDown() { client.close(); bank.close() }

    private fun awaitConnected() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!client.isConnected) { check(System.currentTimeMillis() < deadline) { "not connected" }; Thread.sleep(20) }
    }

    @Test
    fun `인터페이스로 성명조회 · 이체 · 결과조회를 부른다`() {
        awaitConnected()
        assertEquals("홍길동", gateway.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS).holderName)
        val transfer = gateway.transfer("090", "1002123456789", 1_000, "IF", "T261110IFTEST0000001").get(3, TimeUnit.SECONDS)
        assertEquals(ResponseCodes.OK, transfer.header.responseCode)
        assertEquals(ResponseCodes.OK, gateway.inquire("T261110IFTEST0000001").get(3, TimeUnit.SECONDS).result)
    }

    @Test
    fun `응답 시간 초과는 BankTimeoutException 이고 기다린 시간을 알려 준다`() {
        awaitConnected()
        val error = runCatching { gateway.transfer("090", "9990000000001", 1_000, "IF", "T261110IFTEST0000002").get(3, TimeUnit.SECONDS) }
            .exceptionOrNull()
        val cause = assertIs<BankTimeoutException>((error as ExecutionException).cause)
        assertEquals(Duration.ofMillis(300), cause.timeout)
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:bank-gateway:test --tests "*BankGatewayInterfaceTest*"`
Expected: 컴파일 실패 — `Unresolved reference 'BankGateway'`, `'timeout'`.

- [ ] **Step 3: 인터페이스 · 구현 선언**

`modules/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/BankGateway.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.util.concurrent.CompletableFuture

/**
 * 은행 호출 세 가지 (P8-1). 처리 코드(어댑터 · 처리기 · 정리 작업 · 리액티브 흐름)는 이것만 본다.
 * 구현: [BankGatewayClient] (프로세스 안에서 Netty 로 FEP 에 직결, mode=direct) · HTTP 클라이언트 (게이트웨이 앱을 부름, mode=http — modules/bank-gateway-http).
 *
 * 실패는 future 안에 담긴다. 구현이 달라도 뜻은 같아야 한다 — 처리 코드가 이 셋으로 "다시 보내도 되나" 를 정한다:
 * - [BankNotSentException]: 은행에 확실히 가지 않았다 (다시 보내도 안전)
 * - [BankTimeoutException]: 보냈는데 응답이 없다 (모름 → 결과조회)
 * - [BankUnavailableException] 과 그 밖: 보냈을 수 있다 (모름)
 * 은행의 거절(응답코드 0000 이 아님)은 실패가 아니라 정상 응답이다.
 */
interface BankGateway {
    fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse>

    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse>

    /** 결과조회 (v0.2). 결과를 모르는 이체를 이용기관 거래번호로 묻는다 — 이체를 다시 보내지 않는다. */
    fun inquire(reference: String): CompletableFuture<InquiryResponse>
}
```

`BankGatewayClient.kt` 수정:
```kotlin
class BankTimeoutException(val tranNo: Long, val timeout: Duration) :
    RuntimeException("bank did not respond to tranNo=$tranNo within $timeout")
```
```kotlin
class BankGatewayClient(
    private val config: BankGatewayConfig,
    private val clock: Clock = Clock.systemDefaultZone(),
    private val sendGate: SendGate = SendGate.OPEN,
) : BankGateway, AutoCloseable {
```
세 메서드에 `override` 를 붙인다 (본문 그대로):
```kotlin
    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> = ...
    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> = ...
    override fun inquire(reference: String): CompletableFuture<InquiryResponse> = ...
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew :modules:bank-gateway:test`
Expected: PASS (기존 BankGatewayClientTest 포함).

- [ ] **Step 5: 호출부를 인터페이스로**

각 파일에서 생성자 · 빈 메서드 매개변수 타입만 `BankGatewayClient` → `BankGateway` 로 바꾸고 import 를 `io.github.hyeokjinon.banktransfer.gateway.BankGateway` 로 바꾼다. 본문은 그대로다.
- `AppConfiguration.kt`: `class BankGatewayAdapter(private val client: BankGateway, ...)`, `fun bankPort(client: BankGateway, ...)`. `bankGatewayClient` 빈 메서드는 **반환 타입 `BankGatewayClient` 그대로** 둔다 (테스트가 구체 타입으로도 찾는다, `destroyMethod = "close"`).
- `BankTransferAdapter(private val client: BankGateway)`, `SuspendBankTransferAdapter(private val client: BankGateway)`, `BankInquiryAdapter(private val client: BankGateway)`.
- `MessagingConfiguration.transferProcessor(store: TransferStore, client: BankGateway, clock: Clock)`.
- `ConsumerConfiguration.suspendingTransferRequestedListener(..., client: BankGateway, ...)`.
- `ReconcileConfiguration.transferReconciler(store, client: BankGateway, ...)`.
- `ReactiveConfiguration.coroutineVerificationFlow(client: BankGateway, ...)` · `reactorVerificationFlow(client: BankGateway, ...)`, `CoroutineVerificationFlow(private val client: BankGateway, ...)`, `ReactorVerificationFlow(private val client: BankGateway, ...)`. 리액티브 `bankGatewayClient` 빈 반환 타입도 `BankGatewayClient` 그대로.
- 주석에서 "BankGatewayClient" 를 "게이트웨이" 로 바꿔야 문장이 맞는 곳만 고친다 (예: `SuspendBankTransferAdapter` KDoc 첫 줄 "Netty 게이트웨이에 연결한다" → "은행 게이트웨이(BankGateway)에 연결한다").

- [ ] **Step 6: 테스트 기반 클래스**

`ApiIntegrationTest.kt`:
```kotlin
    @Autowired
    protected lateinit var gateway: BankGateway
```
```kotlin
    /**
     * 은행 회선이 붙어 있나. 직결(기본)은 Netty 클라이언트의 연결, http 모드 하위 클래스는 게이트웨이 앱의 bankConnection health 로 덮어쓴다 (P8-1).
     */
    protected open fun bankConnected(): Boolean = (gateway as? BankGatewayClient)?.isConnected ?: true
```
`awaitGateway` 본문의 `gateway.isConnected` 두 곳을 `bankConnected()` 로 바꾼다:
```kotlin
    private fun awaitGateway(connected: Boolean, timeout: Duration) {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (bankConnected() != connected) {
            check(System.currentTimeMillis() < deadline) { "bank connected=${bankConnected()}, expected $connected within $timeout" }
            Thread.sleep(20)
        }
    }
```
import 에 `io.github.hyeokjinon.banktransfer.gateway.BankGateway` 추가 (`BankGatewayClient` import 는 `as?` 에 쓰므로 남긴다). `ReconcileFlowTest` 의 `BankInquiryAdapter(gateway)` 는 그대로 컴파일된다.

`ReactiveIntegrationTest.kt`:
```kotlin
    @Autowired protected lateinit var gateway: BankGateway

    /** 직결은 Netty 연결, http 모드 하위 클래스는 게이트웨이 앱 health 로 덮어쓴다 (P8-1). */
    protected open fun bankConnected(): Boolean = (gateway as? BankGatewayClient)?.isConnected ?: true

    @BeforeEach
    fun waitForBankConnection() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!bankConnected()) { check(System.currentTimeMillis() < deadline) { "bank not connected" }; Thread.sleep(20) }
    }
```
리액티브 테스트 중 `gateway` 를 `BankGatewayClient` 로 쓰는 곳(예: `.isConnected`, 생성자 인자)이 있으면 그 줄만 `(gateway as BankGatewayClient)` 로 바꾼다 — `grep -rn "gateway\." apps/api-reactive/src/test` 로 확인.

- [ ] **Step 7: 전체 빌드**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL, 테스트 수 = 기존 524 + 2.

- [ ] **Step 8: 커밋**

```bash
git add modules/bank-gateway apps/api/src apps/api-reactive/src
git commit -m "refactor(p8-1): 은행 호출을 BankGateway 인터페이스로 — 처리 코드는 구현(Netty 직결)이 아니라 계약을 본다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `modules/bank-gateway-http` — HTTP 계약 · 실패 분류 · WebClient 클라이언트 · 끊는 프록시

**Files:**
- Modify: `settings.gradle.kts`
- Create: `modules/bank-gateway-http/build.gradle.kts`
- Create: `modules/bank-gateway-http/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/{GatewayContract,GatewayJson,HttpFailures,HttpGatewaySettings,HttpBankGateway,WebClientBankGateway}.kt`
- Create: `modules/bank-gateway-http/src/testFixtures/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/fixture/TcpCutProxy.kt`
- Test: `modules/bank-gateway-http/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/{HttpFailuresTest,GatewayContractTest,WebClientBankGatewayTest}.kt`

**Interfaces:**
- Consumes: `BankGateway`, `BankNotSentException(message)`, `BankTimeoutException(tranNo, timeout)`, `BankUnavailableException(message, cause)` (Task 1)
- Produces:
  ```kotlin
  object GatewayPaths { const val NAME_INQUIRIES: String; const val TRANSFERS: String; const val INQUIRIES: String }
  object GatewayErrorCodes { const val BANK_NOT_SENT; BANK_TIMEOUT; BANK_UNAVAILABLE; INVALID_REQUEST; INTERNAL }
  data class GatewayError(val code: String, val message: String, val tranNo: Long? = null, val timeoutMs: Long? = null)
  data class HeaderDto(val tranNo: Long, val tranDateTime: LocalDateTime, val responseCode: String)
  data class NameInquiryBody(val bankCode: String, val accountNo: String)
  data class NameInquiryResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val holderName: String)
  data class TransferBody(val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
  data class TransferResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
  data class InquiryBody(val reference: String)
  data class InquiryResult(val header: HeaderDto, val reference: String, val result: String)
  fun NameInquiryResponse.toResult(): NameInquiryResult ; fun NameInquiryResult.toResponse(): NameInquiryResponse
  fun TransferResponse.toResult(): TransferResult ; fun TransferResult.toResponse(): TransferResponse
  fun InquiryResponse.toResult(): InquiryResult ; fun InquiryResult.toResponse(): InquiryResponse
  object GatewayJson { val mapper: JsonMapper; fun write(value: Any): String; fun <T> read(json: String, type: Class<T>): T; fun errorOrNull(json: String): GatewayError? }
  object HttpFailures {
      fun fromTransport(error: Throwable): RuntimeException            // 연결 단계 → BankNotSentException, 나머지 → BankUnavailableException
      fun fromStatus(status: Int, body: String): RuntimeException      // 503+BANK_NOT_SENT → NotSent, 504+BANK_TIMEOUT → Timeout, 그 밖 → Unavailable
      fun <T> parse(status: Int, body: String, ok: (String) -> T): T  // 200 → ok(body) (읽기 실패는 Unavailable), 그 밖 → throw fromStatus
  }
  data class HttpGatewaySettings(val baseUrl: String, val maxConnections: Int = 500, val connectTimeout: Duration = 1s, val pendingAcquireTimeout: Duration = 1s, val responseTimeout: Duration = 5s)
  interface HttpBankGateway : BankGateway, AutoCloseable
  class WebClientBankGateway(settings: HttpGatewaySettings) : HttpBankGateway
  // testFixtures
  class TcpCutProxy(upstreamPort: Int) : AutoCloseable {
      enum class Mode { PASS, DROP_RESPONSE, DELAY_RESPONSE }
      @Volatile var mode: Mode ; @Volatile var responseDelay: Duration
      val port: Int ; val baseUrl: String
      fun start(): TcpCutProxy ; fun pause() ; fun resume() ; override fun close()
  }
  ```

- [ ] **Step 1: 모듈 등록 · 빌드 파일**

`settings.gradle.kts` 의 라이브러리 모듈 목록 끝(`account-infra` 줄 다음)에:
```kotlin
include(":modules:bank-gateway-http")  // 게이트웨이 앱의 HTTP 계약 · 클라이언트(WebClient · JDK) · 실패 분류 (P8-1)
```
(게이트웨이 앱 `:apps:bank-gateway` 는 폴더를 만드는 Task 3 에서 include 한다 — 폴더 없는 프로젝트를 include 하면 Gradle 9 가 실패한다.)

`modules/bank-gateway-http/build.gradle.kts`:
```kotlin
plugins {
    `java-library`
    `java-test-fixtures`   // 끊는 프록시 · 게이트웨이 프로세스 — 게이트웨이 앱 · api · api-reactive 테스트가 같이 쓴다
}

// 게이트웨이 앱(apps/bank-gateway)과 그 호출자(apps/api · apps/api-reactive)가 같이 쓰는 HTTP 계약 · 클라이언트 (P8-1).
// Spring Boot 에 의존하지 않는다 — WebClient(spring-webflux + Reactor Netty)와 Jackson 만. 버전은 Boot BOM.
dependencies {
    api(project(":modules:bank-gateway"))   // BankGateway · 예외 3종 · 전문 타입
    implementation("org.springframework:spring-webflux")
    implementation("io.projectreactor.netty:reactor-netty-http")
    implementation("tools.jackson.core:jackson-databind")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    testImplementation("org.awaitility:awaitility-kotlin")
}
```

- [ ] **Step 2: 실패하는 테스트 — 분류표와 DTO 왕복**

`modules/bank-gateway-http/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/HttpFailuresTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.io.IOException
import java.net.ConnectException
import java.net.UnknownHostException
import java.net.http.HttpConnectTimeoutException
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** 스펙 §4-3 표를 한 줄씩. "안 보냄" 은 은행에 확실히 안 간 경우뿐이다. */
class HttpFailuresTest {

    private fun err(code: String, tranNo: Long? = null, timeoutMs: Long? = null) =
        GatewayJson.write(GatewayError(code, "m", tranNo, timeoutMs))

    @Test
    fun `503 + BANK_NOT_SENT 만 안 보냄`() {
        assertIs<BankNotSentException>(HttpFailures.fromStatus(503, err(GatewayErrorCodes.BANK_NOT_SENT)))
    }

    @Test
    fun `코드 없는 503 은 모름 - 앞단 프록시가 만든 503 일 수 있다`() {
        val e = HttpFailures.fromStatus(503, "<html>Service Unavailable</html>")
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
    }

    @Test
    fun `504 + BANK_TIMEOUT 은 BankTimeoutException, 전문번호 · 시간을 옮긴다`() {
        val e = assertIs<BankTimeoutException>(HttpFailures.fromStatus(504, err(GatewayErrorCodes.BANK_TIMEOUT, tranNo = 42, timeoutMs = 3000)))
        assertEquals(42, e.tranNo)
        assertEquals(Duration.ofSeconds(3), e.timeout)
    }

    @Test
    fun `502 · 500 · 400 은 모름`() {
        for (status in listOf(502, 500, 400)) {
            val e = HttpFailures.fromStatus(status, err(GatewayErrorCodes.BANK_UNAVAILABLE))
            assertIs<BankUnavailableException>(e)
            assert(e !is BankNotSentException) { "status $status" }
        }
    }

    @Test
    fun `연결 단계 실패는 안 보냄 - 거부 · 연결 시간 초과 · 이름 풀이 · 연결 풀 대기`() {
        assertIs<BankNotSentException>(HttpFailures.fromTransport(ConnectException("refused")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(CompletionException(RuntimeException("wrap", ConnectException("refused")))))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(HttpConnectTimeoutException("connect timed out")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(UnknownHostException("gw")))
        assertIs<BankNotSentException>(HttpFailures.fromTransport(PoolAcquireTimeoutException()))
    }

    @Test
    fun `응답 시간 초과 · 응답 전 끊김 · 그 밖은 모름`() {
        for (e in listOf(HttpTimeoutException("request timed out"), IOException("closed"), IllegalStateException("x"))) {
            val r = HttpFailures.fromTransport(e)
            assertIs<BankUnavailableException>(r)
            assert(r !is BankNotSentException) { e.toString() }
        }
    }

    @Test
    fun `200 이라도 본문을 읽지 못하면 모름`() {
        val e = runCatching { HttpFailures.parse(200, "{broken") { GatewayJson.read(it, TransferResult::class.java) } }.exceptionOrNull()
        assertIs<BankUnavailableException>(e)
    }

    /** Reactor Netty 의 셰이드된 예외와 이름만 같은 가짜 — 분류는 클래스 이름으로 본다 (셰이드 패키지에 기대지 않게). */
    private class PoolAcquireTimeoutException : RuntimeException("pool")
}
```

`modules/bank-gateway-http/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/GatewayContractTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

/** 전문 응답 → JSON → 전문 응답이 그대로 돌아온다 (게이트웨이 앱이 쓰고 클라이언트가 읽는 길). */
class GatewayContractTest {
    private val at = LocalDateTime.of(2026, 11, 10, 9, 30, 15)
    private fun header(biz: BizType, code: String = "0000") = Header(MessageClass.RESPONSE, biz, 1234, at, code)

    @Test
    fun `성명조회 응답 왕복`() {
        val original = NameInquiryResponse(header(BizType.NAME_INQUIRY), "090", "1002123456789", "홍길동")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, NameInquiryResult::class.java).toResponse())
    }

    @Test
    fun `이체 응답 왕복 - 거절 응답코드도 그대로`() {
        val original = TransferResponse(header(BizType.TRANSFER, "1001"), "090", "1002123456789", 30_000, "적요", "T261110ABCDEFGHIJKLM")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, TransferResult::class.java).toResponse())
    }

    @Test
    fun `결과조회 응답 왕복`() {
        val original = InquiryResponse(header(BizType.INQUIRY), "T261110ABCDEFGHIJKLM", "2001")
        val json = GatewayJson.write(original.toResult())
        assertEquals(original, GatewayJson.read(json, InquiryResult::class.java).toResponse())
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :modules:bank-gateway-http:test`
Expected: 컴파일 실패 — `GatewayJson`, `HttpFailures`, `GatewayError` 등 없음.

- [ ] **Step 4: 계약 · JSON · 분류 구현**

`GatewayContract.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.time.LocalDateTime

/** 게이트웨이 앱 내부 API 경로 (P8-1). 모두 POST · JSON · 내부망 전용. */
object GatewayPaths {
    const val NAME_INQUIRIES = "/internal/v1/bank/name-inquiries"
    const val TRANSFERS = "/internal/v1/bank/transfers"
    const val INQUIRIES = "/internal/v1/bank/inquiries"
}

/** 오류 본문의 code. 503 이라도 BANK_NOT_SENT 일 때만 "안 보냄" 이다 (스펙 §4-3). */
object GatewayErrorCodes {
    const val BANK_NOT_SENT = "BANK_NOT_SENT"         // 503
    const val BANK_TIMEOUT = "BANK_TIMEOUT"           // 504
    const val BANK_UNAVAILABLE = "BANK_UNAVAILABLE"   // 502
    const val INVALID_REQUEST = "INVALID_REQUEST"     // 400
    const val INTERNAL = "INTERNAL"                   // 500
}

data class GatewayError(val code: String, val message: String, val tranNo: Long? = null, val timeoutMs: Long? = null)

/** 은행 응답 헤더 중 호출자가 쓰는 것 (전문구분 · 업무구분은 경로가 정한다). */
data class HeaderDto(val tranNo: Long, val tranDateTime: LocalDateTime, val responseCode: String)

data class NameInquiryBody(val bankCode: String, val accountNo: String)
data class NameInquiryResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val holderName: String)
data class TransferBody(val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
data class TransferResult(val header: HeaderDto, val bankCode: String, val accountNo: String, val amount: Long, val memo: String, val reference: String)
data class InquiryBody(val reference: String)
data class InquiryResult(val header: HeaderDto, val reference: String, val result: String)

private fun Header.toDto() = HeaderDto(tranNo, tranDateTime, responseCode)
private fun HeaderDto.toHeader(biz: BizType) = Header(MessageClass.RESPONSE, biz, tranNo, tranDateTime, responseCode)

fun NameInquiryResponse.toResult() = NameInquiryResult(header.toDto(), bankCode, accountNo, holderName)
fun NameInquiryResult.toResponse() = NameInquiryResponse(header.toHeader(BizType.NAME_INQUIRY), bankCode, accountNo, holderName)
fun TransferResponse.toResult() = TransferResult(header.toDto(), bankCode, accountNo, amount, memo, reference)
fun TransferResult.toResponse() = TransferResponse(header.toHeader(BizType.TRANSFER), bankCode, accountNo, amount, memo, reference)
fun InquiryResponse.toResult() = InquiryResult(header.toDto(), reference, result)
fun InquiryResult.toResponse() = InquiryResponse(header.toHeader(BizType.INQUIRY), reference, result)
```

`GatewayJson.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

/**
 * 게이트웨이 계약 JSON 매퍼 하나 (P8-1). 서버(두 판)와 클라이언트(두 판)가 모두 이것으로 문자열을 쓰고 읽는다 —
 * 프레임워크 코덱 설정 차이가 판 비교에 섞이지 않게. Jackson 3 은 java.time 을 기본으로 ISO 문자열로 다룬다.
 */
object GatewayJson {
    val mapper: JsonMapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()

    fun write(value: Any): String = mapper.writeValueAsString(value)

    fun <T> read(json: String, type: Class<T>): T = mapper.readValue(json, type)

    /** 오류 본문이면 읽고, 아니면(빈 본문 · HTML · 깨진 JSON) null. */
    fun errorOrNull(json: String): GatewayError? = runCatching { read(json, GatewayError::class.java) }.getOrNull()
}
```
(Jackson 3 에서 `LocalDateTime` 이 숫자 배열로 써지면 — 위 왕복 테스트가 그래도 통과하지만 — 로그 가독성을 위해 `.disable(tools.jackson.databind.cfg.DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)` 를 빌더에 더한다. 기본값을 확인해 필요할 때만.)

`HttpFailures.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import java.net.ConnectException
import java.net.UnknownHostException
import java.net.http.HttpConnectTimeoutException
import java.time.Duration

/**
 * HTTP 구간의 실패를 은행 예외 셋으로 옮기는 곳 하나 (P8-1, 스펙 §4-3). 두 클라이언트 판(WebClient · JDK)이 같이 쓴다 —
 * 규칙이 판마다 갈라지면 한쪽에서 "모름" 을 "안 보냄" 으로 바꿔 이중 출금이 날 수 있다.
 *
 * 원칙: 은행에 확실히 가지 않았다고 증명할 수 있을 때만 [BankNotSentException].
 * - 게이트웨이가 503 + BANK_NOT_SENT 라고 말했다 (게이트웨이가 은행에 쓰기 전에 멈췄다)
 * - 게이트웨이에 연결하지 못했다 (거부 · 연결 시간 초과 · 이름 풀이 실패 · 연결 풀에서 연결을 얻지 못함) — 요청을 쓰기 전이다
 * 나머지(응답 시간 초과, 응답 전 끊김, 5xx, 읽을 수 없는 본문, 모르는 예외)는 요청이 갔을 수 있으므로 모름.
 */
object HttpFailures {

    fun fromTransport(error: Throwable): RuntimeException =
        if (causes(error).any(::isBeforeRequest)) {
            BankNotSentException("gateway not reached: ${rootMessage(error)}")
        } else {
            BankUnavailableException("gateway call failed, the request may have reached the bank: ${rootMessage(error)}", error)
        }

    fun fromStatus(status: Int, body: String): RuntimeException {
        val error = GatewayJson.errorOrNull(body)
        return when {
            status == 503 && error?.code == GatewayErrorCodes.BANK_NOT_SENT ->
                BankNotSentException("gateway: ${error.message}")
            status == 504 && error?.code == GatewayErrorCodes.BANK_TIMEOUT ->
                BankTimeoutException(error.tranNo ?: 0, Duration.ofMillis(error.timeoutMs ?: 0))
            else -> BankUnavailableException("gateway status=$status code=${error?.code}")
        }
    }

    fun <T> parse(status: Int, body: String, ok: (String) -> T): T {
        if (status != 200) throw fromStatus(status, body)
        return try {
            ok(body)
        } catch (e: Exception) {
            throw BankUnavailableException("unreadable gateway response", e)
        }
    }

    /**
     * 요청을 쓰기 전 단계의 실패인가. 셰이드된 Reactor Netty 예외(reactor.netty.internal.shaded...PoolAcquireTimeoutException)는
     * 패키지가 버전마다 바뀔 수 있어 클래스 단순 이름으로 본다. Netty ConnectTimeoutException 은 ConnectException 의 하위다.
     */
    private fun isBeforeRequest(t: Throwable): Boolean =
        t is ConnectException || t is HttpConnectTimeoutException || t is UnknownHostException ||
            t.javaClass.simpleName == "PoolAcquireTimeoutException"

    private fun causes(t: Throwable): Sequence<Throwable> = generateSequence(t) { it.cause?.takeIf { c -> c !== it } }

    private fun rootMessage(t: Throwable): String = causes(t).last().let { "${it.javaClass.simpleName}: ${it.message}" }
}
```
(`BankUnavailableException(message, cause)` 생성자는 Task 1 이전부터 있다: `open class BankUnavailableException(message: String, cause: Throwable? = null)`.)

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :modules:bank-gateway-http:test --tests "*HttpFailuresTest*" --tests "*GatewayContractTest*"`
Expected: PASS

- [ ] **Step 6: 실패하는 테스트 — WebClient 클라이언트 · 끊는 프록시**

JDK 내장 `com.sun.net.httpserver.HttpServer` 로 게이트웨이 흉내를 낸다 (게이트웨이 앱은 Task 3).

`modules/bank-gateway-http/src/test/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/WebClientBankGatewayTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.TcpCutProxy
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class WebClientBankGatewayTest {
    private val requests = AtomicInteger()
    @Volatile private var reply: Pair<Int, String> = 200 to ok()
    @Volatile private var replyDelay: Duration = Duration.ZERO

    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext(GatewayPaths.TRANSFERS) { ex ->
            requests.incrementAndGet()
            ex.requestBody.readAllBytes()
            Thread.sleep(replyDelay.toMillis())
            val (status, body) = reply
            val bytes = body.toByteArray()
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(status, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }
        executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()
        start()
    }
    private val closeables = mutableListOf<AutoCloseable>()

    @AfterTest
    fun tearDown() { closeables.asReversed().forEach { it.close() }; server.stop(0) }

    private fun gateway(baseUrl: String = "http://127.0.0.1:${server.address.port}", responseTimeout: Duration = Duration.ofSeconds(2)) =
        WebClientBankGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = responseTimeout)).also { closeables += it }

    private fun transferFailure(gw: HttpBankGateway): Throwable =
        (runCatching { gw.transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS) }.exceptionOrNull() as ExecutionException).cause!!

    @Test
    fun `200 이면 전문 응답으로 되살린다`() {
        val res = gateway().transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS)
        assertEquals("0000", res.header.responseCode)
        assertEquals(REF, res.reference)
    }

    @Test
    fun `503 BANK_NOT_SENT 는 안 보냄`() {
        reply = 503 to GatewayJson.write(GatewayError(GatewayErrorCodes.BANK_NOT_SENT, "bank channel is not connected"))
        assertIs<BankNotSentException>(transferFailure(gateway()))
    }

    @Test
    fun `504 BANK_TIMEOUT 은 BankTimeoutException`() {
        reply = 504 to GatewayJson.write(GatewayError(GatewayErrorCodes.BANK_TIMEOUT, "t", tranNo = 7, timeoutMs = 3000))
        assertEquals(7, assertIs<BankTimeoutException>(transferFailure(gateway())).tranNo)
    }

    @Test
    fun `게이트웨이가 꺼져 있으면(연결 거부) 안 보냄`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        assertIs<BankNotSentException>(transferFailure(gateway(baseUrl = "http://127.0.0.1:$closedPort")))
    }

    @Test
    fun `응답 시간 초과는 모름 - 요청은 이미 갔다`() {
        replyDelay = Duration.ofMillis(800)
        val e = transferFailure(gateway(responseTimeout = Duration.ofMillis(300)))
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
    }

    @Test
    fun `받고 응답 전에 끊기면 모름이고, 다시 보내지 않는다`() {
        val proxy = TcpCutProxy(server.address.port).start().also { closeables += it }
        proxy.mode = TcpCutProxy.Mode.DROP_RESPONSE
        val e = transferFailure(gateway(baseUrl = proxy.baseUrl))
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
        Thread.sleep(300)
        assertEquals(1, requests.get())   // 자동 재시도 없음 (disableRetry)
    }

    private companion object {
        const val REF = "T261110WCTEST0000001"
        fun ok() = GatewayJson.write(
            TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", REF),
        )
    }
}
```

- [ ] **Step 7: 실패 확인**

Run: `./gradlew :modules:bank-gateway-http:test --tests "*WebClientBankGatewayTest*"`
Expected: 컴파일 실패 — `WebClientBankGateway`, `HttpGatewaySettings`, `TcpCutProxy` 없음.

- [ ] **Step 8: 설정 · 인터페이스 · WebClient 판**

`HttpGatewaySettings.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import java.time.Duration

/**
 * 게이트웨이 HTTP 호출 설정 (P8-1, 스펙 §4-4 · §6). 시간 순서: 연결 · 연결 풀 대기 < 게이트웨이 은행 응답(3s) < [responseTimeout].
 * api 가 게이트웨이보다 먼저 끊으면 게이트웨이는 답을 받았는데 api 는 "모름" 이 되는 건이 쓸데없이 는다.
 */
data class HttpGatewaySettings(
    val baseUrl: String,
    /** WebClient 만. Reactor Netty 기본은 max(코어, 8) × 2 (이 PC 16코어 = 32) — 측정의 동시 대기(수백)보다 작아 새 천장이 된다. */
    val maxConnections: Int = 500,
    val connectTimeout: Duration = Duration.ofSeconds(1),
    /** WebClient 만. 기본 45초면 연결을 못 얻은 컨슈머가 그만큼 묶인다 — 빨리 "안 보냄" 으로 끝낸다. */
    val pendingAcquireTimeout: Duration = Duration.ofSeconds(1),
    val responseTimeout: Duration = Duration.ofSeconds(5),
) {
    init {
        require(maxConnections >= 1) { "maxConnections must be >= 1: $maxConnections" }
    }
}
```

`HttpBankGateway.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.gateway.BankGateway

/** 게이트웨이 앱을 HTTP 로 부르는 BankGateway (P8-1). 연결 풀 · 스레드를 가지므로 닫아야 한다. */
interface HttpBankGateway : BankGateway, AutoCloseable {
    companion object {
        const val WEBCLIENT = "webclient"

        fun create(kind: String, settings: HttpGatewaySettings): HttpBankGateway = when (kind) {
            WEBCLIENT -> WebClientBankGateway(settings)
            else -> throw IllegalArgumentException("unknown http client '$kind' (webclient)")
        }
    }
}
```

`WebClientBankGateway.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import io.netty.channel.ChannelOption
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import java.util.concurrent.CompletableFuture

/**
 * WebClient(Reactor Netty) 판 — 기본 (P8-1, 스펙 §5-2).
 * 응답은 Reactor Netty 이벤트 루프가 받고 future 를 완료한다. 그 뒤의 DB 작업은 호출자가 이 스레드에서 돌리면 안 된다
 * (api 의 BankGatewayAdapter 가 thenApplyAsync 로 넘기는 이유가 직결 판과 같다).
 *
 * 자동 재시도는 끈다(disableRetry): Reactor Netty 는 재사용하던 연결이 요청 중 끊기면 한 번 다시 보낼 수 있는데,
 * 요청이 정말 안 갔는지 우리가 판단할 수 없다 — 이체가 두 번 갈 여지를 없앤다. 재시도는 Kafka 재시도 토픽 · 정리 작업만 한다.
 */
class WebClientBankGateway(private val settings: HttpGatewaySettings) : HttpBankGateway {

    private val provider: ConnectionProvider = ConnectionProvider.builder("bank-gateway")
        .maxConnections(settings.maxConnections)
        .pendingAcquireTimeout(settings.pendingAcquireTimeout)
        .build()

    private val webClient: WebClient = WebClient.builder()
        .baseUrl(settings.baseUrl)
        .clientConnector(
            ReactorClientHttpConnector(
                HttpClient.create(provider)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.connectTimeout.toMillis().toInt())
                    .responseTimeout(settings.responseTimeout)
                    .disableRetry(true),
            ),
        )
        .build()

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        post(GatewayPaths.NAME_INQUIRIES, NameInquiryBody(bankCode, accountNo)) { GatewayJson.read(it, NameInquiryResult::class.java).toResponse() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        post(GatewayPaths.TRANSFERS, TransferBody(bankCode, accountNo, amount, memo, reference)) { GatewayJson.read(it, TransferResult::class.java).toResponse() }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        post(GatewayPaths.INQUIRIES, InquiryBody(reference)) { GatewayJson.read(it, InquiryResult::class.java).toResponse() }

    /** 본문은 문자열로 주고받는다 — JSON 은 GatewayJson 한 곳 (두 클라이언트 판이 같게). 상태 코드는 예외로 바꾸지 않고 직접 본다. */
    private fun <T> post(path: String, body: Any, parse: (String) -> T): CompletableFuture<T> =
        webClient.post().uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(GatewayJson.write(body))
            .exchangeToMono { res -> res.bodyToMono(String::class.java).defaultIfEmpty("").map { res.statusCode().value() to it } }
            .toFuture()
            .handle { reply, error ->
                if (error != null) throw HttpFailures.fromTransport(error)
                HttpFailures.parse(reply.first, reply.second, parse)
            }

    override fun close() {
        provider.dispose()
    }
}
```

- [ ] **Step 9: 끊는 프록시 (testFixtures)**

`modules/bank-gateway-http/src/testFixtures/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/fixture/TcpCutProxy.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http.fixture

import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * 호출자와 게이트웨이 사이에 두는 TCP 프록시 (P8-1 테스트). 바이트를 그대로 옮기되:
 * - [Mode.DROP_RESPONSE]: 게이트웨이의 응답이 오기 시작하면 옮기지 않고 양쪽 연결을 끊는다 = "요청은 갔는데 응답 전에 끊김"
 * - [Mode.DELAY_RESPONSE]: 응답 조각마다 [responseDelay] 만큼 늦게 옮긴다
 * - [pause]: 듣기 소켓과 붙은 연결을 모두 닫는다 = "게이트웨이가 꺼짐"(연결 거부). [resume] 은 같은 포트로 다시 연다.
 * 연결마다 가상 스레드 두 개(양방향 복사). keep-alive 연결도 바이트 단위라 그대로 통과한다.
 */
class TcpCutProxy(private val upstreamPort: Int, private val upstreamHost: String = "127.0.0.1") : AutoCloseable {
    enum class Mode { PASS, DROP_RESPONSE, DELAY_RESPONSE }

    @Volatile var mode: Mode = Mode.PASS
    @Volatile var responseDelay: Duration = Duration.ZERO

    @Volatile private var server: ServerSocket? = null
    private val live = ConcurrentHashMap.newKeySet<Socket>()

    var port: Int = 0
        private set

    val baseUrl: String get() = "http://127.0.0.1:$port"

    fun start(): TcpCutProxy {
        bind(0)
        return this
    }

    fun pause() {
        server?.close()
        server = null
        live.forEach { runCatching { it.close() } }
        live.clear()
    }

    fun resume(): TcpCutProxy {
        if (server == null) bind(port)
        return this
    }

    override fun close() = pause()

    private fun bind(requested: Int) {
        val socket = ServerSocket().apply { reuseAddress = true; bind(InetSocketAddress("127.0.0.1", requested)) }
        server = socket
        port = socket.localPort
        Thread.ofVirtual().name("cut-proxy-accept").start {
            while (!socket.isClosed) {
                val client = runCatching { socket.accept() }.getOrNull() ?: break
                val upstream = runCatching { Socket(upstreamHost, upstreamPort) }.getOrElse { client.close(); continue }
                live += client; live += upstream
                Thread.ofVirtual().start { pipe(client.getInputStream(), upstream.getOutputStream(), client, upstream, response = false) }
                Thread.ofVirtual().start { pipe(upstream.getInputStream(), client.getOutputStream(), client, upstream, response = true) }
            }
        }
    }

    private fun pipe(from: InputStream, to: OutputStream, client: Socket, upstream: Socket, response: Boolean) {
        val buf = ByteArray(8192)
        try {
            while (true) {
                val n = from.read(buf)
                if (n < 0) break
                if (response && mode == Mode.DROP_RESPONSE) break
                if (response && mode == Mode.DELAY_RESPONSE) Thread.sleep(responseDelay.toMillis())
                to.write(buf, 0, n)
                to.flush()
            }
        } catch (_: Exception) {
            // 한쪽이 닫히면 끝
        } finally {
            runCatching { client.close() }; runCatching { upstream.close() }
            live -= client; live -= upstream
        }
    }
}
```

- [ ] **Step 10: 통과 확인**

Run: `./gradlew :modules:bank-gateway-http:test`
Expected: PASS (HttpFailuresTest 7 · GatewayContractTest 3 · WebClientBankGatewayTest 6).
연결 거부 테스트가 `BankNotSentException` 이 아니면: 실패 메시지의 예외 사슬을 보고 `isBeforeRequest` 가 그 타입을 알아보는지 확인한다 (Reactor Netty 는 `AbstractChannel.AnnotatedConnectException` — `ConnectException` 하위 — 를 `WebClientRequestException` 으로 감싼다).

- [ ] **Step 11: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add settings.gradle.kts modules/bank-gateway-http
git commit -m "feat(p8-1): 게이트웨이 HTTP 계약 · 실패 분류 · WebClient 클라이언트 (재시도 끔, 연결 단계 실패만 안 보냄)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `apps/bank-gateway` — WebFlux 판 · 오류 변환 · 송신 보류 · health · 계약 테스트

**Files:**
- Create: `apps/bank-gateway/build.gradle.kts`
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/BankGatewayApplication.kt`
- Create: `.../gatewayapp/config/GatewayConfiguration.kt` (설정 속성 2개 · 빈)
- Create: `.../gatewayapp/web/GatewayEndpoints.kt` (요청 읽기 · 은행 호출 · 응답 만들기 — 두 판 공통)
- Create: `.../gatewayapp/web/ReactiveGatewayController.kt`
- Create: `.../gatewayapp/health/BankConnectionHealthIndicator.kt`
- Create: `apps/bank-gateway/src/main/resources/application.yml`
- Test: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/{GatewayApps,GatewayContractTest,BankConnectionHealthTest}.kt`

**Interfaces:**
- Consumes: Task 1 `BankGateway` · `BankGatewayClient(config, clock, sendGate)` · `isConnected` · `pendingCount`; Task 2 `GatewayPaths` · `GatewayErrorCodes` · `GatewayError` · DTO · `toResult()` · `GatewayJson` · `HttpGatewaySettings` · `HttpBankGateway.create(kind, settings)` · `TcpCutProxy`
- Produces:
  ```kotlin
  // 앱 main 클래스: io.github.hyeokjinon.banktransfer.gatewayapp.BankGatewayApplicationKt
  object GatewayEndpoints {
      fun nameInquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>>
      fun transfer(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>>
      fun inquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>>
  }
  // 테스트 지원: GatewayApps.start(serverType: WebApplicationType, bankPort: Int, vararg args: String): ConfigurableApplicationContext ; ctx.port()
  // 계약 테스트 조합: data class Combo(val server: WebApplicationType, val client: String) — Task 5 에서 목록만 늘린다
  ```

- [ ] **Step 1: 빌드 파일 · 앱 골격**

`settings.gradle.kts` 실행 모듈 목록 끝에:
```kotlin
include(":apps:bank-gateway")          // 은행 FEP 연결을 혼자 가진 게이트웨이 앱, 내부 HTTP API (P8-1)
```

`apps/bank-gateway/build.gradle.kts`:
```kotlin
plugins {
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

// 은행 FEP 연결을 혼자 가진 게이트웨이 앱 (P8-1, ADR-0015). DB · Kafka 없음.
// 서버 판은 spring.main.web-application-type 으로 고른다: reactive(기본, WebFlux) | servlet(MVC 비동기, Task 5 에서 스타터 추가)
dependencies {
    implementation(project(":modules:bank-gateway"))
    implementation(project(":modules:bank-gateway-http"))   // 계약 DTO · 오류 코드 · JSON

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")

    testImplementation(project(":apps:mock-bank"))
    testImplementation(testFixtures(project(":modules:bank-gateway-http")))   // TcpCutProxy
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility-kotlin")
}

tasks.withType<Test> {
    jvmArgs("-Duser.timezone=UTC")   // 점검 시간 판단이 JVM 기본 시간대에 기대지 않는지 (apps/api 와 같다)
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
    workingDir = rootProject.projectDir
}
```

`BankGatewayApplication.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class BankGatewayApplication

fun main(args: Array<String>) {
    runApplication<BankGatewayApplication>(*args)
}
```

`apps/bank-gateway/src/main/resources/application.yml`:
```yaml
# 은행 게이트웨이 앱 (P8-1). 은행 FEP 연결 하나를 가지고 내부 HTTP API 3개로 노출한다. 내부망 전용 — 인증 없음 (P8-3 에서 NetworkPolicy).
server:
  port: 8082
  shutdown: graceful
spring:
  application:
    name: bank-gateway
  main:
    web-application-type: reactive   # reactive = WebFlux 판(기본) | servlet = MVC 비동기 판 (P8-1 비교)

bank:
  gateway:                     # api 의 direct 모드와 같은 값
    host: localhost
    port: 9090
    response-timeout: 3s       # 호출자의 HTTP 응답 시간 초과(5s)보다 짧아야 한다 (스펙 §4-4)
    poll-interval: 30s
  maintenance:                 # 송신 보류 (P5-1). api 의 접수 503 과 같은 창이어야 한다 (두 곳 설정 — 스펙 §10)
    enabled: true
    start: "23:50"
    end: "00:10"

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics   # metrics: 측정 스크립트가 CPU · 스레드 수를 읽는다
  endpoint:
    health:
      show-details: always             # bankConnection 상태를 본다 (내부망 전용)
  info:
    env:
      enabled: true

info:
  gateway:
    server: ${spring.main.web-application-type}
```

- [ ] **Step 2: 실패하는 계약 테스트**

`apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayApps.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext

/** 계약 테스트용: 게이트웨이 앱을 판(서버 타입)별로 같은 JVM 에 임의 포트로 띄운다. */
object GatewayApps {
    fun start(server: WebApplicationType, bankPort: Int, vararg args: String): ConfigurableApplicationContext =
        SpringApplicationBuilder(BankGatewayApplication::class.java)
            .web(server)
            .run(
                "--server.port=0",
                "--spring.main.web-application-type=${server.name.lowercase()}",
                "--bank.gateway.host=127.0.0.1",
                "--bank.gateway.port=$bankPort",
                "--bank.gateway.response-timeout=500ms",
                "--bank.maintenance.enabled=false",
                *args,
            )
}

fun ConfigurableApplicationContext.port(): Int = environment.getProperty("local.server.port")!!.toInt()
```

`apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/GatewayContractTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.TcpCutProxy
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.boot.WebApplicationType
import org.springframework.context.ConfigurableApplicationContext
import java.net.ServerSocket
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * 스펙 §7 계약 테스트 — 서버 판 × 클라이언트 판 조합마다 같은 표를 확인한다.
 * 모의 은행: 9990000000001 = 처리도 응답도 안 함(무응답), 그 밖 등록 계좌는 정상.
 */
class GatewayContractTest {

    data class Combo(val server: WebApplicationType, val client: String) {
        override fun toString() = "${server.name.lowercase()}+$client"
    }

    companion object {
        @JvmStatic
        fun combos() = listOf(
            Combo(WebApplicationType.REACTIVE, HttpBankGateway.WEBCLIENT),
        )

        val bank: MockBankServer = MockBankServer(silentAccounts = setOf(SILENT)).start()
        private val apps = mutableMapOf<WebApplicationType, ConfigurableApplicationContext>()

        fun app(server: WebApplicationType): ConfigurableApplicationContext = synchronized(apps) {
            apps.getOrPut(server) { GatewayApps.start(server, bank.boundPort) }
        }

        @JvmStatic
        @AfterAll
        fun stopAll() { apps.values.forEach { it.close() }; bank.close() }

        const val OK_ACCOUNT = "1002123456789"
        const val SILENT = "9990000000001"
    }

    private val closeables = mutableListOf<AutoCloseable>()

    @AfterEach
    fun tearDown() { closeables.asReversed().forEach { it.close() }; bank.responseDelay = Duration.ZERO }

    private fun client(combo: Combo, baseUrl: String = "http://127.0.0.1:${app(combo.server).port()}", settings: (HttpGatewaySettings) -> HttpGatewaySettings = { it }): BankGateway =
        HttpBankGateway.create(combo.client, settings(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = Duration.ofSeconds(2))))
            .also { closeables += it }

    private fun appBank(combo: Combo): BankGatewayClient = app(combo.server).getBean(BankGatewayClient::class.java)

    private fun awaitConnected(combo: Combo) = await.atMost(Duration.ofSeconds(10)) untilAsserted { assert(appBank(combo).isConnected) }

    private fun <T> CompletableFuture<T>.failure(): Throwable =
        (runCatching { get(10, TimeUnit.SECONDS) }.exceptionOrNull() as ExecutionException).cause!!

    private fun ref() = "T261110" + UUID.randomUUID().toString().replace("-", "").take(13).uppercase()

    @ParameterizedTest @MethodSource("combos")
    fun `1 - 0000 응답은 그대로`(combo: Combo) {
        awaitConnected(combo)
        val r = ref()
        val res = client(combo).transfer("090", OK_ACCOUNT, 1_000, "C1", r).get(5, TimeUnit.SECONDS)
        assertEquals("0000", res.header.responseCode)
        assertEquals(r, res.reference)
        assertEquals("홍길동", client(combo).inquireName("090", OK_ACCOUNT).get(5, TimeUnit.SECONDS).holderName)
        assertEquals("0000", client(combo).inquire(r).get(5, TimeUnit.SECONDS).result)
    }

    @ParameterizedTest @MethodSource("combos")
    fun `2 - 거절 1001 도 200 으로 응답코드 그대로`(combo: Combo) {
        awaitConnected(combo)
        val res = client(combo).transfer("090", "5500000000001", 1_000, "C2", ref()).get(5, TimeUnit.SECONDS)
        assertEquals("1001", res.header.responseCode)
    }

    @ParameterizedTest @MethodSource("combos")
    fun `3 - 은행 연결이 없으면 안 보냄`(combo: Combo) {
        val isolated = MockBankServer().start().also { closeables += it }
        val ctx = GatewayApps.start(combo.server, isolated.boundPort).also { closeables += it }
        val gw = ctx.getBean(BankGatewayClient::class.java)
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assert(gw.isConnected) }
        isolated.pause()
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assert(!gw.isConnected) }
        val e = client(combo, baseUrl = "http://127.0.0.1:${ctx.port()}").transfer("090", OK_ACCOUNT, 1_000, "C3", ref()).failure()
        assertIs<BankNotSentException>(e)
    }

    @ParameterizedTest @MethodSource("combos")
    fun `4 - 점검 시간 송신 보류는 안 보냄`(combo: Combo) {
        val now = LocalTime.now(ZoneId.of("Asia/Seoul"))
        val fmt = DateTimeFormatter.ofPattern("HH:mm")
        val ctx = GatewayApps.start(
            combo.server, bank.boundPort,
            "--bank.maintenance.enabled=true",
            "--bank.maintenance.start=${now.minusMinutes(1).format(fmt)}",
            "--bank.maintenance.end=${now.plusMinutes(2).format(fmt)}",
        ).also { closeables += it }
        val e = client(combo, baseUrl = "http://127.0.0.1:${ctx.port()}").transfer("090", OK_ACCOUNT, 1_000, "C4", ref()).failure()
        assertIs<BankNotSentException>(e)
    }

    @ParameterizedTest @MethodSource("combos")
    fun `5 - 은행 무응답은 504 BankTimeoutException`(combo: Combo) {
        awaitConnected(combo)
        assertIs<BankTimeoutException>(client(combo).transfer("090", SILENT, 1_000, "C5", ref()).failure())
    }

    @ParameterizedTest @MethodSource("combos")
    fun `6 - 게이트웨이가 꺼져 있으면 안 보냄`(combo: Combo) {
        val closedPort = ServerSocket(0).use { it.localPort }
        assertIs<BankNotSentException>(client(combo, baseUrl = "http://127.0.0.1:$closedPort").transfer("090", OK_ACCOUNT, 1_000, "C6", ref()).failure())
    }

    @ParameterizedTest @MethodSource("combos")
    fun `7 · 11 - 받고 응답 전에 끊기면 모름, 은행에는 한 번만 갔다`(combo: Combo) {
        awaitConnected(combo)
        val proxy = TcpCutProxy(app(combo.server).port()).start().also { closeables += it }
        proxy.mode = TcpCutProxy.Mode.DROP_RESPONSE
        val r = ref()
        val e = client(combo, baseUrl = proxy.baseUrl).transfer("090", OK_ACCOUNT, 1_000, "C7", r).failure()
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
        Thread.sleep(500)
        assertEquals(1, bank.depositsOf(OK_ACCOUNT).count { it.reference == r })
    }

    @ParameterizedTest @MethodSource("combos")
    fun `8 - 연결 풀 고갈은 안 보냄 (WebClient)`(combo: Combo) {
        assumeTrue(combo.client == HttpBankGateway.WEBCLIENT, "JDK HttpClient 는 연결 풀 상한이 없다")
        awaitConnected(combo)
        val gw = client(combo) { it.copy(maxConnections = 1, pendingAcquireTimeout = Duration.ofMillis(200)) }
        val holding = gw.transfer("090", SILENT, 1_000, "C8a", ref())   // 은행 무응답 → 게이트웨이 500ms 동안 연결을 붙잡는다
        Thread.sleep(50)
        assertIs<BankNotSentException>(gw.transfer("090", OK_ACCOUNT, 1_000, "C8b", ref()).failure())
        holding.failure()
    }

    @ParameterizedTest @MethodSource("combos")
    fun `9 - 응답 읽기 시간 초과는 모름`(combo: Combo) {
        awaitConnected(combo)
        bank.responseDelay = Duration.ofMillis(400)   // 게이트웨이 은행 시간 초과(500ms) 안, 클라이언트(200ms) 밖
        val e = client(combo) { it.copy(responseTimeout = Duration.ofMillis(200)) }.transfer("090", OK_ACCOUNT, 1_000, "C9", ref()).failure()
        assertIs<BankUnavailableException>(e)
        assert(e !is BankNotSentException)
    }

    @ParameterizedTest @MethodSource("combos")
    fun `10 - 호출자가 끊어도 은행 송신은 취소되지 않는다`(combo: Combo) {
        awaitConnected(combo)
        bank.responseDelay = Duration.ofMillis(300)
        val r = ref()
        val future = client(combo).transfer("090", OK_ACCOUNT, 1_000, "C10", r)
        Thread.sleep(100)
        future.cancel(true)
        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            assertEquals(1, bank.depositsOf(OK_ACCOUNT).count { it.reference == r })   // 은행은 처리했다
            assertEquals(0, appBank(combo).pendingCount)                                // 응답을 받고 대기 표를 비웠다
        }
    }
}
```

`apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/BankConnectionHealthTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.web.client.RestClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertContains

/** bankConnection health 가 은행 회선을 따라간다 (P8-3 readiness 로 쓴다). */
class BankConnectionHealthTest {
    @Test
    fun `은행이 내려가면 DOWN, 올라오면 UP`() {
        MockBankServer().start().use { bank ->
            GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort).use { ctx ->
                val rest = RestClient.create("http://127.0.0.1:${ctx.port()}")
                fun status(): String = rest.get().uri("/actuator/health/bankConnection").exchange { _, res -> res.bodyTo(String::class.java) ?: "" }
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertContains(status(), "\"UP\"") }
                bank.pause()
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertContains(status(), "\"DOWN\"") }
                bank.resume()
                await.atMost(Duration.ofSeconds(40)) untilAsserted { assertContains(status(), "\"UP\"") }
            }
        }
    }
}
```
(`RestClient` 는 `spring-web` 에 있다 — `spring-boot-starter-test` · webflux 스타터가 `spring-web` 을 가져온다.)

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :apps:bank-gateway:test`
Expected: 계약 테스트 실패 — 컨트롤러가 없어 404 → `BankUnavailableException` 등 (또는 `BankGatewayClient` 빈 없음으로 기동 실패).

- [ ] **Step 4: 설정 · 공통 엔드포인트 · WebFlux 컨트롤러 · health**

`.../gatewayapp/config/GatewayConfiguration.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

@ConfigurationProperties("bank.gateway")
data class GatewayBankProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** api 와 같은 규칙 (KST 벽시계 "HH:mm", 시작 포함 · 끝 제외, 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class GatewayMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

/** 조립은 여기서만. 은행 연결은 이 앱에 하나뿐이다 (스펙 §2 결정 7). */
@Configuration(proxyBeanMethods = false)
class GatewayConfiguration {

    @Bean
    fun clock(): Clock = Clock.systemUTC()

    /** 점검 중에는 전문을 보내지 않는다 (P5-1) — 503 BANK_NOT_SENT 로 돌려준다 (호출자에게 "안 보냄"). */
    @Bean(destroyMethod = "close")
    fun bankGatewayClient(props: GatewayBankProperties, maintenance: GatewayMaintenanceProperties, clock: Clock): BankGatewayClient {
        val window = maintenance.toWindow()
        return BankGatewayClient(
            BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval),
            clock,
            sendGate = { if (window.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()
    }

    @Bean
    fun bankConnectionHealthIndicator(client: BankGatewayClient) = BankConnectionHealthIndicator(client)
}
```

`.../gatewayapp/health/BankConnectionHealthIndicator.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.health

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

/**
 * 은행 회선 상태 — /actuator/health/bankConnection (P8-1). P8-3 에서 K8s readiness 로 쓴다:
 * 회선이 죽은 게이트웨이는 트래픽을 받지 않고, 호출자는 연결 실패 = "안 보냄" 으로 해석한다 (스펙 §4-3 과 맞물림).
 */
class BankConnectionHealthIndicator(private val client: BankGatewayClient) : HealthIndicator {
    override fun health(): Health =
        if (client.isConnected) {
            Health.up().withDetail("pending", client.pendingCount).build()
        } else {
            Health.down().withDetail("reason", "bank channel not connected").build()
        }
}
```
(Boot 4 는 health 를 `spring-boot-health` 로 옮겼다: `org.springframework.boot.health.contributor.HealthIndicator` — jar 로 확인함.)

`.../gatewayapp/web/GatewayEndpoints.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayError
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayErrorCodes
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayJson
import io.github.hyeokjinon.banktransfer.gateway.http.InquiryBody
import io.github.hyeokjinon.banktransfer.gateway.http.NameInquiryBody
import io.github.hyeokjinon.banktransfer.gateway.http.TransferBody
import io.github.hyeokjinon.banktransfer.gateway.http.toResult
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/**
 * 두 서버 판(WebFlux · MVC 비동기)이 같이 쓰는 엔드포인트 본문 (P8-1). 판별 컨트롤러는 이것을 감싸기만 한다 —
 * 그래서 두 판의 차이가 "Mono 로 감싸나 / future 를 그대로 돌려주나" 한 줄로 보인다.
 *
 * 돌려주는 future 는 은행 future 의 의존 future(handle)다. 호출자가 끊겨 프레임워크가 이것을 취소해도
 * CompletableFuture 의 취소는 원본으로 번지지 않는다 — 은행 송신은 그대로 간다 (스펙 §4-4).
 */
object GatewayEndpoints {
    private val log = LoggerFactory.getLogger(GatewayEndpoints::class.java)

    fun nameInquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, NameInquiryBody::class.java) { b -> bank.inquireName(b.bankCode, b.accountNo).thenApply { GatewayJson.write(it.toResult()) } }

    fun transfer(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, TransferBody::class.java) { b ->
            bank.transfer(b.bankCode, b.accountNo, b.amount, b.memo, b.reference).thenApply { GatewayJson.write(it.toResult()) }
        }

    fun inquiry(bank: BankGateway, json: String): CompletableFuture<ResponseEntity<String>> =
        call(json, InquiryBody::class.java) { b -> bank.inquire(b.reference).thenApply { GatewayJson.write(it.toResult()) } }

    private fun <B : Any> call(json: String, type: Class<B>, send: (B) -> CompletableFuture<String>): CompletableFuture<ResponseEntity<String>> {
        val body = try {
            GatewayJson.read(json, type)
        } catch (e: Exception) {
            return CompletableFuture.completedFuture(error(HttpStatus.BAD_REQUEST, GatewayError(GatewayErrorCodes.INVALID_REQUEST, "unreadable request body")))
        }
        return send(body).handle { okJson, failure -> if (failure == null) ok(okJson) else fromFailure(failure, type.simpleName) }
    }

    private fun fromFailure(failure: Throwable, what: String): ResponseEntity<String> {
        val cause = if (failure is CompletionException && failure.cause != null) failure.cause!! else failure
        val (status, error) = when (cause) {
            is BankNotSentException -> HttpStatus.SERVICE_UNAVAILABLE to GatewayError(GatewayErrorCodes.BANK_NOT_SENT, cause.message ?: "not sent")
            is BankTimeoutException -> HttpStatus.GATEWAY_TIMEOUT to
                GatewayError(GatewayErrorCodes.BANK_TIMEOUT, cause.message ?: "timeout", tranNo = cause.tranNo, timeoutMs = cause.timeout.toMillis())
            is BankUnavailableException -> HttpStatus.BAD_GATEWAY to GatewayError(GatewayErrorCodes.BANK_UNAVAILABLE, cause.message ?: "unavailable")
            else -> HttpStatus.INTERNAL_SERVER_ERROR to GatewayError(GatewayErrorCodes.INTERNAL, cause.javaClass.simpleName)
        }
        log.warn("bank call failed: request={} status={} code={} cause={}", what, status.value(), error.code, cause.javaClass.simpleName)
        return error(status, error)
    }

    private fun ok(json: String): ResponseEntity<String> = ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json)

    private fun error(status: HttpStatus, error: GatewayError): ResponseEntity<String> =
        ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(GatewayJson.write(error))
}
```

`.../gatewayapp/web/ReactiveGatewayController.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

/**
 * WebFlux 판 (기본, P8-1 스펙 §5-1). Reactor Netty 이벤트 루프가 요청을 받고, 은행 Netty 이벤트 루프가 future 를 완료하면
 * 이벤트 루프가 응답을 쓴다 — 요청당 스레드가 없다.
 * Mono.fromFuture(f, suppressCancel = true): 호출자가 끊겨 구독이 취소돼도 future 를 취소하지 않는다 (은행 송신은 되돌릴 수 없다).
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
class ReactiveGatewayController(private val bank: BankGateway) {

    @PostMapping(GatewayPaths.NAME_INQUIRIES)
    fun nameInquiry(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.nameInquiry(bank, json), true)

    @PostMapping(GatewayPaths.TRANSFERS)
    fun transfer(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.transfer(bank, json), true)

    @PostMapping(GatewayPaths.INQUIRIES)
    fun inquiry(@RequestBody json: String): Mono<ResponseEntity<String>> = Mono.fromFuture(GatewayEndpoints.inquiry(bank, json), true)
}
```
(`@ConditionalOnWebApplication` 패키지는 Boot 4 에서 `org.springframework.boot.autoconfigure.condition` 그대로인지 컴파일로 확인. 다르면 IDE 자동 import 로 맞춘다.)

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :apps:bank-gateway:test`
Expected: PASS (계약 10개 × 조합 1 · health 1). ⑩ 이 실패하면 은행 future 가 취소되고 있다는 뜻이다 — `Mono.fromFuture` 두 번째 인자(`true`)를 확인한다.

- [ ] **Step 6: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add settings.gradle.kts apps/bank-gateway
git commit -m "feat(p8-1): 은행 게이트웨이 앱 — WebFlux 판, 오류를 HTTP 로(503 BANK_NOT_SENT · 504 · 502), bankConnection health, 계약 테스트

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: api · api-reactive `mode=http` — 설정 · 빈 선택 · 흐름 테스트

**Files:**
- Modify: `apps/api/build.gradle.kts`, `apps/api-reactive/build.gradle.kts`
- Modify: `apps/api/.../api/config/AppConfiguration.kt` (`BankGatewayProperties`, `bankGatewayClient` 빈, 새 `httpBankGateway` 빈)
- Modify: `apps/api/src/main/resources/application.yml` (`bank.gateway` 블록)
- Modify: `apps/api-reactive/.../reactive/config/ReactiveProperties.kt`, `ReactiveConfiguration.kt`, `apps/api-reactive/src/main/resources/application.yml`
- Create: `modules/bank-gateway-http/src/testFixtures/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/fixture/GatewayProcess.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/gateway/{HttpGatewayFlowTest,HttpGatewayCoroutineFlowTest,BankGatewayModeTest}.kt`, `apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/reactive/flow/HttpGatewayReactiveFlowTest.kt`

**Interfaces:**
- Consumes: Task 1 `ApiIntegrationTest.gateway: BankGateway` · `bankConnected()` · `withBankDown`, `ReactiveIntegrationTest.bankConnected()`; Task 2 `HttpBankGateway.create(kind, settings)` · `HttpGatewaySettings` · `TcpCutProxy`; Task 3 앱 main `io.github.hyeokjinon.banktransfer.gatewayapp.BankGatewayApplicationKt`
- Produces:
  ```kotlin
  // api · api-reactive 공통 설정 모양
  data class BankGatewayProperties(val mode: String = "direct", val host, val port, val responseTimeout, val pollInterval, val http: Http = Http()) {
      data class Http(val baseUrl: String = "http://localhost:8082", val client: String = "webclient", val maxConnections: Int = 500,
                      val connectTimeout: Duration = 1s, val pendingAcquireTimeout: Duration = 1s, val responseTimeout: Duration = 5s) { fun toSettings(): HttpGatewaySettings }
      companion object { const val DIRECT = "direct"; const val HTTP = "http"; val MODES; val CLIENTS }
  }
  // testFixtures
  class GatewayProcess : AutoCloseable { val port: Int; val baseUrl: String; fun bankConnected(): Boolean
      companion object { const val CLASSPATH_PROPERTY = "bank-gateway.classpath"; fun start(bankPort: Int, vararg args: String): GatewayProcess } }
  ```

- [ ] **Step 1: 실패하는 설정 테스트**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/gateway/BankGatewayModeTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.config.BankGatewayProperties
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BankGatewayModeTest {
    @Test
    fun `기본은 direct, http 설정 기본값은 스펙 §6`() {
        val p = BankGatewayProperties()
        assertEquals("direct", p.mode)
        assertEquals("http://localhost:8082", p.http.baseUrl)
        assertEquals("webclient", p.http.client)
        assertEquals(500, p.http.maxConnections)
        assertEquals(Duration.ofSeconds(5), p.http.toSettings().responseTimeout)
    }

    @Test
    fun `모드 · 클라이언트는 소문자 목록 안에서만`() {
        assertFailsWith<IllegalArgumentException> { BankGatewayProperties(mode = "HTTP") }
        assertFailsWith<IllegalArgumentException> { BankGatewayProperties.Http(client = "okhttp") }
    }
}
```

Run: `./gradlew :apps:api:test --tests "*BankGatewayModeTest*"`
Expected: 컴파일 실패 (`mode`, `http` 없음).

- [ ] **Step 2: api 설정 · 빈 선택**

`apps/api/build.gradle.kts` dependencies 에 추가 (bank-gateway 줄 다음):
```kotlin
    implementation(project(":modules:bank-gateway-http"))   // mode=http: 게이트웨이 앱을 부르는 클라이언트 (P8-1)
```
테스트용 (testImplementation 목록 끝):
```kotlin
    testImplementation(testFixtures(project(":modules:bank-gateway-http")))   // TcpCutProxy · GatewayProcess (P8-1)
```
게이트웨이 앱을 **별도 프로세스**로 띄우기 위한 클래스패스 (파일 끝, `tasks.withType<Test>` 블록 위):
```kotlin
// http 모드 흐름 테스트가 게이트웨이 앱을 별도 JVM 으로 띄운다 (P8-1). 같은 JVM 에 올리면 이 앱의 JPA · Kafka 자동 설정이
// 게이트웨이 컨텍스트에도 걸린다 — 클래스패스를 따로 받아 java -cp 로 실행한다.
val bankGatewayRuntime: Configuration by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
    }
}
dependencies { bankGatewayRuntime(project(":apps:bank-gateway")) }
```
`tasks.withType<Test>` 블록에 추가:
```kotlin
    inputs.files(bankGatewayRuntime)
    jvmArgumentProviders.add(CommandLineArgumentProvider { listOf("-Dbank-gateway.classpath=" + bankGatewayRuntime.asPath) })
```
(`import org.gradle.api.attributes.*` 는 Kotlin DSL 에 기본으로 열려 있다. 변형 해석 오류가 나면 `./gradlew :apps:api:dependencies --configuration bankGatewayRuntime` 로 고른 변형을 본다.)

`AppConfiguration.kt` — `BankGatewayProperties` 교체:
```kotlin
/**
 * 은행 게이트웨이 (P8-1). mode: direct(기본 — 이 프로세스가 Netty 로 FEP 에 직결) | http(게이트웨이 앱을 HTTP 로 부름, apps/bank-gateway).
 * host · port · response-timeout · poll-interval 은 direct 일 때만 쓴다. http.* 는 http 일 때만 (스펙 §6).
 */
@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val mode: String = DIRECT,
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
    val http: Http = Http(),
) {
    init {
        require(mode in MODES) { "bank.gateway.mode must be one of $MODES (lower case): '$mode'" }
    }

    data class Http(
        val baseUrl: String = "http://localhost:8082",
        val client: String = HttpBankGateway.WEBCLIENT,
        val maxConnections: Int = 500,
        val connectTimeout: Duration = Duration.ofSeconds(1),
        val pendingAcquireTimeout: Duration = Duration.ofSeconds(1),
        val responseTimeout: Duration = Duration.ofSeconds(5),
    ) {
        init {
            require(client in CLIENTS) { "bank.gateway.http.client must be one of $CLIENTS (lower case): '$client'" }
        }

        fun toSettings() = HttpGatewaySettings(baseUrl, maxConnections, connectTimeout, pendingAcquireTimeout, responseTimeout)
    }

    companion object {
        const val DIRECT = "direct"
        const val HTTP = "http"
        val MODES = listOf(DIRECT, HTTP)
        val CLIENTS = listOf(HttpBankGateway.WEBCLIENT)
    }
}
```
`bankGatewayClient` 빈에 조건을 단다 (본문 그대로):
```kotlin
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.DIRECT, matchIfMissing = true)
    fun bankGatewayClient(...): BankGatewayClient = ...
```
새 빈 (같은 클래스 안, `bankGatewayClient` 다음):
```kotlin
    /**
     * mode=http (P8-1): 은행 연결은 게이트웨이 앱이 가진다. 이 프로세스는 HTTP 로만 부른다.
     * 응답 시간 초과가 게이트웨이의 은행 응답 시간 초과(3s) + 여유보다 짧으면 "모름" 이 쓸데없이 는다 — 경고만 (측정 · 테스트가 일부러 줄일 수 있게).
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.HTTP)
    fun httpBankGateway(props: BankGatewayProperties): HttpBankGateway {
        if (props.http.responseTimeout < Duration.ofSeconds(5)) {
            log.warn("bank.gateway.http.response-timeout={} is shorter than 5s (gateway bank timeout 3s + margin) — more results will be unknown", props.http.responseTimeout)
        }
        log.info("bank gateway mode=http client={} baseUrl={}", props.http.client, props.http.baseUrl)
        return HttpBankGateway.create(props.http.client, props.http.toSettings())
    }
```
`AppConfiguration` 클래스에 `private val log = LoggerFactory.getLogger(javaClass)` 가 없으면 추가하고, import 에 `HttpBankGateway` · `HttpGatewaySettings` · `ConditionalOnProperty` · `LoggerFactory` 를 더한다.

`apps/api/src/main/resources/application.yml` 의 `bank.gateway` 블록을:
```yaml
bank:
  gateway:
    mode: direct               # direct(기본, Netty 직결) | http(게이트웨이 앱 — apps/bank-gateway, P8-1)
    host: localhost            # direct 만
    port: 9090                 # direct 만
    response-timeout: 3s       # direct 만
    poll-interval: 30s         # direct 만
    http:                      # http 만 (스펙 §6). 시간 순서: 연결 · 풀 대기 1s < 게이트웨이 은행 응답 3s < 응답 5s
      base-url: http://localhost:8082
      client: webclient        # webclient | jdk (P8-1 비교)
      max-connections: 500     # WebClient 만. 기본(코어×2 = 32)이면 동시 대기 수백에서 천장이 된다
      connect-timeout: 1s
      pending-acquire-timeout: 1s
      response-timeout: 5s
```

- [ ] **Step 3: api-reactive 같은 변경**

`apps/api-reactive/build.gradle.kts`: api 와 같은 세 가지 — `implementation(project(":modules:bank-gateway-http"))`, `testImplementation(testFixtures(project(":modules:bank-gateway-http")))`, `bankGatewayRuntime` 구성 + `tasks.withType<Test>` 에 `inputs.files` · `jvmArgumentProviders` (위 코드 그대로).

`ReactiveProperties.kt` 의 `BankGatewayProperties` 를 api 와 같은 모양(위 data class 전체 — `mode` · `http` · `companion`)으로 바꾼다. `ReactiveConfiguration.kt` 의 `bankGatewayClient` 에 같은 `@ConditionalOnProperty(... DIRECT, matchIfMissing = true)` 를 달고, 같은 `httpBankGateway` 빈을 더한다. `application.yml` 의 `bank.gateway` 블록도 api 와 같게 (`mode: direct`, `http:` 하위).

- [ ] **Step 4: 설정 테스트 통과 확인**

Run: `./gradlew :apps:api:test --tests "*BankGatewayModeTest*"`
Expected: PASS

- [ ] **Step 5: 게이트웨이 프로세스 도우미 (testFixtures)**

`modules/bank-gateway-http/src/testFixtures/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/fixture/GatewayProcess.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http.fixture

import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * 게이트웨이 앱을 별도 JVM 으로 띄운다 (P8-1 흐름 테스트). 호출하는 앱(api)과 같은 JVM 에 올리면 그 앱의 자동 설정(JPA · Kafka)이
 * 게이트웨이 컨텍스트에도 걸리므로, Gradle 이 넘겨 준 게이트웨이 런타임 클래스패스(시스템 속성 [CLASSPATH_PROPERTY])로 java 를 실행한다.
 * 클래스패스는 인자 파일(@file)로 넘긴다 — Windows 명령행 길이 제한(32K)을 피한다.
 */
class GatewayProcess private constructor(private val process: Process, val port: Int, private val log: Path) : AutoCloseable {

    val baseUrl: String get() = "http://127.0.0.1:$port"

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()

    /** /actuator/health/bankConnection 이 UP 인가 (게이트웨이가 아직 안 떴거나 꺼졌으면 false). */
    fun bankConnected(): Boolean = runCatching {
        val req = HttpRequest.newBuilder(URI.create("$baseUrl/actuator/health/bankConnection")).timeout(Duration.ofSeconds(2)).GET().build()
        http.send(req, HttpResponse.BodyHandlers.ofString()).body().contains("\"UP\"")
    }.getOrDefault(false)

    private fun awaitBankConnection(timeout: Duration) {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (!bankConnected()) {
            check(process.isAlive) { "gateway process exited — log: $log\n" + Files.readString(log).takeLast(4000) }
            check(System.nanoTime() < deadline) { "gateway not connected to bank within $timeout — log: $log" }
            Thread.sleep(100)
        }
    }

    override fun close() {
        process.destroy()
        if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
    }

    companion object {
        const val CLASSPATH_PROPERTY = "bank-gateway.classpath"
        const val MAIN_CLASS = "io.github.hyeokjinon.banktransfer.gatewayapp.BankGatewayApplicationKt"

        /** [bankPort] 의 모의 은행에 붙는 게이트웨이를 빈 포트에 띄우고, 은행 연결이 UP 이 될 때까지 기다린다. */
        fun start(bankPort: Int, vararg args: String): GatewayProcess {
            val classpath = System.getProperty(CLASSPATH_PROPERTY)
                ?: error("system property $CLASSPATH_PROPERTY is missing — run through Gradle (build.gradle.kts bankGatewayRuntime)")
            val port = ServerSocket(0).use { it.localPort }
            val argFile = Files.createTempFile("bank-gateway-cp-", ".args")
            Files.writeString(argFile, "-cp \"" + classpath.replace('\\', '/') + "\"\n")
            val log = Files.createTempFile("bank-gateway-", ".log")
            val java = ProcessHandle.current().info().command().orElse("java")
            val command = listOf(
                java, "@$argFile", "-Xmx256m", "-Dstdout.encoding=UTF-8", MAIN_CLASS,
                "--server.port=$port", "--bank.gateway.host=127.0.0.1", "--bank.gateway.port=$bankPort",
                "--bank.maintenance.enabled=false", *args,
            )
            val process = ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start()
            return GatewayProcess(process, port, log).also { it.awaitBankConnection(Duration.ofSeconds(60)) }
        }
    }
}
```

- [ ] **Step 6: 실패하는 흐름 테스트 (api)**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/gateway/HttpGatewayFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.TcpCutProxy
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

/**
 * mode=http (P8-1, 스펙 §7): api → (끊는 프록시) → 게이트웨이 앱(별도 JVM) → 모의 은행.
 * 돈이 걸린 두 경로가 분리 뒤에도 그대로인지: 게이트웨이가 꺼짐 → 안 보냄 → 재시도 토픽, 받고 끊음 → 모름 → 결과조회.
 * 컨텍스트가 기본 테스트와 달라(새 MySQL · Kafka 빈) 클래스 끝에 닫는다 (P7: 닫히지 않은 컨텍스트의 리스너가 CI 4코어를 묶은 일).
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayFlowTest : ApiIntegrationTest() {

    companion object {
        private val gatewayApp: GatewayProcess by lazy { GatewayProcess.start(bank.boundPort, "--bank.gateway.response-timeout=500ms") }
        val proxy: TcpCutProxy by lazy { TcpCutProxy(gatewayApp.port).start() }

        @JvmStatic
        @DynamicPropertySource
        fun httpGateway(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.mode") { "http" }
            registry.add("bank.gateway.http.base-url") { proxy.baseUrl }
            registry.add("bank.gateway.http.response-timeout") { "2s" }
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() { proxy.close(); gatewayApp.close() }
    }

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @AfterEach
    fun resetProxy() { proxy.mode = TcpCutProxy.Mode.PASS; proxy.resume() }

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}
    private val user by lazy { fundedUser(1_000_000) }

    private fun request(accountNo: String, memo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 10_000, "memo" to memo))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun memo() = "H" + Random.nextInt(100_000, 999_999)

    @Test
    fun `http 모드에서는 Netty 직결 빈 대신 HTTP 클라이언트가 은행 게이트웨이다`() {
        assertIs<HttpBankGateway>(gateway)
    }

    @Test
    fun `출금이 게이트웨이 앱을 거쳐 COMPLETED, 은행 입금 1건`() {
        val account = verifiedBy(user, "1002123456789")
        val id = request(account, memo())
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        val ref = store.findById(id)!!.reference
        assertEquals(1, bank.depositsOf(account).count { it.reference == ref })
    }

    @Test
    fun `1원인증 SENT 뒤 적요 코드로 VERIFIED`() {
        val userId = newUserId()
        val created = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .retrieve().body(mapType)!!
        assertEquals("SENT", created["status"])
        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        val confirmed = client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, userId)
            .body(mapOf("code" to code))
            .retrieve().body(mapType)!!
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `게이트웨이가 꺼져 있으면 안 보냄 - UNKNOWN 이 되지 않고, 다시 켜지면 재시도로 COMPLETED`() {
        val account = verifiedBy(user, "1002123456789")
        proxy.pause()                                   // 연결 거부 = 게이트웨이가 요청을 받지 못했다
        Thread.sleep(100)
        val id = request(account, memo())
        relay.publishPending()
        val deadline = System.currentTimeMillis() + 1_500
        while (System.currentTimeMillis() < deadline) {  // 재시도 토픽 0.5s · 1s 가 도는 동안
            assertNotEquals(TransferStatus.UNKNOWN, store.findById(id)!!.status)
            Thread.sleep(100)
        }
        proxy.resume()
        await.atMost(Duration.ofSeconds(20)) untilAsserted { assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status) }
        val ref = store.findById(id)!!.reference
        assertEquals(1, bank.depositsOf(account).count { it.reference == ref })
    }

    @Test
    fun `게이트웨이가 받고 응답 전에 끊기면 UNKNOWN, 결과조회(HTTP)로 COMPLETED - 이체 전문은 한 번`() {
        val account = verifiedBy(user, "1002123456789")
        proxy.mode = TcpCutProxy.Mode.DROP_RESPONSE
        val id = request(account, memo())
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.UNKNOWN, store.findById(id)!!.status)
        }
        proxy.mode = TcpCutProxy.Mode.PASS
        TransferReconciler(store, BankInquiryAdapter(gateway), Clock.offset(Clock.systemUTC(), Duration.ofSeconds(31))).reconcile()
        val after = store.findById(id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })
    }
}
```
(`USER_ID_HEADER` 의 위치 · `TransferReconciler` 생성자 시그니처는 `ReconcileFlowTest` 가 쓰는 것과 같다 — import 경로가 다르면 그 파일의 import 를 그대로 따른다.)

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/gateway/HttpGatewayCoroutineFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals

/** mode=http + 코루틴 컨슈머 (P6-1 판): suspend 어댑터가 HTTP 판 future 를 await 해도 같은 결과. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayCoroutineFlowTest : ApiIntegrationTest() {

    companion object {
        private val gatewayApp: GatewayProcess by lazy { GatewayProcess.start(bank.boundPort, "--bank.gateway.response-timeout=500ms") }

        @JvmStatic
        @DynamicPropertySource
        fun httpGateway(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.mode") { "http" }
            registry.add("bank.gateway.http.base-url") { gatewayApp.baseUrl }
            registry.add("bank.gateway.http.response-timeout") { "2s" }
            registry.add("transfer.consumer.mode") { "coroutine" }
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() = gatewayApp.close()
    }

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @Test
    fun `코루틴 컨슈머도 게이트웨이 앱을 거쳐 COMPLETED`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, "1002123456789")
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "HC1"))
            .retrieve().body(object : ParameterizedTypeReference<Map<String, Any?>>() {})!!
        val id = UUID.fromString(body["id"] as String)
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
    }
}
```

`apps/api-reactive/src/test/kotlin/io/github/hyeokjinon/banktransfer/reactive/flow/HttpGatewayReactiveFlowTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.reactive.support.ReactiveIntegrationTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import kotlin.test.assertEquals

/** api-reactive mode=http (P8-1): 코루틴 판 1원인증이 게이트웨이 앱을 거쳐 SENT. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayReactiveFlowTest : ReactiveIntegrationTest() {

    companion object {
        private val gatewayApp: GatewayProcess by lazy { GatewayProcess.start(bank.boundPort, "--bank.gateway.response-timeout=500ms") }

        @JvmStatic
        @DynamicPropertySource
        fun httpGateway(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.mode") { "http" }
            registry.add("bank.gateway.http.base-url") { gatewayApp.baseUrl }
            registry.add("bank.gateway.http.response-timeout") { "2s" }
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() = gatewayApp.close()
    }

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @Test
    fun `1원인증 요청이 게이트웨이 앱을 거쳐 201 SENT`() {
        val userId = newUserId()
        val (status, body) = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header("X-User-Id", userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() to (res.bodyTo(object : ParameterizedTypeReference<Map<String, Any?>>() {}) ?: emptyMap()) }
        assertEquals(201, status)
        assertEquals("SENT", body["status"])
        assertEquals(1, rowsOf(userId))
    }
}
```
(리액티브 앱의 기본 판이 코루틴이 아니면 — `CoroutineVerificationFlowTest` 가 판을 고르는 방법(속성)을 그대로 `httpGateway` 에 더한다.)

- [ ] **Step 7: 흐름 테스트 확인**

Run: `./gradlew :apps:api:test --tests "*HttpGateway*" :apps:api-reactive:test --tests "*HttpGatewayReactiveFlowTest*"`
Expected: PASS (api 5 + 1, reactive 1). 게이트웨이가 뜨지 않으면 실패 메시지에 로그 경로 · 마지막 4,000자가 나온다.
"꺼짐" 테스트가 UNKNOWN 으로 실패하면: 프록시를 닫은 직후 연결 풀에 남은 연결로 요청이 나갔다는 뜻이다 — `proxy.pause()` 뒤 대기(100ms)를 늘리지 말고, 원인(풀의 끊긴 연결이 "응답 전 끊김" 으로 분류됨)을 보고에 적는다. 이것은 실제 운영에서도 생기는 "모름" 이며 정리 작업이 결과조회로 확정한다 — 테스트는 그때 `withBankDown` 처럼 "연결 거부 상태가 된 뒤 접수" 순서를 보장하도록 고친다.

- [ ] **Step 8: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add apps/api apps/api-reactive modules/bank-gateway-http
git commit -m "feat(p8-1): api · api-reactive bank.gateway.mode=http — 게이트웨이 앱을 HTTP 로, 꺼짐은 재시도 토픽 · 끊김은 결과조회로 확정

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 비교 판 — MVC 비동기 서버 · JDK 클라이언트, 계약 테스트 4조합

**Files:**
- Modify: `apps/bank-gateway/build.gradle.kts` (webmvc 스타터)
- Create: `apps/bank-gateway/src/main/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/web/ServletGatewayController.kt`
- Modify: `apps/bank-gateway/src/main/resources/application.yml` (`spring.mvc.async.request-timeout`)
- Create: `modules/bank-gateway-http/src/main/kotlin/io/github/hyeokjinon/banktransfer/gateway/http/JdkHttpBankGateway.kt`
- Modify: `modules/bank-gateway-http/.../gateway/http/HttpBankGateway.kt` (팩토리 `JDK`)
- Modify: `apps/api/.../config/AppConfiguration.kt` · `apps/api-reactive/.../config/ReactiveProperties.kt` (`CLIENTS` 에 jdk)
- Modify: `apps/bank-gateway/src/test/.../GatewayContractTest.kt` (`combos()` 4개)
- Test: `modules/bank-gateway-http/src/test/.../JdkHttpBankGatewayTest.kt`

**Interfaces:**
- Consumes: Task 2 `HttpFailures` · `GatewayJson` · DTO · `HttpGatewaySettings`; Task 3 `GatewayEndpoints`
- Produces: `HttpBankGateway.JDK = "jdk"`, `class JdkHttpBankGateway(settings: HttpGatewaySettings) : HttpBankGateway`, `ServletGatewayController`

- [ ] **Step 1: 실패하는 테스트 — JDK 클라이언트 · 조합 확장**

`JdkHttpBankGatewayTest.kt` 는 `WebClientBankGatewayTest` 를 복사해 클래스 이름과 `gateway(...)` 만 바꾼다:
```kotlin
    private fun gateway(baseUrl: String = "http://127.0.0.1:${server.address.port}", responseTimeout: Duration = Duration.ofSeconds(2)) =
        JdkHttpBankGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = responseTimeout)).also { closeables += it }
```
(나머지 6개 테스트 본문은 같다 — 두 판이 같은 표를 지킨다는 증명이다. 마지막 테스트 이름을 "받고 응답 전에 끊기면 모름이고, 다시 보내지 않는다 (JDK 는 POST 를 재시도하지 않는 기본값)" 으로.)

`GatewayContractTest.combos()`:
```kotlin
        @JvmStatic
        fun combos() = listOf(
            Combo(WebApplicationType.REACTIVE, HttpBankGateway.WEBCLIENT),
            Combo(WebApplicationType.REACTIVE, HttpBankGateway.JDK),
            Combo(WebApplicationType.SERVLET, HttpBankGateway.WEBCLIENT),
            Combo(WebApplicationType.SERVLET, HttpBankGateway.JDK),
        )
```

Run: `./gradlew :modules:bank-gateway-http:test :apps:bank-gateway:test`
Expected: 컴파일 실패 (`JdkHttpBankGateway`, `HttpBankGateway.JDK` 없음).

- [ ] **Step 2: JDK 판**

`JdkHttpBankGateway.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gateway.http

import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture

/**
 * JDK java.net.http.HttpClient 판 — 비교용 (P8-1, 스펙 §5-2).
 * sendAsync 가 CompletableFuture 를 그대로 돌려준다. 응답은 셀렉터 스레드가 읽고, future 완료 · 뒤처리는 기본 실행기
 * (늘어나는 캐시 스레드 풀 — 이것 자체가 비교 대상이라 바꾸지 않는다)에서 돈다.
 * 연결 풀 개수 상한이 없고 조절할 수 없다. POST 는 연결이 끊겨도 다시 보내지 않는다 (jdk.httpclient.enableAllMethodRetry 기본 false).
 * HTTP/2 를 먼저 시도하는 기본값 대신 HTTP/1.1 로 고정한다 — WebClient 판과 같은 조건으로 비교한다.
 */
class JdkHttpBankGateway(private val settings: HttpGatewaySettings) : HttpBankGateway {

    private val http: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(settings.connectTimeout)
        .build()

    override fun inquireName(bankCode: String, accountNo: String): CompletableFuture<NameInquiryResponse> =
        post(GatewayPaths.NAME_INQUIRIES, NameInquiryBody(bankCode, accountNo)) { GatewayJson.read(it, NameInquiryResult::class.java).toResponse() }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): CompletableFuture<TransferResponse> =
        post(GatewayPaths.TRANSFERS, TransferBody(bankCode, accountNo, amount, memo, reference)) { GatewayJson.read(it, TransferResult::class.java).toResponse() }

    override fun inquire(reference: String): CompletableFuture<InquiryResponse> =
        post(GatewayPaths.INQUIRIES, InquiryBody(reference)) { GatewayJson.read(it, InquiryResult::class.java).toResponse() }

    private fun <T> post(path: String, body: Any, parse: (String) -> T): CompletableFuture<T> {
        val request = HttpRequest.newBuilder(URI.create(settings.baseUrl + path))
            .timeout(settings.responseTimeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(GatewayJson.write(body)))
            .build()
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .handle { response, error ->
                if (error != null) throw HttpFailures.fromTransport(error)
                HttpFailures.parse(response.statusCode(), response.body(), parse)
            }
    }

    override fun close() {
        http.close()   // Java 21: HttpClient 는 AutoCloseable
    }
}
```

`HttpBankGateway.kt` companion:
```kotlin
    companion object {
        const val WEBCLIENT = "webclient"
        const val JDK = "jdk"

        fun create(kind: String, settings: HttpGatewaySettings): HttpBankGateway = when (kind) {
            WEBCLIENT -> WebClientBankGateway(settings)
            JDK -> JdkHttpBankGateway(settings)
            else -> throw IllegalArgumentException("unknown http client '$kind' (webclient | jdk)")
        }
    }
```
api `AppConfiguration.kt` 와 api-reactive `ReactiveProperties.kt` 의 `CLIENTS`:
```kotlin
        val CLIENTS = listOf(HttpBankGateway.WEBCLIENT, HttpBankGateway.JDK)
```
`BankGatewayModeTest` 의 두 번째 테스트에 `BankGatewayProperties.Http(client = "jdk")` 가 예외 없이 만들어지는 줄을 더한다.

- [ ] **Step 3: MVC 비동기 판**

`apps/bank-gateway/build.gradle.kts` dependencies 에 (webflux 줄 다음):
```kotlin
    implementation("org.springframework.boot:spring-boot-starter-webmvc")   // servlet 판 (MVC 비동기, P8-1 비교). 둘 다 있으면 web-application-type 이 고른다
```
`ServletGatewayController.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp.web

import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.CompletableFuture

/**
 * MVC 비동기 판 — 비교용 (P8-1 스펙 §5-1). Tomcat 스레드가 요청을 받고 future 를 돌려준 뒤 바로 반납된다.
 * 은행 응답으로 future 가 완료되면 AsyncContext 재디스패치로 Tomcat 스레드가 다시 잡혀 응답을 쓴다 (WebFlux 판과의 차이 — 한 요청에 Tomcat 스레드 두 번).
 * 돌려주는 future 는 은행 future 의 의존 future 라, 비동기 시간 초과 · 끊김으로 Spring 이 이것을 취소해도 은행 송신은 취소되지 않는다.
 */
@RestController
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
class ServletGatewayController(private val bank: BankGateway) {

    @PostMapping(GatewayPaths.NAME_INQUIRIES)
    fun nameInquiry(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.nameInquiry(bank, json)

    @PostMapping(GatewayPaths.TRANSFERS)
    fun transfer(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.transfer(bank, json)

    @PostMapping(GatewayPaths.INQUIRIES)
    fun inquiry(@RequestBody json: String): CompletableFuture<ResponseEntity<String>> = GatewayEndpoints.inquiry(bank, json)
}
```
`application.yml` 의 `spring:` 아래:
```yaml
  mvc:
    async:
      request-timeout: 30s     # servlet 판만. 은행 응답 시간 초과(3s)보다 길게 — 먼저 끊으면 은행 결과를 버린다
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:bank-gateway-http:test :apps:bank-gateway:test`
Expected: PASS — 계약 10개 × 4조합 (⑧ 은 JDK 두 조합에서 건너뜀), JDK 클라이언트 6개.
servlet 조합이 기동하지 않으면: 두 스타터가 같이 있을 때 `--spring.main.web-application-type=servlet` 로 Tomcat 이 뜨는지 로그로 확인한다 (`GatewayApps.start` 가 `.web(server)` 와 속성을 둘 다 준다).

- [ ] **Step 5: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add apps/bank-gateway modules/bank-gateway-http apps/api apps/api-reactive
git commit -m "feat(p8-1): 비교 판 — 게이트웨이 MVC 비동기 서버 · JDK HttpClient 클라이언트, 계약 테스트 4조합

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Toxiproxy — Compose `chaos` 프로필 · 망 지연 · 끊김 테스트

**Files:**
- Modify: `infra/docker-compose.yml` (`k6` 서비스 앞에 `toxiproxy`)
- Modify: `infra/README.md` (사용법 절)
- Modify: `apps/bank-gateway/build.gradle.kts` (테스트 의존성)
- Test: `apps/bank-gateway/src/test/kotlin/io/github/hyeokjinon/banktransfer/gatewayapp/ToxiproxyGatewayTest.kt`

**Interfaces:**
- Consumes: Task 3 `GatewayApps.start` · `port()`, Task 2 `WebClientBankGateway` · `HttpGatewaySettings`
- Produces: Compose 서비스 `toxiproxy` (제어 8474, 프록시 28082), Task 7 이 쓴다

- [ ] **Step 1: Compose · 문서**

`infra/docker-compose.yml` — `k6` 서비스 주석 앞에:
```yaml
  # 망 지연 · 끊김 주입 (P8-1). 기본 실행에는 포함 안 됨 — 측정 스크립트(load/lib/gateway.sh)가 쓴다:
  #   docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy
  # 제어 API :8474 (프록시 · toxic 만들기), 프록시 :28082 → PC 에서 띄운 측정용 게이트웨이(host.docker.internal:18082)
  toxiproxy:
    image: ghcr.io/shopify/toxiproxy:2.9.0
    profiles: ["chaos"]
    ports:
      - "8474:8474"
      - "28082:28082"
    extra_hosts:
      - "host.docker.internal:host-gateway"   # 리눅스(CI)에서도 PC(호스트)를 이 이름으로 (Docker Desktop 은 기본 제공)
```
`infra/README.md` 에 절 추가 (기존 `tools` · `load` 프로필 설명 옆):
```markdown
### 망 지연 · 끊김 주입 — `chaos` 프로필 (P8-1)
api 와 게이트웨이 앱을 같은 PC 에서 띄우면 망 지연이 거의 0 이다. Toxiproxy 를 사이에 두어 "다른 서버를 부르는" 지연 · 끊김을 만든다.
- 띄우기: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy`
- 제어 API `http://localhost:8474` — 측정 스크립트(`load/lib/gateway.sh`)가 프록시 `p81-gateway`(:28082 → `host.docker.internal:18082`)와 지연 toxic 을 만들고 지운다.
- 손으로: `curl -s localhost:8474/proxies` 로 남은 프록시 · toxic 확인, `curl -X DELETE localhost:8474/proxies/p81-gateway` 로 지운다.
- 내리기: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos stop toxiproxy`
```

- [ ] **Step 2: 실패하는 테스트**

`apps/bank-gateway/build.gradle.kts` testImplementation 에:
```kotlin
    testImplementation("org.testcontainers:testcontainers-toxiproxy")   // 버전은 Boot BOM (Testcontainers 2.0.5)
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("eu.rekawek.toxiproxy:toxiproxy-java:2.1.7")    // Toxiproxy 제어 클라이언트 (BOM 밖 — 버전 고정)
```
`ToxiproxyGatewayTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.gatewayapp

import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.gateway.http.WebClientBankGateway
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.Testcontainers
import org.testcontainers.junit.jupiter.Testcontainers as TestcontainersTest
import org.testcontainers.toxiproxy.ToxiproxyContainer
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * 실제 망 장애에 가까운 재현 (P8-1 스펙 §7): 호출자 → Toxiproxy(컨테이너) → 게이트웨이 앱(이 JVM) → 모의 은행.
 * 지연은 그만큼 늦게 성공하고, 응답이 막히면(timeout toxic) 호출자는 "모름" — 은행에는 한 번만 갔다.
 */
@TestcontainersTest(disabledWithoutDocker = true)
class ToxiproxyGatewayTest {

    companion object {
        private val bank = MockBankServer().start()
        private lateinit var app: ConfigurableApplicationContext
        private lateinit var toxiproxy: ToxiproxyContainer
        private lateinit var proxy: Proxy
        private lateinit var baseUrl: String

        @JvmStatic
        @BeforeAll
        fun up() {
            app = GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort)
            Testcontainers.exposeHostPorts(app.port())                       // 컨테이너가 host.testcontainers.internal 로 이 JVM 의 게이트웨이에 닿게
            toxiproxy = ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0").also { it.start() }
            proxy = ToxiproxyClient(toxiproxy.host, toxiproxy.controlPort)
                .createProxy("gateway", "0.0.0.0:8666", "host.testcontainers.internal:${app.port()}")
            baseUrl = "http://${toxiproxy.host}:${toxiproxy.getMappedPort(8666)}"
            await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(app.getBean(BankGatewayClient::class.java).isConnected) }
        }

        @JvmStatic
        @AfterAll
        fun down() { toxiproxy.stop(); app.close(); bank.close() }
    }

    private val client = WebClientBankGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = Duration.ofSeconds(2)))

    @AfterEach
    fun clean() { proxy.toxics().all.forEach { it.remove() }; client.close() }

    private fun ref() = "T261110" + UUID.randomUUID().toString().replace("-", "").take(13).uppercase()

    @Test
    fun `지연 300ms 를 넣으면 그만큼 늦게 성공`() {
        proxy.toxics().latency("lat", ToxicDirection.DOWNSTREAM, 300)
        val started = System.nanoTime()
        val res = client.transfer("090", "1002123456789", 1_000, "TX1", ref()).get(5, TimeUnit.SECONDS)
        assertEquals("0000", res.header.responseCode)
        assertTrue(Duration.ofNanos(System.nanoTime() - started) >= Duration.ofMillis(300))
    }

    @Test
    fun `응답이 막히면 모름 - 은행에는 한 번만 갔다`() {
        proxy.toxics().timeout("cut", ToxicDirection.DOWNSTREAM, 0)       // 응답 방향을 막는다 (요청은 통과)
        val r = ref()
        val e = (runCatching { client.transfer("090", "1002123456789", 1_000, "TX2", r).get(5, TimeUnit.SECONDS) }.exceptionOrNull() as ExecutionException).cause!!
        assertIs<BankUnavailableException>(e)
        assertTrue(e !is BankNotSentException)
        await.atMost(Duration.ofSeconds(3)) untilAsserted { assertEquals(1, bank.depositsOf("1002123456789").count { it.reference == r }) }
    }
}
```
(Testcontainers 2 · toxiproxy-java 의 패키지 · 메서드 이름(`ToxiproxyContainer.controlPort`, `toxics().latency(...)`, `.timeout(...)`)은 컴파일로 확인한다. 이름이 다르면 해당 라이브러리 소스 jar 를 보고 맞추되, 시나리오 두 개는 바꾸지 않는다.)

- [ ] **Step 3: 확인**

Run: `./gradlew :apps:bank-gateway:test --tests "*ToxiproxyGatewayTest*"` (Docker Desktop 켜짐)
Expected: PASS 2.
Run: `docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy && curl -s localhost:8474/version && docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos stop toxiproxy`
Expected: 버전 문자열 출력.

- [ ] **Step 4: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add infra/docker-compose.yml infra/README.md apps/bank-gateway
git commit -m "test(p8-1): Toxiproxy 망 지연 · 끊김 — Compose chaos 프로필, 응답이 막혀도 은행 송신은 한 번

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 측정 도구 — 게이트웨이 · 판 · 망 지연을 고르는 환경 변수

**Files:**
- Create: `load/lib/gateway.sh`
- Modify: `load/consumer/run.sh` (사용법 주석 8 ~ 19행, 이름 113행, 시작 검사 160 ~ 165행, 처리 단계 220 ~ 270행, 결과 JSON)
- Modify: `load/verification/run.sh` (사용법 주석, 포트 검사 97행, 앱 인자 127 ~ 135행, 결과 JSON)
- Modify: `load/consumer/summarize.sh`, `load/verification/summarize.sh` (게이트웨이 열)
- Modify: `OPERATION.md` (§11 · §12 에 P8-1 측정 줄)

**Interfaces:**
- Consumes: Task 3 게이트웨이 bootJar (`apps/bank-gateway/build/libs/bank-gateway-0.1.0-SNAPSHOT.jar`), `/actuator/health/bankConnection`, `/actuator/metrics`; Task 6 Toxiproxy 8474 · 28082; 기존 `load/lib/proc.sh` 의 `port_busy` · `jvm_pid` · `jdk_bin`, run.sh 의 `stop_pid` · `free_port` · `die` · `SAMPLERS`
- Produces: 환경 변수 `GATEWAY=direct|http` `SERVER=reactive|servlet` `CLIENT=webclient|jdk` `GW_POOL=<n>` `NET_DELAY=<ms>`; 원자료 이름 접미사 `-gw-<server>-<client>[-p<pool>][-n<ms>ms]`; 결과 JSON 키 `gateway` · `gw_cpu_avg` · `gw_cpu_max` · `gw_threads_max`

- [ ] **Step 1: 공통 라이브러리**

`load/lib/gateway.sh`:
```bash
# shellcheck shell=bash
# 측정용 게이트웨이 앱 (P8-1) — load/consumer/run.sh · load/verification/run.sh 가 source 한다 (lib/proc.sh 다음).
# 쓰는 변수: GATEWAY(direct|http, 기본 direct) SERVER(reactive|servlet) CLIENT(webclient|jdk) GW_POOL(WebClient 최대 연결) NET_DELAY(Toxiproxy 지연 ms)
#            BANK_PORT(측정용 모의 은행) — run.sh 가 정한다. 쓰는 함수: die · stop_pid · free_port (run.sh).
GW_PORT=18082
GW_JAR=apps/bank-gateway/build/libs/bank-gateway-0.1.0-SNAPSHOT.jar
GW_PID=
TOXI=http://localhost:8474
TOXI_PROXY=p81-gateway

gw_on() { [ "${GATEWAY:-direct}" = http ]; }

gw_validate() {
  case "${GATEWAY:-direct}" in direct|http) ;; *) die "GATEWAY 는 direct | http: '${GATEWAY}'";; esac
  case "${SERVER:-reactive}" in reactive|servlet) ;; *) die "SERVER 는 reactive | servlet: '${SERVER}'";; esac
  case "${CLIENT:-webclient}" in webclient|jdk) ;; *) die "CLIENT 는 webclient | jdk: '${CLIENT}'";; esac
  [ -z "${GW_POOL:-}" ] || [[ "$GW_POOL" =~ ^[1-9][0-9]*$ ]] || die "GW_POOL 은 양의 정수: '$GW_POOL'"
  [ -z "${NET_DELAY:-}" ] || [[ "$NET_DELAY" =~ ^[0-9]+$ ]] || die "NET_DELAY 는 ms 정수: '$NET_DELAY'"
  if ! gw_on && { [ -n "${SERVER:-}${CLIENT:-}${GW_POOL:-}${NET_DELAY:-}" ]; }; then die "SERVER · CLIENT · GW_POOL · NET_DELAY 는 GATEWAY=http 일 때만"; fi
  if gw_on; then
    [ -f "$GW_JAR" ] || die "게이트웨이 jar 가 없다 — ./gradlew :apps:bank-gateway:bootJar"
    port_busy "$GW_PORT" && die "포트 $GW_PORT 를 이미 쓰고 있다 (측정용 게이트웨이)"
    if [ -n "${NET_DELAY:-}" ]; then curl -fs "$TOXI/version" >/dev/null || die "Toxiproxy 가 없다 — docker compose ... --profile chaos up -d toxiproxy"; fi
  fi
  return 0
}

# 원자료 이름 접미사. direct 면 빈 문자열 (P6 · P7 원자료 이름과 같게)
gw_tag() { gw_on || return 0; printf -- '-gw-%s-%s%s%s' "${SERVER:-reactive}" "${CLIENT:-webclient}" "${GW_POOL:+-p$GW_POOL}" "${NET_DELAY:+-n${NET_DELAY}ms}"; }

gw_url() { if [ -n "${NET_DELAY:-}" ]; then echo "http://localhost:28082"; else echo "http://localhost:$GW_PORT"; fi; }

# API 에 더할 인자 (한 줄에 하나). direct 면 아무것도 없다
gw_api_args() {
  gw_on || return 0
  printf '%s\n' --bank.gateway.mode=http --bank.gateway.http.base-url="$(gw_url)" --bank.gateway.http.client="${CLIENT:-webclient}"
  [ -z "${GW_POOL:-}" ] || printf '%s\n' --bank.gateway.http.max-connections="$GW_POOL"
}

gw_start() {   # $1 = 로그 파일
  gw_on || return 0
  java -Xms512m -Xmx512m -Dstdout.encoding=UTF-8 -jar "$GW_JAR" --server.port="$GW_PORT" \
    --bank.gateway.port="$BANK_PORT" --bank.maintenance.enabled=false \
    --spring.main.web-application-type="${SERVER:-reactive}" > "$1" 2>&1 &
  GW_PID=$!
  local i
  for i in $(seq 1 120); do
    curl -fs "localhost:$GW_PORT/actuator/health/bankConnection" 2>/dev/null | grep -q '"UP"' && break
    kill -0 "$GW_PID" 2>/dev/null || die "게이트웨이가 기동 중에 끝났다 — $1"
    sleep 0.5
  done
  curl -fs "localhost:$GW_PORT/actuator/health/bankConnection" | grep -q '"UP"' || die "게이트웨이가 은행에 붙지 못했다 — $1"
  [ -z "${NET_DELAY:-}" ] || toxi_setup
}

gw_stop() {
  gw_on || return 0
  [ -z "${NET_DELAY:-}" ] || toxi_clear
  stop_pid "$GW_PID"; GW_PID=
  free_port "$GW_PORT" || die "게이트웨이가 포트 $GW_PORT 를 놓지 않는다"
}

# 응답 방향(downstream)에 NET_DELAY ms — 한 번 왕복에 그만큼 더한다
toxi_setup() {
  curl -fs -X DELETE "$TOXI/proxies/$TOXI_PROXY" >/dev/null 2>&1 || true
  curl -fs -X POST "$TOXI/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$TOXI_PROXY\",\"listen\":\"0.0.0.0:28082\",\"upstream\":\"host.docker.internal:$GW_PORT\",\"enabled\":true}" >/dev/null \
    || die "Toxiproxy 프록시를 만들지 못했다"
  curl -fs -X POST "$TOXI/proxies/$TOXI_PROXY/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$NET_DELAY}}" >/dev/null \
    || die "지연 toxic 을 만들지 못했다"
}

toxi_clear() { curl -fs -X DELETE "$TOXI/proxies/$TOXI_PROXY" >/dev/null 2>&1 || true; }

# 게이트웨이 지표 한 줄: "<ms> <process.cpu.usage> <jvm.threads.live>" (읽지 못하면 '-')
gw_metric() { curl -fs "localhost:$GW_PORT/actuator/metrics/$1" 2>/dev/null | grep -o '"value":[0-9.eE+-]*' | head -1 | cut -d: -f2 || echo -; }
gw_sample_line() { echo "$(date +%s%3N) $(gw_metric process.cpu.usage) $(gw_metric jvm.threads.live)"; }

# 게이트웨이 스레드 덤프 1회 (판 비교용). jcmd 는 Windows PID 가 필요하다 — 명령행의 --server.port 로 찾는다 (lib/proc.sh)
gw_dump() {   # $1 = 출력 파일
  gw_on || return 0
  local b pid; b=$(jdk_bin) || return 0; pid=$(jvm_pid "--server.port=$GW_PORT")
  [ -n "$pid" ] && "$b/jcmd" "$pid" Thread.dump_to_file -format=text "$(cygpath -w "$1" 2>/dev/null || echo "$1")" >/dev/null 2>&1 || true
}
```

- [ ] **Step 2: 컨슈머 측정에 연결 (`load/consumer/run.sh`)**

1. 사용법 주석(8행 아래)에 한 줄:
   ```bash
   #   [GATEWAY=http [SERVER=reactive|servlet] [CLIENT=webclient|jdk] [GW_POOL=n] [NET_DELAY=ms]] — P8-1: 처리 단계 API 가 게이트웨이 앱(18082)을 HTTP 로 부른다. RAW_SUB=p8-1 과 함께
   ```
2. `source load/lib/proc.sh` 줄 다음에 `source load/lib/gateway.sh`. 인자 검사가 끝난 곳(110행 근처, `BANK_PORT` 정한 뒤)에 `gw_validate`.
3. 이름(113행): 두 분기 모두 끝에 `$(gw_tag)` 를 붙인다 — `NAME="blocking$(vt_tag)-d$DELAY-h$HIKARI$(gw_tag)-$ROUND"` · `NAME="coroutine-d$DELAY-c$CAP-h$HIKARI$(gw_tag)-$ROUND"`.
4. `finish()` 에 `gw_stop 2>/dev/null || true` 를 `stop_pid "$API_PID"` 앞에.
5. 처리 단계: 모의 은행을 띄우고 기다린 직후(225행 근처) `gw_start "$WORK/gateway.log"`. 처리 단계 `start_api "$WORK/api-process.log" ...` 인자 끝에 `$(gw_api_args)` 를 펼친다:
   ```bash
   mapfile -t GW_ARGS < <(gw_api_args)
   start_api "$WORK/api-process.log" --transfer.outbox.relay.enabled=false ... "${GW_ARGS[@]}"
   ```
   (발행 단계 API 는 은행을 부르지 않으므로 그대로 둔다.)
6. 표본(244행 근처 cpu 표본 다음):
   ```bash
   if gw_on; then ( while :; do gw_sample_line; sleep 1; done ) > "$WORK/gw.txt" 2>/dev/null & SAMPLERS+=($!); fi
   ```
   처리가 끝난 직후, 샘플러를 끄기 전에 `gw_dump "$WORK/gw-threads.txt"`. `stop_api` 다음에 `gw_stop`.
7. 결과 JSON 에 키 추가 (기존 `cpu_stat` 과 같은 창 `START_MS ~ END_MS`):
   ```bash
   if gw_on; then read -r GW_CPU_AVG GW_CPU_MAX _ < <(cpu_stat_file "$WORK/gw.txt" 2); GW_THREADS_MAX=$(awk -v s="$START_MS" -v e="$END_MS" '$1 >= s && $1 <= e && $3 ~ /^[0-9]/ && $3 > m { m = $3 } END { print m + 0 }' "$WORK/gw.txt"); else GW_CPU_AVG=null; GW_CPU_MAX=null; GW_THREADS_MAX=null; fi
   ```
   `cpu_stat_file` 은 기존 `cpu_stat` 을 파일 인자를 받게 일반화한 것이다 — 기존 `cpu_stat` 정의(278행)를 이 둘로 바꾼다 (기존 호출은 그대로):
   ```bash
   cpu_stat_file() { awk -v s="$START_MS" -v e="$END_MS" -v c="$2" '$1 >= s && $1 <= e && $c ~ /^[0-9]/ { x = $c * 100; t += x; n++; if (x > m) m = x } END { if (n) printf "%.1f %.1f %d
", t / n, m, n; else print "null null 0" }' "$1"; }
   cpu_stat() { cpu_stat_file "$WORK/cpu.txt" "$1"; }
   ``` JSON 에 `"gateway":"${GATEWAY:-direct}$(gw_tag)"`, `"gw_cpu_avg":$GW_CPU_AVG`, `"gw_cpu_max":$GW_CPU_MAX`, `"gw_threads_max":$GW_THREADS_MAX` 를 더한다.

- [ ] **Step 3: 웹 측정에 연결 (`load/verification/run.sh`)**

1. 사용법 주석에 같은 한 줄 (판은 `async` 만 지원 — `GATEWAY=http` 이면서 판이 `async` 가 아니면 `die "GATEWAY=http 는 async 판만 (P8-1 묶음 E)"`).
2. `source load/lib/gateway.sh`, `gw_validate`, 포트 검사(97행 `for p in 18080 18081 "$BANK_PORT"`)는 그대로 — `gw_validate` 가 18082 를 본다.
3. 이름에 `$(gw_tag)` (회차 앞).
4. 모의 은행 기동 직후 `gw_start "$WORK/gateway.log"`, 앱 인자 `ARGS` 에 `mapfile -t GW_ARGS < <(gw_api_args); ARGS+=("${GW_ARGS[@]}")`, `finish` 에 `gw_stop`, 측정 직후 `gw_dump "$WORK/gw-threads.txt"`, 표본 · JSON 키는 Step 2 의 6 · 7 과 같다.

- [ ] **Step 4: 요약 스크립트**

`load/consumer/summarize.sh` · `load/verification/summarize.sh`: JSON 의 `gateway` 키로 묶고(없으면 `direct`), 표에 `gw CPU 평균 · 최대`, `gw 스레드 최대` 열을 더한다 — 기존 열 계산(중앙값 · 범위)과 같은 방식. `RAW_SUB=p8-1` 폴더를 읽을 때만 새 열을 보인다 (P6 · P7 표 모양은 그대로).

- [ ] **Step 5: 시험 회차 (정확성만 — 원자료로 남기지 않는다)**

Run (Docker · infra up, 개발용 API 8080 꺼짐, 먼저 `./gradlew :apps:api:bootJar :apps:api-reactive:bootJar :apps:bank-gateway:bootJar :apps:mock-bank:installDist`):
```bash
N=200 RAW_SUB=p8-1-try GATEWAY=http bash load/consumer/run.sh coroutine 10 1
N=200 RAW_SUB=p8-1-try GATEWAY=http SERVER=servlet CLIENT=jdk bash load/consumer/run.sh coroutine 10 1
docker compose -f infra/docker-compose.yml --env-file infra/.env --profile chaos up -d toxiproxy
N=200 RAW_SUB=p8-1-try GATEWAY=http NET_DELAY=5 bash load/consumer/run.sh coroutine 0 1
RAW_SUB=p8-1-try GATEWAY=http bash load/verification/run.sh async 500 50 1
bash load/consumer/run.sh cleanup && bash load/verification/run.sh cleanup
rm -rf docs/perf/raw/p8-1-try
```
Expected: 네 회차 모두 정확성 확인 통과, JSON 에 `gateway` · `gw_*` 값이 숫자, `build/load-p61/<이름>/gw-threads.txt` 가 있다 (servlet 판은 `http-nio-18082-exec-*`, WebFlux 판은 `reactor-http-nio-*` 스레드).
(`N` 이 run.sh 에서 쓰는 이름과 다르면 사용법 주석의 환경 변수 이름을 따른다.)

- [ ] **Step 6: 문서 · 커밋**

`OPERATION.md` §11(컨슈머 측정) · §12(웹 측정)에 각각 한 단락: P8-1 환경 변수, 먼저 할 일(게이트웨이 bootJar, `NET_DELAY` 면 `chaos` 프로필), 포트 18082 · 28082, 남은 toxic 확인 명령.
```bash
git add load OPERATION.md
git commit -m "chore(p8-1): 측정 도구 — GATEWAY · SERVER · CLIENT · GW_POOL · NET_DELAY, 게이트웨이 CPU · 스레드 표본 · 스레드 덤프

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: 측정 42회 · 결과 문서 (컨트롤러가 직접 — 사람의 PC 상태 확인 필요)

**Files:**
- Create: `docs/perf/raw/p8-1/*.json`
- Create: `docs/perf/p8-1-gateway-split.md`

**Interfaces:**
- Consumes: Task 7 도구

- [ ] **Step 1: 사전 확인**

개발용 API(8080) · IntelliJ 실행 구성이 꺼져 있다. `bash load/consumer/run.sh` 의 사전 확인(측정 사용자 밖 미발행 아웃박스 · 처리 대기 없음)이 통과한다. Toxiproxy 는 묶음 B' 전에만 띄운다. 회차 사이 30초 쉼 (P6 · P7 과 같다).

- [ ] **Step 2: 회차 (스펙 §8-1)**

```bash
export RAW_SUB=p8-1
# A 분리 비용 (12)
for r in 1 2 3; do
  bash load/consumer/run.sh blocking 100 $r
  GATEWAY=http bash load/consumer/run.sh blocking 100 $r
  bash load/consumer/run.sh coroutine 100 $r 64
  GATEWAY=http bash load/consumer/run.sh coroutine 100 $r 64
done
# B 순수 오버헤드 (6)
for r in 1 2 3; do bash load/consumer/run.sh coroutine 0 $r 64; GATEWAY=http bash load/consumer/run.sh coroutine 0 $r 64; done
# B' 망 지연 (6) — Toxiproxy up
for r in 1 2 3; do GATEWAY=http NET_DELAY=1 bash load/consumer/run.sh coroutine 0 $r 64; GATEWAY=http NET_DELAY=5 bash load/consumer/run.sh coroutine 0 $r 64; done
# C 판 비교 (8)
for r in 1 2; do for s in reactive servlet; do for c in webclient jdk; do GATEWAY=http SERVER=$s CLIENT=$c bash load/consumer/run.sh coroutine 10 $r 128; done; done; done
# D 연결 풀 천장 (4)
for r in 1 2; do GATEWAY=http GW_POOL=32 bash load/consumer/run.sh coroutine 100 $r 128; GATEWAY=http GW_POOL=500 bash load/consumer/run.sh coroutine 100 $r 128; done
# E 웹 1원인증 (6)
for r in 1 2 3; do bash load/verification/run.sh async 500 400 $r; GATEWAY=http bash load/verification/run.sh async 500 400 $r; done
bash load/consumer/run.sh cleanup; bash load/verification/run.sh cleanup
```
`.json.invalid` 가 생긴 회차는 원인을 로그에서 찾아 문서 "측정 중 생긴 일" 에 적고 같은 회차를 다시 돈다. 측정이 끝나면 개발용 그룹 lag 를 확인하고, 비우는 것(`reset-dev-group`)은 **사용자에게 묻고** 한다.

- [ ] **Step 3: 결과 문서**

`docs/perf/p8-1-gateway-split.md` — P7 문서(`docs/perf/p7-virtual-threads.md`)와 같은 절 순서:
1. 한 줄 결론 (묶음별 숫자 한 줄씩)
2. 무엇을 왜 재는가 (분리의 목적은 성능이 아니라 확장 · 연결 소유 — 비용이 얼마인지 잰다)
3. 그림: 직결 vs http 경로, 판별 스레드 흐름 (스펙 §5 표를 그림으로), 연결 풀 천장
4. 조건 (PC · 버전 · 회차 · 포트 · Toxiproxy 위치)
5. 결과 — 묶음 A ~ E · B' 표 (`summarize.sh` 출력)
6. 예상과 비교 (스펙 §8-3 줄마다 맞음 / 다름 + 이유)
7. 판 비교 읽는 법 — 스레드 덤프 발췌 (WebFlux `reactor-http-nio-*` · servlet `http-nio-*-exec-*` · JDK `HttpClient-*-Worker-*`), 게이트웨이 CPU · 스레드 수
8. 판단 (스펙 §8-4 기준대로 — 기본 판 유지 여부)
9. 한계 (한 PC, 루프백, Toxiproxy 는 지연만, 게이트웨이 1대)
10. 원자료 (`docs/perf/raw/p8-1/`)
11. 말로만 비교: `RestClient` + 가상 스레드 — 코드 발췌 5줄과 흐름 (P7 결과로 설명)

- [ ] **Step 4: 커밋**

```bash
git add docs/perf/p8-1-gateway-split.md docs/perf/raw/p8-1
git commit -m "docs(p8-1): 측정 42회 — 분리 비용 · 순수 오버헤드 · 망 지연 · 판 비교 · 연결 풀 천장 · 웹 1원인증

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: 결정 기록 · 문서 · 실행 구성

**Files:**
- Create: `docs/adr/0015-bank-gateway-service.md`
- Modify: `ARCHITECTURE.md` (§1 그림 · §2 모듈 표 · 의존 방향), `README.md` (로드맵 P8 하위 줄 · 기술 스택), `SETUP.md` (게이트웨이 실행), `OPERATION.md` (게이트웨이 장애 때 보는 법), `HANDOVER.md` (§1 P8-1 줄 — 상태 "진행 중"), `CHANGELOG.md` (P8-1 절)
- Create: `.run/11. Bank Gateway (8082).run.xml`, `.run/12. API (http 게이트웨이).run.xml`

- [ ] **Step 1: ADR-0015**

기존 ADR 형식(`docs/adr/0014-virtual-threads.md`)을 따른다. 상태 "제안 (2026-11-10)" — 머지 뒤 정리 PR 에서 "채택". 절:
- 맥락: api 를 늘리면 은행 연결 · 응답 매칭이 깨진다 (스펙 §1), ADR-0001 의 예고.
- 결정: 스펙 §2 표 9줄을 문장으로 — 특히 HTTP 를 고른 근거(처리량은 동시 대기 수가 정한다, P6-1 20.9 → 387건/s; 출금은 이미 Kafka 뒤), 실패 분류 원칙과 "503 은 코드가 있을 때만", 재시도 끔, 송신 취소 안 함, 기본 판.
- 버린 대안: Kafka 명령/결과 이벤트(게이트웨이 재배달 시 이중 송신 → 게이트웨이 멱등 · P3 재설계), Kafka 요청/응답, gRPC, JDK · MVC 를 기본으로, `RestClient` + 가상 스레드, OpenAPI, 게이트웨이 여러 대.
- 결과: Task 8 숫자 (한 줄 결론 · 판단), 위험(점검 설정 두 곳, 단일 장애점, 인증 없음), P8-2 · P8-3 로 넘기는 것.

- [ ] **Step 2: 구조 문서**

- `ARCHITECTURE.md` §1 mermaid 에 `apps/bank-gateway` 노드와 `api -- HTTP (mode=http) --> GWAPP --> GW` 를 더하고, 머리말 기준 줄에 P8-1 을 쓴다. §2 모듈 표에 `modules/bank-gateway-http` · `apps/bank-gateway` 두 줄, 의존 방향 블록에 두 줄.
- `README.md` 로드맵 `P8` 아래에 `- [ ] P8-1 은행 게이트웨이 서비스 분리: HTTP · 실패 분류 유지 · 서버 2판 × 클라이언트 2판 · Toxiproxy → [ADR-0015](docs/adr/0015-bank-gateway-service.md) · [측정 결과](docs/perf/p8-1-gateway-split.md)` (머지 뒤 정리 PR 에서 `[x]`). 기술 스택에 `Reactor Netty WebClient · JDK HttpClient · Toxiproxy`.
- `SETUP.md`: 게이트웨이 띄우기 — 실행 구성 `11. Bank Gateway (8082)` → `12. API (http 게이트웨이)`, 또는 `./gradlew :apps:bank-gateway:bootRun` 후 api 를 `--bank.gateway.mode=http` 로. 순서: 모의 은행 → 게이트웨이 → api.
- `OPERATION.md`: "게이트웨이 장애 때" 절 — `/actuator/health/bankConnection`, api 로그 `bank transfer not sent` (재시도 토픽으로 감) vs `result unknown` (결과조회로 감), 게이트웨이 로그 `bank call failed: ... code=`, 점검 설정 두 곳을 같게.
- `HANDOVER.md` §1 표에 `P8-1 게이트웨이 분리 | 🔄 진행 중 (PR 대기)` 줄, 머지 뒤 정리 PR 에서 완료로.
- `CHANGELOG.md` 맨 위에 `## P8-1 은행 게이트웨이 서비스 분리` — Added(게이트웨이 앱 · HTTP 모듈 · mode=http · 비교 판 · Toxiproxy · 측정 도구), Changed(처리 코드가 BankGateway 를 봄, BankTimeoutException.timeout), 문서.

- [ ] **Step 3: 실행 구성**

`.run/11. Bank Gateway (8082).run.xml` — 기존 `.run/1. Mock Bank (9090).run.xml` 이 Gradle 실행 구성이면 같은 형식으로 `:apps:bank-gateway:bootRun`, Spring Boot 실행 구성이면 메인 클래스 `io.github.hyeokjinon.banktransfer.gatewayapp.BankGatewayApplicationKt` · 모듈 `bank-transfer.apps.bank-gateway.main`. `.run/12. API (http 게이트웨이).run.xml` 은 `2. API (8080).run.xml` 을 복사해 이름과 인자 `--bank.gateway.mode=http` 만 바꾼다 (`2. API (8080).run.xml` 자체는 건드리지 않는다).

- [ ] **Step 4: 전체 빌드 · 커밋**

Run: `./gradlew build` → BUILD SUCCESSFUL
```bash
git add docs/adr/0015-bank-gateway-service.md ARCHITECTURE.md README.md SETUP.md OPERATION.md HANDOVER.md CHANGELOG.md ".run/11. Bank Gateway (8082).run.xml" ".run/12. API (http 게이트웨이).run.xml"
git commit -m "docs(p8-1): ADR-0015 은행 게이트웨이 서비스 분리, 구조 · 실행 · 운영 문서, 실행 구성

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
`git status` 로 `.run/2. API (8080).run.xml` 이 여전히 커밋되지 않은 채 남아 있는지 확인한다.

---

## 머지 전 (컨트롤러 · 사용자)

- 손 확인 (스펙 §9): 실행 구성 1 → 11 → 12 순서, Postman 1원인증 · 출금, 게이트웨이를 내린 채 출금 → 올리면 완료, health `DOWN` / `UP`, 스레드 덤프 두 판. 결과는 PR 본문 · HANDOVER 에.
- PR: `feat/p8-1-gateway-split` → `main`. 사용자가 머지한다 (자동 머지 금지).
- 머지 뒤: 정리 PR (ADR 채택 · HANDOVER 완료 · README `[x]`), job-prep 가이드 `09-p8-1-gateway-split.md` · `07` 보강, velog #14 (비공개).
