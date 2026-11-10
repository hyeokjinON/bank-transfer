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
    /**
     * 증명하는 것: 호출자가 응답 전에 끊어도 은행은 이체를 정확히 한 번 받았고, 은행 응답이 와서 게이트웨이의 대기 표가 비었다.
     * 이 보장의 근거는 GatewayEndpoints 가 은행 future 가 아니라 그 의존 future(handle)를 돌려주는 구조다 — CompletableFuture 의 취소는
     * 위로 번지지 않는다. Mono.fromFuture(.., suppressCancel = true) 는 두 번째 안전장치이며, 이 테스트가 그것의 유무를 가르지는 않는다.
     */
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
