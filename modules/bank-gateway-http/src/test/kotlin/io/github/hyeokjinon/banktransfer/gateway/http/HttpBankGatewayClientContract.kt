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
import kotlin.test.assertFalse
import kotlin.test.assertIs

/** 두 클라이언트 판(WebClient · JDK)이 같은 표를 지키는지 — 판마다 newGateway 만 다르다 (P8-1). */
abstract class HttpBankGatewayClientContract {
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

    protected abstract fun newGateway(settings: HttpGatewaySettings): HttpBankGateway

    private fun gateway(baseUrl: String = "http://127.0.0.1:${server.address.port}", responseTimeout: Duration = Duration.ofSeconds(2)) =
        newGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = responseTimeout)).also { closeables += it }

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
        assertFalse(e is BankNotSentException)
    }

    @Test
    fun `받고 응답 전에 끊기면 모름이고, 다시 보내지 않는다 (WebClient 는 disableRetry, JDK 는 POST 재시도 기본 꺼짐)`() {
        val proxy = TcpCutProxy(server.address.port).start().also { closeables += it }
        proxy.mode = TcpCutProxy.Mode.DROP_RESPONSE
        val e = transferFailure(gateway(baseUrl = proxy.baseUrl))
        assertIs<BankUnavailableException>(e)
        assertFalse(e is BankNotSentException)
        Thread.sleep(300)
        // 자동 재시도 없음. 주의: 이 테스트는 disableRetry(true) 만 따로 가르지 못한다 — FIN 종료는 PrematureCloseException 이고
        // Reactor Netty 1.3.7 은 헤더를 보낸 뒤에는 스스로 재시도하지 않는다. 라이브러리 동작 + 설정을 함께 고정한다.
        assertEquals(1, requests.get())
    }

    private companion object {
        const val REF = "T261110WCTEST0000001"
        fun ok() = GatewayJson.write(
            TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", REF),
        )
    }
}
