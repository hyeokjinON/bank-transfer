package io.github.hyeokjinon.banktransfer.gateway.http

import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.gateway.BankUnavailableException
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.TcpCutProxy
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.transport.SenderContext
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
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
    @Volatile private var lastTraceparent: String? = null

    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext(GatewayPaths.TRANSFERS) { ex ->
            requests.incrementAndGet()
            lastTraceparent = ex.requestHeaders.getFirst("traceparent")
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

    protected abstract fun newGateway(settings: HttpGatewaySettings, observations: ObservationRegistry): HttpBankGateway

    private fun gateway(
        baseUrl: String = "http://127.0.0.1:${server.address.port}",
        responseTimeout: Duration = Duration.ofSeconds(2),
        observations: ObservationRegistry = ObservationRegistry.NOOP,
    ) = newGateway(HttpGatewaySettings(baseUrl = baseUrl, responseTimeout = responseTimeout), observations).also { closeables += it }

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

    @Test
    fun `관측 레지스트리를 주면 보내는 쪽 문맥으로 요청 헤더를 채울 수 있고, 응답 뒤 관측이 끝난다 (P9b 전파 자리)`() {
        val handler = HeaderInjectingHandler()
        val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }

        val res = gateway(observations = registry).transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS)

        assertEquals("0000", res.header.responseCode)
        assertEquals(TRACEPARENT, lastTraceparent)   // 추적 처리기(PropagatingSenderTracingObservationHandler)가 하는 일과 같은 자리
        await.atMost(Duration.ofSeconds(2)) untilAsserted { assertEquals(1, handler.stopped.get()) }
    }

    /** 운영에서는 Micrometer Tracing 의 보내는 쪽 처리기가 setter 로 traceparent 를 넣는다 — 모듈은 추적 라이브러리를 모르므로 같은 자리를 흉내 낸다. */
    private class HeaderInjectingHandler : ObservationHandler<SenderContext<*>> {
        val stopped = AtomicInteger()

        @Suppress("UNCHECKED_CAST")
        override fun onStart(context: SenderContext<*>) {
            val ctx = context as SenderContext<Any>
            ctx.setter.set(ctx.carrier, "traceparent", TRACEPARENT)
        }

        override fun onStop(context: SenderContext<*>) {
            stopped.incrementAndGet()
        }

        override fun supportsContext(context: Observation.Context): Boolean = context is SenderContext<*>
    }

    private companion object {
        const val TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        const val REF = "T261110WCTEST0000001"
        fun ok() = GatewayJson.write(
            TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", REF),
        )
    }
}
