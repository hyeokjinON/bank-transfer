package io.github.hyeokjinon.banktransfer.gateway.http

import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.LocalDateTime
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** E10: 추적 처리기가 던져도 JDK 판의 이체 결과 · 오류 분류는 그대로다. */
class JdkTracingFailureTest {
    private val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext(GatewayPaths.TRANSFERS) { ex ->
            ex.requestBody.readAllBytes()
            val bytes = GatewayJson.write(
                TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", REF),
            ).toByteArray()
            ex.sendResponseHeaders(200, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }
        start()
    }

    @AfterTest
    fun stop() = server.stop(0)

    private fun registry(failOn: Set<String>) = ObservationRegistry.create().apply {
        observationConfig().observationHandler(object : ObservationHandler<Observation.Context> {
            override fun supportsContext(context: Observation.Context) = true
            override fun onStart(context: Observation.Context) { if ("start" in failOn) error("boom start") }
            override fun onError(context: Observation.Context) { if ("error" in failOn) error("boom error") }
            override fun onStop(context: Observation.Context) { if ("stop" in failOn) error("boom stop") }
        })
    }

    @Test
    fun `시작 · 정지 처리기가 던져도 이체 응답은 그대로 돌아온다`() {
        for (failOn in listOf(setOf("start"), setOf("stop"), setOf("start", "stop"))) {
            JdkHttpBankGateway(HttpGatewaySettings(baseUrl = "http://127.0.0.1:${server.address.port}"), registry(failOn)).use { gw ->
                val res = gw.transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS)
                assertEquals("0000", res.header.responseCode, "$failOn")
                assertEquals(REF, res.reference)
            }
        }
    }

    @Test
    fun `연결 거부는 처리기가 던져도 똑같이 안 보냄이다`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        JdkHttpBankGateway(HttpGatewaySettings(baseUrl = "http://127.0.0.1:$closedPort"), registry(setOf("start", "error", "stop"))).use { gw ->
            val e = assertIs<ExecutionException>(runCatching { gw.transfer("090", "1002123456789", 1_000, "M", REF).get(5, TimeUnit.SECONDS) }.exceptionOrNull())
            assertIs<BankNotSentException>(e.cause)
        }
    }

    private companion object {
        const val REF = "T261110JDKFAIL0000001"
    }
}
