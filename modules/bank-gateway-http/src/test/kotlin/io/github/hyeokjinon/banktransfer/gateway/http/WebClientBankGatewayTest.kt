package io.github.hyeokjinon.banktransfer.gateway.http

import com.sun.net.httpserver.HttpServer
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import java.net.InetSocketAddress
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class WebClientBankGatewayTest : HttpBankGatewayClientContract() {
    override fun newGateway(settings: HttpGatewaySettings): HttpBankGateway = WebClientBankGateway(settings)

    /**
     * WebClient 판에만 있는 실패 — 연결 풀 대기열 상한(Reactor Netty 기본 maxConnections × 2).
     * 풀 1 · 동시 4건이면 1건 사용 · 2건 대기 · 1건은 대기열 초과로 바로 거절된다. 거절된 건은 게이트웨이에 쓰지도 않았으므로
     * "안 보냄" 이어야 한다 (P8-1 측정 D 에서 "모름" 으로 잘못 분류돼 324건이 조회 경로로 빠졌던 것).
     */
    @Test
    fun `연결 풀 대기열이 꽉 차 거절된 요청은 안 보냄이고 게이트웨이에 닿지 않는다`() {
        val requests = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext(GatewayPaths.TRANSFERS) { ex ->
                requests.incrementAndGet()
                ex.requestBody.readAllBytes()
                Thread.sleep(300)
                val bytes = OK.toByteArray()
                ex.responseHeaders.add("Content-Type", "application/json")
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            executor = Executors.newVirtualThreadPerTaskExecutor()
            start()
        }
        try {
            WebClientBankGateway(
                HttpGatewaySettings(
                    baseUrl = "http://127.0.0.1:${server.address.port}",
                    maxConnections = 1,
                    pendingAcquireTimeout = Duration.ofSeconds(5),
                    responseTimeout = Duration.ofSeconds(5),
                ),
            ).use { gw ->
                val futures = (1..4).map { gw.transfer("090", "1002123456789", 1_000, "M", "T261110POOLLIMIT00000$it") }
                val failures = futures.mapNotNull { f ->
                    runCatching { f.get(10, TimeUnit.SECONDS) }.exceptionOrNull()?.let { (it as ExecutionException).cause }
                }
                assertTrue(failures.isNotEmpty(), "대기열 상한(2)을 넘은 요청이 없었다 — 기본값이 바뀌었는지 확인")
                failures.forEach { assertIs<BankNotSentException>(it) }
                assertEquals(4 - failures.size, requests.get())   // 거절된 건은 서버에 닿지 않았다
            }
        } finally {
            server.stop(0)
        }
    }

    private companion object {
        val OK: String = GatewayJson.write(
            TransferResult(HeaderDto(1, LocalDateTime.of(2026, 11, 10, 9, 0), "0000"), "090", "1002123456789", 1_000, "M", "T261110POOLLIMIT000001"),
        )
    }
}
