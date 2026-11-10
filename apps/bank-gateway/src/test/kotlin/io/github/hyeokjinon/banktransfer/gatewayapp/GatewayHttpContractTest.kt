package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayErrorCodes
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayJson
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import io.github.hyeokjinon.banktransfer.gateway.http.TransferResult
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.springframework.boot.WebApplicationType
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * 스펙 §4-2 의 HTTP 표를 클라이언트 없이 원본 요청으로 확인한다 — 상태 코드와 오류 본문 code 가 계약 그대로인지.
 * (GatewayContractTest 는 클라이언트가 만든 예외 종류만 본다.)
 */
class GatewayHttpContractTest {
    companion object {
        const val OK_ACCOUNT = "1002123456789"
        const val SILENT = "9990000000001"
        private val bank = MockBankServer(silentAccounts = setOf(SILENT)).start()
        private val app = GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort)
        private val http = HttpClient.newHttpClient()

        @JvmStatic
        @AfterAll
        fun stopAll() { app.close(); bank.close() }
    }

    private val closeables = mutableListOf<AutoCloseable>()

    @AfterEach
    fun tearDown() { closeables.asReversed().forEach { it.close() }; bank.responseDelay = Duration.ZERO }

    private fun post(port: Int, json: String): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI("http://127.0.0.1:$port${GatewayPaths.TRANSFERS}"))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun transferJson(account: String) =
        """{"bankCode":"090","accountNo":"$account","amount":1000,"memo":"H","reference":"T261110${UUID.randomUUID().toString().replace("-", "").take(13).uppercase()}"}"""

    private fun awaitConnected(ctxBank: BankGatewayClient) = await.atMost(Duration.ofSeconds(10)) untilAsserted { assert(ctxBank.isConnected) }

    @Test
    fun `200 - 은행 응답 본문 그대로, 거절 1001 도 200`() {
        awaitConnected(app.getBean(BankGatewayClient::class.java))
        val ok = post(app.port(), transferJson(OK_ACCOUNT))
        assertEquals(200, ok.statusCode())
        assertEquals("0000", GatewayJson.read(ok.body(), TransferResult::class.java).header.responseCode)
        val rejected = post(app.port(), transferJson("5500000000001"))
        assertEquals(200, rejected.statusCode())
        assertEquals("1001", GatewayJson.read(rejected.body(), TransferResult::class.java).header.responseCode)
    }

    @Test
    fun `503 BANK_NOT_SENT - 은행 연결이 없다`() {
        val isolated = MockBankServer().start().also { closeables += it }
        val ctx = GatewayApps.start(WebApplicationType.REACTIVE, isolated.boundPort).also { closeables += it }
        val gw = ctx.getBean(BankGatewayClient::class.java)
        awaitConnected(gw)
        isolated.pause()
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assert(!gw.isConnected) }
        val res = post(ctx.port(), transferJson(OK_ACCOUNT))
        assertEquals(503, res.statusCode())
        assertEquals(GatewayErrorCodes.BANK_NOT_SENT, GatewayJson.errorOrNull(res.body())!!.code)
    }

    @Test
    fun `504 BANK_TIMEOUT - tranNo 와 timeoutMs 를 싣는다`() {
        awaitConnected(app.getBean(BankGatewayClient::class.java))
        val res = post(app.port(), transferJson(SILENT))
        assertEquals(504, res.statusCode())
        val error = GatewayJson.errorOrNull(res.body())!!
        assertEquals(GatewayErrorCodes.BANK_TIMEOUT, error.code)
        assertNotNull(error.tranNo)
        assertEquals(500L, error.timeoutMs)
    }

    @Test
    fun `502 BANK_UNAVAILABLE - 보낸 뒤 응답 전에 은행 회선이 끊겼다`() {
        val isolated = MockBankServer().start().also { closeables += it }
        val ctx = GatewayApps.start(WebApplicationType.REACTIVE, isolated.boundPort, "--bank.gateway.response-timeout=3s").also { closeables += it }
        awaitConnected(ctx.getBean(BankGatewayClient::class.java))
        isolated.responseDelay = Duration.ofMillis(300)
        val pauser = Thread { Thread.sleep(100); isolated.pause() }.also { it.start() }
        val res = post(ctx.port(), transferJson(OK_ACCOUNT))
        pauser.join()
        assertEquals(502, res.statusCode())
        assertEquals(GatewayErrorCodes.BANK_UNAVAILABLE, GatewayJson.errorOrNull(res.body())!!.code)
    }

    @Test
    fun `400 INVALID_REQUEST - 읽을 수 없는 본문`() {
        val res = post(app.port(), "{not json")
        assertEquals(400, res.statusCode())
        assertEquals(GatewayErrorCodes.INVALID_REQUEST, GatewayJson.errorOrNull(res.body())!!.code)
    }
}
