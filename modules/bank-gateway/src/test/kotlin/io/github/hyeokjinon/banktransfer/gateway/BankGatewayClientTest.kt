package io.github.hyeokjinon.banktransfer.gateway

import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class BankGatewayClientTest {
    private val resources = mutableListOf<AutoCloseable>()

    @AfterTest
    fun tearDown() = resources.asReversed().forEach { it.close() }

    private fun <T : AutoCloseable> T.closeLater(): T = also { resources += it }

    private fun client(port: Int, timeout: Duration = Duration.ofSeconds(2), poll: Duration = Duration.ofSeconds(30)) =
        BankGatewayClient(
            BankGatewayConfig(
                host = "127.0.0.1",
                port = port,
                responseTimeout = timeout,
                pollInterval = poll,
                reconnectMinDelay = Duration.ofMillis(100),
                reconnectMaxDelay = Duration.ofMillis(500),
            ),
        ).closeLater()

    private fun waitUntil(timeoutMs: Long = 5_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!condition()) {
            check(System.currentTimeMillis() < deadline) { "condition not met within $timeoutMs ms" }
            Thread.sleep(20)
        }
    }

    private fun <T> CompletableFuture<T>.awaitFailure(): Throwable =
        runCatching { get(5, TimeUnit.SECONDS) }.exceptionOrNull().let { (it as? ExecutionException)?.cause ?: it!! }

    @Test
    fun `성명조회 요청에 같은 전문번호로 온 응답이 매칭된다`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        val res = client.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS)

        assertEquals(ResponseCodes.OK, res.header.responseCode)
        assertEquals("홍길동", res.holderName)
        assertEquals(0, client.pendingCount)
    }

    @Test
    fun `동시에 여러 요청을 보내도 각자 자기 응답을 받는다`() {
        val bank = MockBankServer(responseDelay = Duration.ofMillis(50)).start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        val futures = (1..50).map { i -> client.transfer("090", "1002123456789", i.toLong(), "T$i", "R$i") }
        val responses = futures.map { it.get(5, TimeUnit.SECONDS) }

        responses.forEachIndexed { i, res -> assertEquals("T${i + 1}", res.memo) }
        assertEquals(50, bank.depositsOf("1002123456789").size)
    }

    @Test
    fun `은행이 응답하지 않으면 BankTimeoutException`() {
        val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start().closeLater()
        val client = client(bank.boundPort, timeout = Duration.ofMillis(300)).start()
        waitUntil { client.isConnected }

        val error = client.inquireName("090", "9990000000001").awaitFailure()

        assertIs<BankTimeoutException>(error)
        assertEquals(0, client.pendingCount, "타임아웃 난 요청은 대기 목록에서 지워져야 한다")
    }

    @Test
    fun `연결이 없으면 기다리지 않고 바로 BankUnavailableException`() {
        val client = client(freePort())

        val failure = client.inquireName("090", "1").awaitFailure()
        assertIs<BankUnavailableException>(failure)
        assertIs<BankNotSentException>(failure)   // 은행에 가지 않았다 → 다시 보내도 안전
    }

    @Test
    fun `보낸 뒤 연결이 끊기면 BankNotSentException 이 아니다 (은행이 받았을 수 있다)`() {
        val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
        val client = client(bank.boundPort, timeout = Duration.ofSeconds(5)).start()
        waitUntil { client.isConnected }

        val pending = client.transfer("090", "9990000000001", 1, "x", "R1")
        bank.close()

        val failure = pending.awaitFailure()
        assertIs<BankUnavailableException>(failure)
        assertFalse(failure is BankNotSentException)
    }

    @Test
    fun `은행이 늦게 떠도 백오프로 재접속한다`() {
        val port = freePort()
        val client = client(port).start()
        Thread.sleep(300) // 몇 번 실패하게 둔다

        MockBankServer(port).start().closeLater()

        waitUntil { client.isConnected }
    }

    @Test
    fun `보낼 게 없으면 폴링 전문으로 회선을 확인한다`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort, poll = Duration.ofMillis(100)).start()
        waitUntil { client.isConnected }

        waitUntil { bank.pollCount >= 2 }
        assertTrue(client.isConnected)
    }

    @Test
    fun `결과조회 - 처리한 거래번호는 0000, 모르는 번호는 2001`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        client.transfer("090", "1002123456789", 1_000, "m", "T2610210000000000001").get(5, TimeUnit.SECONDS)

        assertEquals(ResponseCodes.OK, client.inquire("T2610210000000000001").get(5, TimeUnit.SECONDS).result)
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T2610219999999999999").get(5, TimeUnit.SECONDS).result)
    }

    @Test
    fun `처리하고 응답만 안 한 은행 - 이체는 시간 초과, 결과조회는 처리됨`() {
        val bank = MockBankServer().start().closeLater()   // 9990000000002 = 처리 후 무응답 (기본값)
        val client = client(bank.boundPort, timeout = Duration.ofMillis(300)).start()
        waitUntil { client.isConnected }

        assertIs<BankTimeoutException>(client.transfer("090", "9990000000002", 1_000, "m", "T2610210000000000002").awaitFailure())

        assertTrue(bank.hasProcessed("T2610210000000000002"))
        assertEquals(ResponseCodes.OK, client.inquire("T2610210000000000002").get(5, TimeUnit.SECONDS).result)
    }

    @Test
    fun `처리 후 무응답 계좌도 1원 이체에는 응답한다 (1원인증으로 수취 계좌 등록)`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }

        assertEquals(ResponseCodes.OK, client.transfer("090", "9990000000002", 1, "BT1234", "V2610210000000000001").get(5, TimeUnit.SECONDS).header.responseCode)
    }

    @Test
    fun `모의 은행을 잠시 내렸다 올리면 같은 포트로 다시 붙는다 (은행 장애 시나리오용)`() {
        val bank = MockBankServer().start().closeLater()
        val client = client(bank.boundPort).start()
        waitUntil { client.isConnected }
        val port = bank.boundPort

        bank.pause()
        waitUntil { !client.isConnected }   // 붙어 있던 연결도 끊긴다
        assertIs<BankNotSentException>(client.inquire("T2610220000000000001").awaitFailure())   // 은행에 가지 않았다

        bank.resume()
        waitUntil { client.isConnected }
        assertEquals(port, bank.boundPort)
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T2610220000000000001").get(5, TimeUnit.SECONDS).result)
    }

    @Test
    fun `보내기 문이 닫혀 있으면(점검 시간) 연결이 있어도 쓰지 않고 BankNotSentException - 폴링은 계속된다`() {
        val bank = MockBankServer().start().closeLater()
        var closed = true
        val client = BankGatewayClient(
            BankGatewayConfig(host = "127.0.0.1", port = bank.boundPort, pollInterval = Duration.ofMillis(200)),
            sendGate = { if (closed) "bank maintenance window" else null },
        ).closeLater().start()
        waitUntil { client.isConnected }

        val error = client.transfer("090", "1002123456789", 1_000, "점검", "T261028MAINTENANCE01").awaitFailure()

        assertIs<BankNotSentException>(error)
        assertEquals("bank maintenance window", error.message)
        assertFalse(bank.hasProcessed("T261028MAINTENANCE01"))
        val polls = bank.pollCount
        waitUntil { bank.pollCount > polls }   // 링크 체크는 막지 않는다 — 점검이 끝나면 바로 보낼 수 있다
        closed = false
        assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, client.inquire("T261028MAINTENANCE01").get(3, TimeUnit.SECONDS).result)
    }

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }
}
