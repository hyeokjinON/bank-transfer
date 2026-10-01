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

        val futures = (1..50).map { i -> client.transfer("090", "1002123456789", i.toLong(), "T$i") }
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

        assertIs<BankUnavailableException>(client.inquireName("090", "1").awaitFailure())
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

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }
}
