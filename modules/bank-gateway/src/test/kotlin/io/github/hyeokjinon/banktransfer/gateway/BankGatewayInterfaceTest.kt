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
