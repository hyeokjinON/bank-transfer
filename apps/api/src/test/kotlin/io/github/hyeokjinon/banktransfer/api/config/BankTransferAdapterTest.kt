package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

/** 예외를 "안 보냄 / 모름 / 거절 / 완료" 로 가르는 분류를 모의 은행으로 확인한다 (프로세서가 이 분류에 기대어 재전송 여부를 정한다). */
class BankTransferAdapterTest {

    private val silent = "9990000000001"

    private fun newClient(bank: MockBankServer, timeout: Duration = Duration.ofMillis(500)) =
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = timeout)).start()

    private fun awaitConnected(client: BankGatewayClient) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!client.isConnected) {
            check(System.currentTimeMillis() < deadline) { "not connected" }
            Thread.sleep(20)
        }
    }

    private fun mockBank() = MockBankServer(silentAccounts = setOf(silent)).start()

    @Test
    fun `연결이 없으면 NotSent`() {
        val closedPort = MockBankServer().start().use { it.boundPort }   // 닫힌 포트
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = closedPort)).start().use { client ->
            assertEquals(BankTransferOutcome.NotSent, BankTransferAdapter(client).transfer("090", "1002123456789", 1_000, "m"))
        }
    }

    @Test
    fun `은행이 응답하지 않으면 ResultUnknown`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.ResultUnknown, BankTransferAdapter(client).transfer("090", silent, 1_000, "m"))
            }
        }
    }

    @Test
    fun `등록된 계좌면 Completed`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.Completed, BankTransferAdapter(client).transfer("090", "1002123456789", 1_000, "m"))
            }
        }
    }

    @Test
    fun `등록 안 된 계좌면 Rejected 1001`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.Rejected("1001"), BankTransferAdapter(client).transfer("090", "5500000000009", 1_000, "m"))
            }
        }
    }

    @Test
    fun `응답을 기다리는 중에 연결이 끊기면 ResultUnknown`() {
        val bank = mockBank()
        val pool = Executors.newSingleThreadExecutor()
        try {
            newClient(bank, timeout = Duration.ofSeconds(5)).use { client ->
                awaitConnected(client)
                val outcome = pool.submit<BankTransferOutcome> { BankTransferAdapter(client).transfer("090", silent, 1_000, "m") }
                // 요청이 은행으로 나가 응답을 기다리는 상태가 될 때까지 기다린 뒤 은행을 닫는다
                val deadline = System.currentTimeMillis() + 5_000
                while (client.pendingCount == 0) {
                    check(System.currentTimeMillis() < deadline) { "request was not sent" }
                    Thread.sleep(10)
                }
                bank.close()

                assertEquals(BankTransferOutcome.ResultUnknown, outcome.get(5, TimeUnit.SECONDS))
            }
        } finally {
            pool.shutdownNow()
            bank.close()
        }
    }
}
