package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * 코루틴 판 어댑터 (P6-1). 분류는 블로킹 판과 같은 classifyBankFailure 를 쓴다 — 결과가 같은지 모의 은행으로 본다.
 * 다른 것은 취소 하나: 기다리는 중 코루틴이 취소되면 분류하지 않고 취소를 다시 던진다 (설계 §4 — NotSent 로 돌려주면 다시 보내져 이중 지급).
 */
class SuspendBankTransferAdapterTest {

    private val silent = "9990000000001"
    private val reference = "T2611040000000000099"

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

    private fun transfer(client: BankGatewayClient, accountNo: String) =
        runBlocking { SuspendBankTransferAdapter(client).transfer("090", accountNo, 1_000, "m", reference) }

    @Test
    fun `연결이 없으면 NotSent`() {
        val closedPort = MockBankServer().start().use { it.boundPort }   // 닫힌 포트
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = closedPort)).start().use { client ->
            assertEquals(BankTransferOutcome.NotSent, transfer(client, "1002123456789"))
        }
    }

    @Test
    fun `등록된 계좌면 Completed`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.Completed, transfer(client, "1002123456789"))
            }
        }
    }

    @Test
    fun `등록 안 된 계좌면 Rejected 1001`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.Rejected("1001"), transfer(client, "5500000000009"))
            }
        }
    }

    @Test
    fun `은행이 응답하지 않으면(시간 초과) ResultUnknown`() {
        mockBank().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(BankTransferOutcome.ResultUnknown, transfer(client, silent))
            }
        }
    }

    @Test
    fun `기다리는 중에 코루틴이 취소되면 분류하지 않고 취소를 다시 던진다 - NotSent 로 돌려주면 다시 보내져 이중 지급`() {
        mockBank().use { bank ->
            newClient(bank, timeout = Duration.ofSeconds(5)).use { client ->
                awaitConnected(client)
                var returned: BankTransferOutcome? = null
                var thrown: Throwable? = null
                runBlocking {
                    val job = launch {
                        try {
                            returned = SuspendBankTransferAdapter(client).transfer("090", silent, 1_000, "m", reference)
                        } catch (e: Throwable) {
                            thrown = e
                            throw e
                        }
                    }
                    withTimeout(5_000) { while (client.pendingCount == 0) delay(10) }   // 요청이 은행으로 나가 응답을 기다리는 중
                    job.cancelAndJoin()
                }
                assertNull(returned)   // 어떤 결과로도 분류해 돌려주지 않았다
                assertIs<CancellationException>(thrown)
            }
        }
    }
}
