package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.InquiryOutcome
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

/** 결과조회 응답을 "처리됨 / 원거래 없음 / 모름" 으로 가르는 분류를 모의 은행으로 확인한다. */
class BankInquiryAdapterTest {

    private fun newClient(bank: MockBankServer, timeout: Duration = Duration.ofMillis(500)) =
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = timeout)).start()

    private fun awaitConnected(client: BankGatewayClient) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!client.isConnected) {
            check(System.currentTimeMillis() < deadline) { "not connected" }
            Thread.sleep(20)
        }
    }

    @Test
    fun `처리한 거래번호는 Processed`() {
        MockBankServer().start().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                client.transfer("090", "1002123456789", 1_000, "m", "T2610210000000000011").get(5, TimeUnit.SECONDS)

                assertEquals(InquiryOutcome.Processed, BankInquiryAdapter(client).inquire("T2610210000000000011"))
            }
        }
    }

    @Test
    fun `모르는 거래번호는 NotProcessed 2001`() {
        MockBankServer().start().use { bank ->
            newClient(bank).use { client ->
                awaitConnected(client)
                assertEquals(InquiryOutcome.NotProcessed("2001"), BankInquiryAdapter(client).inquire("T2610219999999999998"))
            }
        }
    }

    @Test
    fun `연결이 없으면 Unknown (예외를 던지지 않는다)`() {
        val closedPort = MockBankServer().start().use { it.boundPort }
        BankGatewayClient(BankGatewayConfig(host = "localhost", port = closedPort)).start().use { client ->
            assertEquals(InquiryOutcome.Unknown, BankInquiryAdapter(client).inquire("T2610210000000000012"))
        }
    }
}
