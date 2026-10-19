package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class BankGatewayAdapterTest {

    @Test
    fun `은행 응답 뒤 처리는 Netty 이벤트 루프가 아니라 앱 실행기로 넘긴다`() {
        MockBankServer().start().use { bank ->
            BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort)).start().use { client ->
                val deadline = System.currentTimeMillis() + 5_000
                while (!client.isConnected) {
                    check(System.currentTimeMillis() < deadline) { "not connected" }
                    Thread.sleep(20)
                }
                val pool = Executors.newSingleThreadExecutor()
                val dispatched = AtomicInteger()
                val executor = Executor { task -> dispatched.incrementAndGet(); pool.execute(task) }

                val name = BankGatewayAdapter(client, executor).inquireHolderName("090", "1002123456789").get(5, TimeUnit.SECONDS)

                assertEquals("홍길동", name)
                assertEquals(1, dispatched.get())
                pool.shutdown()
            }
        }
    }
}
