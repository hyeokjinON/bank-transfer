package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.micrometer.context.ContextRegistry
import java.util.concurrent.CompletableFuture
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

    @Test
    fun `carryContext - 부른 스레드의 ThreadLocal(관측 · 추적 문맥)을 실행기 작업에서 되살린다 (P9b 1원인증 trace)`() {
        val local = ThreadLocal<String>()
        val registry = ContextRegistry.getInstance()
        registry.registerThreadLocalAccessor("test.p9b.carry", local)
        val pool = Executors.newSingleThreadExecutor()
        try {
            local.set("request-1")
            val carried = carryContext(pool)   // 요청 스레드에서 잡는다
            local.remove()
            val seen = CompletableFuture.supplyAsync({ local.get() }, carried).get(5, TimeUnit.SECONDS)
            assertEquals("request-1", seen)
            assertEquals(null, CompletableFuture.supplyAsync({ local.get() }, pool).get(5, TimeUnit.SECONDS))   // 감싸지 않은 실행기는 모른다
        } finally {
            pool.shutdown()
            registry.removeThreadLocalAccessor("test.p9b.carry")
        }
    }
}
