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

    @Test
    fun `carryContext - 문맥을 잡다가 던져도 실행기를 그대로 돌려주고 작업은 한 번 돈다 (E10)`() {
        val registry = ContextRegistry.getInstance()
        registry.registerThreadLocalAccessor(brokenAccessor("test.p9b.broken-capture", failOnGet = true, failOnSet = false))
        val pool = Executors.newSingleThreadExecutor()
        try {
            val carried = carryContext(pool)
            val runs = AtomicInteger()
            CompletableFuture.supplyAsync({ runs.incrementAndGet() }, carried).get(5, TimeUnit.SECONDS)
            assertEquals(1, runs.get())
        } finally {
            pool.shutdown()
            registry.removeThreadLocalAccessor("test.p9b.broken-capture")
        }
    }

    @Test
    fun `carryContext - 작업 앞 복원이 던져도 작업은 추적 없이 정확히 한 번 돌고 결과는 그대로다 (E10)`() {
        val registry = ContextRegistry.getInstance()
        val local = ThreadLocal<String>()
        registry.registerThreadLocalAccessor("test.p9b.ok", local)
        registry.registerThreadLocalAccessor(brokenAccessor("test.p9b.broken-restore", failOnGet = false, failOnSet = true))
        val pool = Executors.newSingleThreadExecutor()
        try {
            local.set("x")
            val carried = carryContext(pool)
            local.remove()
            val runs = AtomicInteger()
            val result = CompletableFuture.supplyAsync({ runs.incrementAndGet(); "ok" }, carried).get(5, TimeUnit.SECONDS)
            assertEquals("ok", result)
            assertEquals(1, runs.get())
        } finally {
            pool.shutdown()
            registry.removeThreadLocalAccessor("test.p9b.broken-restore")
            registry.removeThreadLocalAccessor("test.p9b.ok")
        }
    }

    private fun brokenAccessor(key: String, failOnGet: Boolean, failOnSet: Boolean) = object : io.micrometer.context.ThreadLocalAccessor<String> {
        override fun key(): Any = key
        override fun getValue(): String? = if (failOnGet) error("capture boom") else "v"
        override fun setValue(value: String) { if (failOnSet) error("restore boom") }
        override fun setValue() {}
        override fun reset() {}
    }
}
