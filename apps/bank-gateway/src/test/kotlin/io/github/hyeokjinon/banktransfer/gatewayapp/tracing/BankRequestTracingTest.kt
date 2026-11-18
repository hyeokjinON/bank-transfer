package io.github.hyeokjinon.banktransfer.gatewayapp.tracing

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.micrometer.tracing.test.simple.SimpleTracer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 은행 호출 구간 (P9b 스펙 §4-3, E8 · E9): withClient 한 점 — 결과 4종 · 사유 · 전문번호 · 거래번호, future 완료 때 닫힌다. 계좌 · 금액은 없다. */
class BankRequestTracingTest {
    private val bank = MockBankServer(silentAccounts = setOf(SILENT)).start()
    private val tracer = SimpleTracer()
    private val tranNo = InMemoryTranNoSource()
    private val owner = BankConnectionOwner(tranNo, tracing = BankRequestTracing(tracer)) {
        BankGatewayClient(
            BankGatewayConfig(
                "127.0.0.1", bank.boundPort,
                responseTimeout = Duration.ofMillis(300),
                reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300),
            ),
            tranNoSource = tranNo,
        )
    }

    @AfterTest
    fun tearDown() {
        owner.close()
        bank.close()
    }

    /** 닫힌 구간만 (SimpleSpan 은 end 때 끝 시각이 생긴다). */
    private fun ended() = tracer.spans.filter { it.endTimestamp.toEpochMilli() > 0L }

    private fun lead() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()?.isConnected == true) }
    }

    @Test
    fun `대기자 - not_sent 사유 not_leader, 바로 닫힌다`() {
        runCatching { owner.transfer("090", "1002123456789", 1_000, "m", REF).get(3, TimeUnit.SECONDS) }

        val span = ended().single()
        assertEquals(BankRequestTracing.SPAN_NAME, span.name)
        assertEquals(
            mapOf("bank.request.type" to "transfer", "transfer.reference" to REF, "bank.outcome" to "not_sent", "bank.not_sent.reason" to "not_leader"),
            span.tags,
        )
    }

    @Test
    fun `리더 - 성공은 ok 와 전문번호, 없는 계좌 이체는 rejected, 성명조회에는 거래번호가 없다 - 계좌 · 금액 없음`() {
        lead()

        owner.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS)
        owner.transfer("090", "5512345678901", 1_000, "m", REF).get(3, TimeUnit.SECONDS)   // 모의 은행에 없는 계좌 → 1001

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(2, ended().size) }
        val name = ended().single { it.tags["bank.request.type"] == "name" }
        val transfer = ended().single { it.tags["bank.request.type"] == "transfer" }
        assertEquals("ok", name.tags["bank.outcome"])
        assertNotNull(name.tags["bank.tran_no"])
        assertNull(name.tags["transfer.reference"])
        assertEquals("rejected", transfer.tags["bank.outcome"])
        assertEquals(REF, transfer.tags["transfer.reference"])
        ended().forEach { s ->
            // 금액은 속성으로 가는 길 자체가 없다 — 대신 계좌번호가 어떤 속성 값에도 없는지 본다
            assertTrue(s.tags.values.none { "1002123456789" in it || "5512345678901" in it }, "계좌가 속성에: ${s.tags}")
        }
    }

    @Test
    fun `리더 - 응답이 없으면 unknown 과 예외 클래스 이름만, 메시지 · 설명은 없다`() {
        lead()

        runCatching { owner.transfer("090", SILENT, 1_000, "m", REF).get(3, TimeUnit.SECONDS) }

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1, ended().size) }
        val span = ended().first()
        assertEquals("unknown", span.tags["bank.outcome"])
        assertEquals("BankTimeoutException", span.tags["error.type"])
        assertEquals(REF, span.tags["transfer.reference"])
        // 메시지 · 상태 설명 · 계좌가 어디에도 없다 (E9)
        assertFalse(span.tags.keys.any { "message" in it || "description" in it }, "메시지 속성이 있다: ${span.tags}")
        assertTrue(span.tags.values.none { SILENT in it || "within" in it }, "예외 메시지가 속성에: ${span.tags}")
    }

    @Test
    fun `E10 - 추적이 던져도 은행 호출은 한 번, 결과 그대로`() {
        var calls = 0
        val throwing = java.lang.reflect.Proxy.newProxyInstance(javaClass.classLoader, arrayOf(io.micrometer.tracing.Tracer::class.java)) { _, _, _ ->
            calls++
            error("추적 고장")
        } as io.micrometer.tracing.Tracer
        val broken = BankConnectionOwner(tranNo, tracing = BankRequestTracing(throwing)) {
            BankGatewayClient(BankGatewayConfig("127.0.0.1", bank.boundPort, responseTimeout = Duration.ofMillis(300)), tranNoSource = tranNo)
        }
        try {
            broken.startLeading()
            await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(broken.current()?.isConnected == true) }

            val response = broken.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS)

            assertNotNull(response)
            assertTrue(calls >= 1, "고장 난 추적기가 불리지 않았다")
        } finally {
            broken.close()
        }
    }

    private companion object {
        const val REF = "T261118TRACING00001"
        const val SILENT = "9990000000001"
    }
}
