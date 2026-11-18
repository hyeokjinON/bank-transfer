package io.github.hyeokjinon.banktransfer.gatewayapp.metrics

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 은행 호출은 BankConnectionOwner 한 곳에서 센다 (P9a 스펙 §4-3) — 리더 · 연결 · 번호 · 점검 Gauge 는 Owner · 출처 · 점검 창을 그대로 읽는다. */
class BankConnectionOwnerMetricsTest {
    private val bank = MockBankServer().start()
    private val registry = SimpleMeterRegistry()
    private val tranNo = InMemoryTranNoSource()
    private val owner = BankConnectionOwner(tranNo, GatewayMetrics(registry)) {
        BankGatewayClient(
            BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
            tranNoSource = tranNo,
        )
    }
    private var now: Instant = Instant.parse("2026-11-17T03:00:00Z")   // KST 12:00
    private val clock = object : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }

    init {
        GatewayGauges(owner, tranNo, BankMaintenanceWindow(LocalTime.of(23, 50), LocalTime.of(0, 10), ZoneId.of("Asia/Seoul")), clock).bindTo(registry)
    }

    @AfterTest
    fun tearDown() {
        owner.close()
        bank.close()
    }

    private fun gauge(name: String) = registry.get(name).gauge().value()
    private fun calls(type: String, outcome: String) = registry.get(GatewayMetrics.BANK_REQUEST).tag("type", type).tag("outcome", outcome).timer().count()
    private fun notSent(reason: String) = registry.get(GatewayMetrics.NOT_SENT).tag("reason", reason).counter().count()

    @Test
    fun `대기자 - 리더 0 · 연결 0, 요청은 not_sent 사유 not_leader`() {
        assertEquals(0.0, gauge(GatewayMetrics.LEADER))
        assertEquals(0.0, gauge(GatewayMetrics.BANK_CONNECTED))

        runCatching { owner.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS) }

        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            assertEquals(1, calls("name", "not_sent"))
            assertEquals(1.0, notSent("not_leader"))
        }
    }

    @Test
    fun `리더 - 임기 1 · 리더 1 · 연결 1, 성공은 ok 이고 없는 계좌 이체는 rejected`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1.0, gauge(GatewayMetrics.BANK_CONNECTED)) }

        assertEquals(1.0, gauge(GatewayMetrics.LEADER))
        assertEquals(1.0, registry.get(GatewayMetrics.LEADER_TERMS).counter().count())
        owner.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS)
        owner.transfer("090", "5512345678901", 1_000, "m", "T261117METRICS00002").get(3, TimeUnit.SECONDS)   // 모의 은행에 없는 계좌 → 1001

        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            assertEquals(1, calls("name", "ok"))
            assertEquals(1, calls("transfer", "rejected"))
        }
    }

    @Test
    fun `정상 교대에 들어가면 리더 0, 새 요청은 not_sent 사유 stepping_down`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1.0, gauge(GatewayMetrics.BANK_CONNECTED)) }

        owner.stepDown(Duration.ofSeconds(1))
        runCatching { owner.inquire("T261117METRICS00003").get(3, TimeUnit.SECONDS) }

        assertEquals(0.0, gauge(GatewayMetrics.LEADER))
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1.0, notSent("stepping_down")) }
    }

    @Test
    fun `메모리 번호 출처면 남은 번호는 NaN, 점검 Gauge 는 KST 창을 따른다`() {
        assertTrue(gauge(GatewayMetrics.TRAN_NO_REMAINING).isNaN())
        assertEquals(0.0, gauge(GatewayMetrics.BANK_MAINTENANCE))

        now = Instant.parse("2026-11-17T14:55:00Z")   // KST 23:55

        assertEquals(1.0, gauge(GatewayMetrics.BANK_MAINTENANCE))
    }
}
