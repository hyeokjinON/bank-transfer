package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.NotSentReason
import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayMetrics
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 전문번호 구간 예약 (P8-3b 스펙 §5): 게이트웨이 여러 대가 잠깐 겹쳐도 구간은 겹치지 않는다 (resourceVersion CAS). */
class ConfigMapTranNoSourceTest {
    private val k8s = MockK8s()
    private val ns = "test"
    private val name = "bank-gateway-tran-no"
    private val clock = MutableClock(Instant.parse("2026-11-16T01:00:00Z"))   // KST 2026-11-16 10:00
    private val sources = mutableListOf<ConfigMapTranNoSource>()
    private val registry = SimpleMeterRegistry()

    @AfterTest
    fun tearDown() {
        sources.forEach { it.close() }
        k8s.close()
    }

    private fun source(blockSize: Long = 1000, prefetch: Long = 100, attempts: Int = 5) =
        ConfigMapTranNoSource(k8s.client, ns, name, clock, blockSize, prefetch, attempts, GatewayMetrics(registry)).also { sources += it }

    private fun data() = k8s.client.configMaps().inNamespace(ns).withName(name).get().data

    @Test
    fun `처음 예약자가 ConfigMap 을 만들고 1부터 쓴다 - 날짜는 KST`() {
        val source = source()
        source.prepare()

        assertEquals(1, source.next())
        assertEquals(2, source.next())
        assertEquals(mapOf("date" to "20261116", "reservedTo" to "1000"), data())
    }

    @Test
    fun `두 예약자가 동시에 예약해도 구간이 겹치지 않는다`() {
        val a = source(attempts = 100)
        val b = source(attempts = 100)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val ranges = listOf(a, b).map { s -> pool.submit(Callable { (1..25).map { s.reserve("20261116") } }) }.flatMap { it.get() }
            val numbers = ranges.flatMap { it.toList() }
            assertEquals(50 * 1000, numbers.size)
            assertEquals(numbers.size, numbers.toSet().size)            // 겹침 없음
            assertEquals((1L..50_000L).toList(), numbers.sorted())       // 빈틈도 없음 (예약은 연속)
            assertEquals("50000", data()["reservedTo"])
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `다른 게이트웨이가 쓰던 날짜면 그 뒤부터 예약한다`() {
        k8s.client.configMaps().inNamespace(ns).resource(
            ConfigMapBuilder().withNewMetadata().withName(name).endMetadata().addToData("date", "20261116").addToData("reservedTo", "7000").build(),
        ).create()
        val source = source()
        source.prepare()

        assertEquals(7001, source.next())
        assertEquals("8000", data()["reservedTo"])
    }

    @Test
    fun `구간 끝 가까이에서 다음 구간을 미리 예약하고 이어서 쓴다`() {
        val source = source(blockSize = 10, prefetch = 3)
        source.prepare()

        val firstEight = (1..8).map { source.next() }
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals("20", data()["reservedTo"]) }
        val nextThree = (1..3).map { source.next() }

        assertEquals((1L..8L).toList(), firstEight)
        assertEquals(listOf(9L, 10L, 11L), nextThree)
    }

    @Test
    fun `날짜가 바뀌면 보내지 않고 새 날짜 구간을 예약해 1부터`() {
        val source = source(blockSize = 10)
        source.prepare()
        assertEquals(1, source.next())

        clock.now = Instant.parse("2026-11-16T15:00:00Z")   // KST 2026-11-17 00:00 (점검 시간 안 — 송신은 원래 막혀 있다)
        assertEquals(NotSentReason.NO_TRAN_NO, assertFailsWith<BankNotSentException> { source.next() }.reason)

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertEquals(1, runCatching { source.next() }.getOrNull()) }
        assertEquals(mapOf("date" to "20261117", "reservedTo" to "10"), data())
    }

    @Test
    fun `구간이 바닥나고 다음 예약이 안 되면 보내지 않는다 - API 서버 단절`() {
        val source = source(blockSize = 2, prefetch = 0)
        source.prepare()
        k8s.stopApiServer()

        assertEquals(1, source.next())
        assertEquals(2, source.next())
        assertEquals(NotSentReason.NO_TRAN_NO, assertFailsWith<BankNotSentException> { source.next() }.reason)
    }

    @Test
    fun `임기 시작 예약이 실패해도 던지지 않는다 - next 가 보내지 않음으로 답한다`() {
        val source = source()
        k8s.stopApiServer()

        source.prepare()

        assertEquals(NotSentReason.NO_TRAN_NO, assertFailsWith<BankNotSentException> { source.next() }.reason)
    }

    @Test
    fun `하루 한도(99,999,999)를 넘는 구간은 예약하지 않는다`() {
        k8s.client.configMaps().inNamespace(ns).resource(
            ConfigMapBuilder().withNewMetadata().withName(name).endMetadata()
                .addToData("date", "20261116").addToData("reservedTo", "${Header.MAX_TRAN_NO - 5}").build(),
        ).create()

        assertEquals(NotSentReason.TRAN_NO_STORE, assertFailsWith<BankNotSentException> { source(blockSize = 10).reserve("20261116") }.reason)
        assertTrue(data()["reservedTo"] == "${Header.MAX_TRAN_NO - 5}")
    }

    private fun store(date: String?, reservedTo: String?) {
        val b = ConfigMapBuilder().withNewMetadata().withName(name).endMetadata()
        date?.let { b.addToData("date", it) }
        reservedTo?.let { b.addToData("reservedTo", it) }
        k8s.client.configMaps().inNamespace(ns).resource(b.build()).create()
    }

    @Test
    fun `저장된 날짜가 더 새로우면 예약하지 않고 기록도 그대로 둔다 - 시계가 늦은 예약자`() {
        store("20261117", "1000")

        assertEquals(NotSentReason.TRAN_NO_STORE, assertFailsWith<BankNotSentException> { source().reserve("20261116") }.reason)
        assertEquals(mapOf("date" to "20261117", "reservedTo" to "1000"), data())
    }

    @Test
    fun `깨진 기록은 덮지 않고 보내지 않는다`() {
        store("20261116", "abc")
        assertEquals(NotSentReason.TRAN_NO_STORE, assertFailsWith<BankNotSentException> { source().reserve("20261116") }.reason)
        assertEquals(mapOf("date" to "20261116", "reservedTo" to "abc"), data())

        k8s.client.configMaps().inNamespace(ns).withName(name).delete()
        store("20261116", null)
        assertEquals(NotSentReason.TRAN_NO_STORE, assertFailsWith<BankNotSentException> { source().reserve("20261116") }.reason)
        assertEquals(mapOf("date" to "20261116"), data())
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }

    private fun reserves(result: String) = registry.get(GatewayMetrics.TRAN_NO_RESERVE).tag("result", result).counter().count()

    @Test
    fun `예약 결과와 남은 번호 - 현재 구간 + 미리 예약 구간`() {
        val source = source(blockSize = 10, prefetch = 0)
        source.prepare()
        assertEquals(1.0, reserves("ok"))
        assertEquals(10, source.remaining())

        source.next()
        assertEquals(9, source.remaining())
    }

    @Test
    fun `API 서버가 없으면 예약은 error 로 센다`() {
        k8s.stopApiServer()
        val source = source()
        source.prepare()   // 던지지 않는다 — 실패를 센다

        assertEquals(1.0, reserves("error"))
        assertEquals(0, source.remaining())
    }
}
