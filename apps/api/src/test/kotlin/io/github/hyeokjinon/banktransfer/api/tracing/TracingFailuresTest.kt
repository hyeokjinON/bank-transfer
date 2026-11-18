package io.github.hyeokjinon.banktransfer.api.tracing

import ch.qos.logback.classic.Level
import io.github.hyeokjinon.banktransfer.api.support.captureLogs
import io.micrometer.tracing.Tracer
import java.lang.reflect.Proxy
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** E10: 추적 호출 실패는 첫 번만 WARN (클래스 · 메시지 한 줄, 스택 없음), 이후는 DEBUG. api 의 가드 지점들이 모두 여기로 보고한다. */
class TracingFailuresTest {
    private val tracer: Tracer = Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Tracer::class.java)) { _, m, _ ->
        if (m.name == "toString") "throwing" else throw IllegalStateException("추적 고장 ${m.name}")
    } as Tracer

    @BeforeTest
    fun reset() = TracingFailures.resetForTest()

    @Test
    fun `첫 실패만 WARN 한 줄이고 이후는 WARN 이 없다 - 스택은 WARN 에 싣지 않는다`() {
        val logs = captureLogs(TracingFailures::class.java) {
            TracingFailures.report("a", IllegalStateException("one"))
            TracingFailures.report("b", IllegalStateException("two"))
            TracingFailures.report("c", IllegalStateException("three"))
        }

        val warns = logs.filter { it.level == Level.WARN }
        assertEquals(1, warns.size)
        assertTrue(warns.single().formattedMessage.contains("IllegalStateException: one"))
        assertNull(warns.single().throwableProxy)
    }

    @Test
    fun `가드 지점이 던지는 추적기로 불려도 업무는 그대로이고 WARN 은 한 번`() {
        val logs = captureLogs(TracingFailures::class.java) {
            val tags = TraceTags(tracer)
            tags.transferId(UUID.randomUUID())
            tags.verificationId(UUID.randomUUID())
            tags.verificationTagger()(UUID.randomUUID())
            assertNull(OutboxTracing(tracer, io.micrometer.tracing.propagation.Propagator.NOOP).currentTraceParent())
            var sent = 0
            OutboxTracing(tracer, io.micrometer.tracing.propagation.Propagator.NOOP).publishing(null, "t", 1L) { sent++ }
            assertEquals(1, sent)
        }

        assertEquals(1, logs.count { it.level == Level.WARN })
        assertFalse(logs.any { it.level == Level.WARN && it.formattedMessage.contains("t-") })
    }
}
