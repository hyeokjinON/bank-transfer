package io.github.hyeokjinon.banktransfer.api.tracing

import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer
import java.lang.reflect.Proxy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** E10: 추적(범위 · 오류 · 끝)이 던져도 inSpan 의 block 은 한 번만 돌고 결과 · 예외는 그대로. */
class InSpanFailureTest {
    private inline fun <reified T> throwing(): T =
        Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, m, _ ->
            if (m.name == "toString") "throwing" else throw IllegalStateException("tracing boom ${m.name}")
        } as T

    private val tracer: Tracer = throwing()
    private val span: Span = throwing()

    @Test
    fun `추적이 전부 던져도 block 은 한 번, 반환값 그대로`() {
        var runs = 0
        val result = tracer.inSpan(span) { runs++; "ok" }
        assertEquals(1, runs)
        assertEquals("ok", result)
    }

    @Test
    fun `추적이 전부 던져도 block 의 예외는 그대로 전파된다`() {
        var runs = 0
        val e = assertFailsWith<IllegalArgumentException> { tracer.inSpan(span) { runs++; throw IllegalArgumentException("biz") } }
        assertEquals("biz", e.message)
        assertEquals(1, runs)
    }
}
