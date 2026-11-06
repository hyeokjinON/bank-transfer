package io.github.hyeokjinon.banktransfer.reactive.web

import java.util.concurrent.CancellationException
import java.util.concurrent.CompletionException
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

/**
 * 처리기가 Exception 을 받게 된 뒤에도 취소는 오류 응답으로 바꾸지 않는다 (P6-2, 설계 §4-3).
 * 클라이언트 끊김은 보통 구독 취소로 와서 처리기에 닿지 않지만, 취소 예외가 흘러 들어오면 500 으로 바꾸지 않고 다시 던져야 취소가 사라지지 않는다.
 */
class ReactiveExceptionHandlerTest {

    private val handler = ReactiveExceptionHandler()

    @Test
    fun `취소 예외는 다시 던진다`() {
        val cancel = CancellationException("client disconnected")
        assertSame(cancel, assertFailsWith<CancellationException> { handler.handle(cancel) })
    }

    @Test
    fun `CompletionException 에 싸인 취소도 다시 던진다`() {
        val wrapped = CompletionException(CancellationException("client disconnected"))
        assertSame(wrapped, assertFailsWith<CompletionException> { handler.handle(wrapped) })
    }
}
