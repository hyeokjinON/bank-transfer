package io.github.hyeokjinon.banktransfer.transfer

import kotlin.test.Test
import kotlin.test.assertFalse

class StoreConflictTest {

    @Test
    fun `StoreConflict 메시지에는 사용자 id · 멱등키 · 금액이 없다`() {
        // 처리되지 않은 예외는 메시지 · 스택이 로그에 찍힌다 (P9c 결정 F6 ①) — 생성자가 값을 받지 않으니 메시지에 실릴 수도 없다
        val messages = listOf(
            StoreConflict.DuplicateIdempotencyKey(),
            StoreConflict.InsufficientBalance(),
            StoreConflict.WalletBusy(),
            StoreConflict.ConcurrentUpdate(),
        ).map { it.message.orEmpty() }

        for (message in messages) {
            assertFalse("user-secret-7" in message, message)
            assertFalse("key-secret-9" in message, message)
            assertFalse("7654321" in message, message)
            assertFalse("'" in message, "값을 감싸는 따옴표가 남았다: $message")
        }
    }
}
