package io.github.hyeokjinon.banktransfer.reactive.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ReactiveVerificationPropertiesTest {

    @Test
    fun `기본은 coroutine, 소문자 그대로만 받는다`() {
        assertEquals("coroutine", ReactiveVerificationProperties().webMode)
        assertEquals("reactor", ReactiveVerificationProperties(webMode = "reactor").webMode)
        for (bad in listOf("Reactor", "COROUTINE", "async", "")) {
            assertFailsWith<IllegalArgumentException>(bad) { ReactiveVerificationProperties(webMode = bad) }
        }
    }
}
