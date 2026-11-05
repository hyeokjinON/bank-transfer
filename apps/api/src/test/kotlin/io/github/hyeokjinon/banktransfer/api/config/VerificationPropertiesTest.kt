package io.github.hyeokjinon.banktransfer.api.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class VerificationPropertiesTest {

    @Test
    fun `기본은 async`() {
        assertEquals("async", VerificationProperties().webMode)
    }

    @Test
    fun `소문자 그대로만 받는다 - 그 밖은 기동 실패`() {
        // @ConditionalOnProperty 는 대소문자를 무시하고 비교한다 — "Blocking" 을 받아 주면 판 이름과 info 표시가 어긋난다 (P6-1 과 같은 이유)
        for (bad in listOf("Blocking", "ASYNC", "coroutine", "")) {
            assertFailsWith<IllegalArgumentException>(bad) { VerificationProperties(webMode = bad) }
        }
        assertEquals("blocking", VerificationProperties(webMode = "blocking").webMode)
    }
}
