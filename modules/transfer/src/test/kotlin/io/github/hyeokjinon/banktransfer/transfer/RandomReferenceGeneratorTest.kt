package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class RandomReferenceGeneratorTest {

    // UTC 10-18 15:30 = KST 10-19 00:30 → 날짜는 KST 기준 261019
    private val clock = Clock.fixed(Instant.parse("2026-10-18T15:30:00Z"), ZoneOffset.UTC)

    @Test
    fun `접두 + KST 날짜 + 대문자·숫자 13자 = 20자`() {
        val reference = RandomReferenceGenerator(prefix = 'T', clock = clock).next()

        assertTrue(Regex("T261019[0-9A-Z]{13}").matches(reference), reference)
    }

    @Test
    fun `호출마다 다른 번호`() {
        val generator = RandomReferenceGenerator(clock = clock)

        assertNotEquals(generator.next(), generator.next())
    }
}
