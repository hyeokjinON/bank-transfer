package io.github.hyeokjinon.banktransfer.api.alert

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AlertTextTest {

    @Test
    fun `계좌번호는 앞 4 · 뒤 4 만 남기고 가운데는 길이와 무관하게 별 4개`() {
        assertEquals("1002****6789", AlertText.maskAccount("1002123456789"))
        assertEquals("3333****5678", AlertText.maskAccount("3333012345678"))
        assertEquals("1234****3456", AlertText.maskAccount("1234567890123456"))
        assertEquals("********", AlertText.maskAccount("12345678"))
    }

    @Test
    fun `본문의 10~16자리 숫자 덩어리는 모두 가린다 - 거래번호 · 금액 · 날짜는 그대로`() {
        val text = "090-1002123456789 T261028ABCDEFGHIJKLM 30000원 2026-10-28 5500000000001"

        assertEquals("090-1002****6789 T261028ABCDEFGHIJKLM 30000원 2026-10-28 5500****0001", AlertText.maskAccountNumbers(text))
    }

    @Test
    fun `수준 · 제목 · 요약 · 목록 순서, 목록은 20건까지 + 외 n건`() {
        val text = AlertText.render(Alert(AlertLevel.CRITICAL, "제목", listOf("요약1", "요약2"), (1..23).map { "건 $it" }))

        val lines = text.split("\n")
        assertEquals("[CRITICAL] 제목", lines[0])
        assertEquals(listOf("요약1", "요약2"), lines.subList(1, 3))
        assertEquals("- 건 1", lines[3])
        assertEquals("- 건 20", lines[22])
        assertEquals("외 3건", lines[23])
        assertEquals(24, lines.size)
    }

    @Test
    fun `4096자를 넘으면 자른다`() {
        val text = AlertText.render(Alert(AlertLevel.INFO, "x".repeat(5_000)))

        assertEquals(AlertText.MAX_CHARS, text.length)
        assertTrue(text.endsWith("…"))
        assertFalse(AlertText.render(Alert(AlertLevel.INFO, "짧다")).endsWith("…"))
    }
}
