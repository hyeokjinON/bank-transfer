package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.Duration
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MockBankOptionsTest {

    @Test
    fun `인자가 없으면 9090 · 저장소 루트의 bank-files · 서버 실행`() {
        assertEquals(MockBankOptions(9090, Path.of("./bank-files"), null), MockBankOptions.parse(emptyArray()))
    }

    @Test
    fun `포트 · 디렉터리 · 마감일 (순서 무관)`() {
        assertEquals(
            MockBankOptions(9191, Path.of("tmp/bf"), LocalDate.of(2026, 10, 28)),
            MockBankOptions.parse(arrayOf("--close-day=20261028", "9191", "--bank-files=tmp/bf")),
        )
    }

    @Test
    fun `모르는 옵션 · 숫자가 아닌 포트 · yyyyMMdd 가 아닌 날짜는 거부`() {
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("--verbose")) }
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("90x0")) }
        assertFailsWith<IllegalArgumentException> { MockBankOptions.parse(arrayOf("--close-day=2026-10-28")) }
    }

    @Test
    fun `응답 지연 --response-delay=n ms 는 0 ~ 2999 만 받는다 (P6-1 - 게이트웨이 응답 시간 초과 3초 미만)`() {
        assertEquals(Duration.ZERO, MockBankOptions.parse(emptyArray()).responseDelay)
        assertEquals(Duration.ofMillis(100), MockBankOptions.parse(arrayOf("--response-delay=100ms")).responseDelay)
        assertEquals(Duration.ZERO, MockBankOptions.parse(arrayOf("--response-delay=0ms")).responseDelay)
        assertEquals(Duration.ofMillis(2999), MockBankOptions.parse(arrayOf("9190", "--response-delay=2999ms")).responseDelay)
        for (bad in listOf("3000ms", "100", "-1ms", "1s", "ms", "12345ms")) {
            assertFailsWith<IllegalArgumentException>(bad) { MockBankOptions.parse(arrayOf("--response-delay=$bad")) }
        }
    }
}
