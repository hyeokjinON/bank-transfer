package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
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
}
