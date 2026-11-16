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

    @Test
    fun `실무 제약 옵션 (P8-3b) - 기본은 모두 꺼짐`() {
        val defaults = MockBankOptions.parse(emptyArray())
        assertEquals(false, defaults.singleSession)
        assertEquals(null, defaults.sessionIdleTimeout)
        assertEquals(false, defaults.uniqueTranNo)

        val on = MockBankOptions.parse(arrayOf("9090", "--bank-files=/data/bank-files", "--single-session", "--session-idle-timeout=60s", "--unique-tran-no"))
        assertEquals(true, on.singleSession)
        assertEquals(Duration.ofSeconds(60), on.sessionIdleTimeout)
        assertEquals(true, on.uniqueTranNo)
        assertEquals(Path.of("/data/bank-files"), on.bankFiles)
    }

    @Test
    fun `세션 유휴 시간은 1 ~ 3600초만 받는다`() {
        assertEquals(Duration.ofSeconds(1), MockBankOptions.parse(arrayOf("--session-idle-timeout=1s")).sessionIdleTimeout)
        assertEquals(Duration.ofSeconds(3600), MockBankOptions.parse(arrayOf("--session-idle-timeout=3600s")).sessionIdleTimeout)
        for (bad in listOf("0s", "3601s", "60", "60ms", "1m", "s")) {
            assertFailsWith<IllegalArgumentException>(bad) { MockBankOptions.parse(arrayOf("--session-idle-timeout=$bad")) }
        }
    }
}
