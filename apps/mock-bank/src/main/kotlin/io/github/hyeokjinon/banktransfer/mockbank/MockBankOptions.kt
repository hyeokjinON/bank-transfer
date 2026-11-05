package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * 모의 은행 실행 인자 (P5-1, P6-1): `[포트] [--bank-files=디렉터리] [--close-day=yyyyMMdd] [--response-delay=<n>ms]`.
 * `--close-day` 가 있으면 서버를 띄우지 않고 그 날짜의 대사 파일만 만들고 끝난다 (손 확인 · 재생성용).
 * 디렉터리 기본값 `./bank-files` 는 작업 디렉터리 = 저장소 루트 기준이다 (Gradle run 이 루트에서 돈다 — API 의 daily-recon.file-dir 과 같은 곳).
 * `--response-delay` 는 모든 응답을 그만큼 늦춘다 (P6-1 측정). 0 ~ 2999ms — 게이트웨이 응답 시간 초과(3초) 이상이면 모든 출금이 결과 모름이 된다.
 */
data class MockBankOptions(
    val port: Int = 9090,
    val bankFiles: Path = Path.of("./bank-files"),
    val closeDay: LocalDate? = null,
    val responseDelay: Duration = Duration.ZERO,
) {
    companion object {
        private const val USAGE = "usage: [port] [--bank-files=dir] [--close-day=yyyyMMdd] [--response-delay=<0~2999>ms]"
        private const val MAX_DELAY_MS = 2999L
        private val DELAY = Regex("""(\d{1,4})ms""")

        fun parse(args: Array<String>): MockBankOptions =
            args.fold(MockBankOptions()) { options, arg ->
                when {
                    arg.startsWith("--bank-files=") -> options.copy(bankFiles = Path.of(arg.substringAfter('=')))
                    arg.startsWith("--close-day=") -> options.copy(closeDay = closeDayOf(arg.substringAfter('=')))
                    arg.startsWith("--response-delay=") -> options.copy(responseDelay = responseDelayOf(arg.substringAfter('=')))
                    arg.startsWith("--") -> throw IllegalArgumentException("unknown option '$arg' — $USAGE")
                    else -> options.copy(port = arg.toIntOrNull() ?: throw IllegalArgumentException("port must be a number: '$arg' — $USAGE"))
                }
            }

        private fun closeDayOf(text: String): LocalDate =
            try {
                LocalDate.parse(text, DateTimeFormatter.BASIC_ISO_DATE)
            } catch (e: DateTimeParseException) {
                throw IllegalArgumentException("close-day must be yyyyMMdd: '$text'", e)
            }

        private fun responseDelayOf(text: String): Duration {
            val ms = DELAY.matchEntire(text)?.groupValues?.get(1)?.toLong()
                ?: throw IllegalArgumentException("response-delay must be <n>ms (0 ~ $MAX_DELAY_MS): '$text'")
            require(ms <= MAX_DELAY_MS) { "response-delay must be below the gateway timeout (3s): ${ms}ms" }
            return Duration.ofMillis(ms)
        }
    }
}
