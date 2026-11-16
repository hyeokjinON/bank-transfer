package io.github.hyeokjinon.banktransfer.mockbank

import java.nio.file.Path
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * 모의 은행 실행 인자 (P5-1, P6-1, P8-3b):
 * `[포트] [--bank-files=디렉터리] [--close-day=yyyyMMdd] [--response-delay=<n>ms] [--single-session] [--session-idle-timeout=<n>s] [--unique-tran-no]`.
 * `--close-day` 가 있으면 서버를 띄우지 않고 그 날짜의 대사 파일만 만들고 끝난다 (손 확인 · 재생성용).
 * 디렉터리 기본값 `./bank-files` 는 작업 디렉터리 = 저장소 루트 기준이다 (Gradle run 이 루트에서 돈다 — API 의 daily-recon.file-dir 과 같은 곳).
 * `--response-delay` 는 모든 응답을 그만큼 늦춘다 (P6-1 측정). 0 ~ 2999ms — 게이트웨이 응답 시간 초과(3초) 이상이면 모든 출금이 결과 모름이 된다.
 * 실무 제약 재현 (P8-3b, 기본 꺼짐 — K8s 매니페스트에서만 켠다):
 * - `--single-session`: 기관당 세션 하나. 이미 있으면 새 연결을 받자마자 끊는다.
 * - `--session-idle-timeout=<n>s`: 그 시간 동안 받은 전문(폴링 포함)이 없으면 세션을 끊는다 (FIN 없이 사라진 상대 정리). 1 ~ 3600초.
 * - `--unique-tran-no`: 같은 KST 날짜에 이미 쓴 전문번호면 처리하지 않고 응답코드 3001.
 */
data class MockBankOptions(
    val port: Int = 9090,
    val bankFiles: Path = Path.of("./bank-files"),
    val closeDay: LocalDate? = null,
    val responseDelay: Duration = Duration.ZERO,
    val singleSession: Boolean = false,
    val sessionIdleTimeout: Duration? = null,
    val uniqueTranNo: Boolean = false,
) {
    companion object {
        private const val USAGE = "usage: [port] [--bank-files=dir] [--close-day=yyyyMMdd] [--response-delay=<0~2999>ms] " +
            "[--single-session] [--session-idle-timeout=<1~3600>s] [--unique-tran-no]"
        private const val MAX_DELAY_MS = 2999L
        private const val MAX_IDLE_S = 3600L
        private val DELAY = Regex("""(\d{1,4})ms""")
        private val IDLE = Regex("""(\d{1,4})s""")

        fun parse(args: Array<String>): MockBankOptions =
            args.fold(MockBankOptions()) { options, arg ->
                when {
                    arg.startsWith("--bank-files=") -> options.copy(bankFiles = Path.of(arg.substringAfter('=')))
                    arg.startsWith("--close-day=") -> options.copy(closeDay = closeDayOf(arg.substringAfter('=')))
                    arg.startsWith("--response-delay=") -> options.copy(responseDelay = responseDelayOf(arg.substringAfter('=')))
                    arg == "--single-session" -> options.copy(singleSession = true)
                    arg.startsWith("--session-idle-timeout=") -> options.copy(sessionIdleTimeout = idleTimeoutOf(arg.substringAfter('=')))
                    arg == "--unique-tran-no" -> options.copy(uniqueTranNo = true)
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

        private fun idleTimeoutOf(text: String): Duration {
            val s = IDLE.matchEntire(text)?.groupValues?.get(1)?.toLong()
                ?: throw IllegalArgumentException("session-idle-timeout must be <n>s (1 ~ $MAX_IDLE_S): '$text'")
            require(s in 1..MAX_IDLE_S) { "session-idle-timeout must be 1 ~ $MAX_IDLE_S s: ${s}s" }
            return Duration.ofSeconds(s)
        }
    }
}
