package io.github.hyeokjinon.banktransfer.gateway

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId

/**
 * 은행 점검 시간 (P5-1). 시작 포함 · 끝 제외, [start] > [end] 면 자정을 넘는 구간이다 (기본 23:50 ~ 00:10 KST).
 * 점검 중에는 아무것도 은행에 보내지 않는다 → "D일에 보낸 출금" 과 "D일 대사 파일" 이 같은 범위가 된다.
 * 판단은 늘 [zone] 의 벽시계로 한다: Instant 는 시간대가 없으므로 `LocalTime.now(clock)` 처럼 시계의 시간대(서버는 UTC)로 읽으면 9시간 어긋난다.
 * 순수 객체다 — 지금 시각은 부르는 쪽이 주입한 Clock 에서 넘긴다 (테스트가 시각을 고정할 수 있게).
 * 점검 길이는 재발행 포기(E001)까지 걸리는 약 26분보다 짧게 둔다 — 길면 점검 때문에 포기하는 출금이 생긴다.
 */
class BankMaintenanceWindow(
    val start: LocalTime,
    val end: LocalTime,
    val zone: ZoneId,
    val enabled: Boolean = true,
) {
    init {
        require(start != end) { "maintenance start and end must differ: $start" }
    }

    private val crossesMidnight = start > end

    /** 점검 길이 (자정을 넘으면 다음 날 끝까지). */
    val length: Duration = Duration.between(start, end).let { if (it.isNegative) it.plusDays(1) else it }

    fun isActive(now: Instant): Boolean {
        if (!enabled) return false
        val time = LocalTime.ofInstant(now, zone)
        return if (crossesMidnight) time >= start || time < end else time >= start && time < end
    }

    /** 점검이 끝날 때까지 남은 시간. 점검 중이 아니면 0. */
    fun remaining(now: Instant): Duration {
        if (!isActive(now)) return Duration.ZERO
        val local = now.atZone(zone)
        // 자정 전(시작 쪽)이면 끝은 다음 날이다
        val endDate = if (crossesMidnight && local.toLocalTime() >= start) local.toLocalDate().plusDays(1) else local.toLocalDate()
        return Duration.between(now, endDate.atTime(end).atZone(zone).toInstant())
    }

    /** `Retry-After` 초 — 남은 시간을 올림한다 (899.5초 → 900). 점검 중이 아니면 0. */
    fun remainingSeconds(now: Instant): Long {
        val remaining = remaining(now)
        return remaining.seconds + if (remaining.nano > 0) 1 else 0
    }
}
