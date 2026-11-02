package io.github.hyeokjinon.banktransfer.api.retention

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicLong

/**
 * 공유 DB 의 보존 배치 테스트는 이 날짜만 쓴다 (P5-2). 삭제 조건은 표 전체에 걸리므로 날짜가 앞뒤로 섞이면 다른 테스트의 행까지 지운다:
 * - 먼 미래(ReconTestDates)면 기준 시각이 미래라 발행된 행을 다 지우고, 관리 API 도 400 으로 거른다
 * - 오늘이면 JpaVerificationRepositoryTest 가 남긴 SENT(2026-10-19 만료)까지 지워 건수가 흔들린다
 * 그래서 2000-01-01 에서 과거로만 간다: 앞 테스트가 남긴 행은 모두 다음 테스트의 기준 시각보다 뒤다.
 * 간격 40일 > 인증 보관 30일 + 심는 여유 1일.
 */
object RetentionTestDates {
    private const val STEP_DAYS = 40L
    private val calls = AtomicLong()

    fun next(): LocalDate = LocalDate.of(2000, 1, 1).minusDays(STEP_DAYS * calls.getAndIncrement())
}
