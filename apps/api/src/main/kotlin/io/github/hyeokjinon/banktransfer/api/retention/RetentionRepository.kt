package io.github.hyeokjinon.banktransfer.api.retention

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Instant

/**
 * 보존 배치의 삭제 SQL (P5-2, 설계 §3). 되돌릴 수 없으므로 조건을 이 두 문장에만 둔다.
 * 시각은 Timestamp(Instant) 로 넘기고 KST 변환은 JDBC 설정이 한다 (database.md §2). 부르는 쪽(단계)의 트랜잭션에 합류한다.
 * ORDER BY 는 두지 않는다: 어느 행부터 지워도 결과가 같고, MySQL 8 기본 binlog_format=ROW 라 LIMIT 의 비결정성이 복제에 문제가 되지 않는다.
 */
class RetentionRepository(private val jdbc: NamedParameterJdbcTemplate) {

    /** 발행된 지 오래된 아웃박스. 미발행(published_at IS NULL)은 NULL 이라 `<` 비교에 걸리지 않아 아무리 오래돼도 남는다. */
    fun deleteOutboxEvents(cutoff: Instant, limit: Int): Int =
        jdbc.update("DELETE FROM outbox_event WHERE published_at < :cutoff LIMIT :limit", params(cutoff, limit))

    /**
     * 끝나지 않고 만료된 1원인증. **VERIFIED 는 절대 지우지 않는다** — expires_at 은 코드 입력 시한이라 VERIFIED 도 지나 있고,
     * 출금 접수는 existsVerified(status = VERIFIED 행이 있는가)만 본다. expires_at 만으로 지우면 인증한 사용자 전부의 출금이 422 다.
     * SENT 도 지운다: 코드를 입력하지 않고 시간만 지난 건은 상태가 SENT 그대로다 (EXPIRED 는 만료 뒤 확인을 시도했을 때만 된다).
     * 상태를 "VERIFIED 가 아닌 것" 이 아니라 목록으로 적는 이유: 나중에 생길 새 상태를 묻지 않고 지우지 않게.
     */
    fun deleteVerifications(cutoff: Instant, limit: Int): Int =
        jdbc.update(
            "DELETE FROM account_verification WHERE status IN ('SENT', 'EXPIRED', 'LOCKED') AND expires_at < :cutoff LIMIT :limit",
            params(cutoff, limit),
        )

    private fun params(cutoff: Instant, limit: Int) =
        MapSqlParameterSource().addValue("cutoff", Timestamp.from(cutoff)).addValue("limit", limit)
}
