package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * 낙관적 락 (P4-2): 락 없이 잔고 · version 을 읽고, version 이 그대로일 때만 계산한 잔고를 쓴다.
 * 안전한 이유: 환불 · 충전 · 다른 차감이 모두 version 을 올리므로, 그 사이 바뀌었으면 UPDATE 가 0건이 되어 충돌로 잡힌다.
 * 충돌이면 [around] 가 트랜잭션 **전체**(출금 INSERT 포함)를 롤백시킨 뒤 같은 출금(같은 id · 거래번호)으로 처음부터 다시 한다 —
 * 같은 트랜잭션 안에서 다시 읽으면 REPEATABLE READ 스냅샷이라 같은 옛 값이 보인다. 재시도 대상은 버전 충돌뿐이다
 * (같은 키 UNIQUE 충돌은 그대로 나가 기존 멱등 응답 경로를 탄다). [maxAttempts] 를 다 쓰면 ConcurrentUpdate → 409.
 * 시도 사이 무작위 대기는 같은 순간 다시 부딪히지 않게 하려는 것이고, 톰캣 스레드에서 잔다(최대 약 200ms — 측정 결과의 일부).
 */
class OptimisticWalletDebit(
    private val jdbc: NamedParameterJdbcTemplate,
    private val ledger: WalletLedger,
    private val maxAttempts: Int = 5,
    private val backoffMin: Duration = Duration.ofMillis(10),
    private val backoffMax: Duration = Duration.ofMillis(50),
    private val sleeper: (Duration) -> Unit = { Thread.sleep(it) },
    /** 테스트 훅: 읽은 뒤 · 쓰기 전. 다른 트랜잭션이 끼어드는 순간을 결정적으로 만든다. */
    private val beforeWrite: (userId: String) -> Unit = {},
) : WalletDebit {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, was $maxAttempts" }
        require(!backoffMin.isNegative && backoffMin <= backoffMax) { "backoff must be 0 <= min <= max, was $backoffMin..$backoffMax" }
    }

    override fun <T> around(userId: String, transaction: () -> T): T {
        var attempt = 1
        while (true) {
            try {
                return transaction()
            } catch (e: VersionConflict) {
                if (attempt >= maxAttempts) {
                    // 핫스팟에서는 흔한 결과라 INFO (부하 측정은 WARN 으로 돌린다 — 요청마다 로그를 쓰면 측정이 흐려진다)
                    log.info("wallet of {} kept changing for {} attempts, answering CONCURRENT_UPDATE", userId, maxAttempts)
                    throw StoreConflict.ConcurrentUpdate(userId)
                }
                sleeper(nextBackoff())
                attempt++
            }
        }
    }

    override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
        // 트랜잭션 밖이면 충돌 때 출금 INSERT 를 되돌릴 수 없다
        requireCallerTransaction()
        val current = jdbc.query(
            "SELECT balance, version FROM wallet WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId),
            RowMapper { rs, _ -> rs.getLong("balance") to rs.getLong("version") },
        ).singleOrNull()
        // 모자라면 다시 해도 소용없다 → 재시도 없이 false (InsufficientBalance)
        if (current == null || current.first < amount) return false
        val (balance, version) = current
        beforeWrite(userId)
        val newBalance = balance - amount
        val updated = jdbc.update(
            "UPDATE wallet SET balance = :newBalance, version = :version + 1, updated_at = :at " +
                "WHERE user_id = :userId AND version = :version",
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("newBalance", newBalance)
                .addValue("version", version)
                .addValue("at", Timestamp.from(at.toDbPrecision())),
        )
        if (updated == 0) throw VersionConflict(userId)
        ledger.recordWithdraw(userId, transferId, amount, newBalance, at)
        return true
    }

    private fun nextBackoff(): Duration =
        Duration.ofMillis(ThreadLocalRandom.current().nextLong(backoffMin.toMillis(), backoffMax.toMillis() + 1))

    /**
     * 읽은 뒤 다른 트랜잭션이 version 을 올렸다. 트랜잭션 전체를 롤백시키고 [around] 가 처음부터 다시 한다. 이 클래스 밖으로 나가지 않는다.
     * 스택 트레이스를 만들지 않는다 — 핫스팟에서는 요청마다 여러 번 생기는 흐름 제어용 예외다.
     */
    internal class VersionConflict(userId: String) : RuntimeException("wallet version of '$userId' changed", null, false, false)
}
