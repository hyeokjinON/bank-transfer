package io.github.hyeokjinon.banktransfer.api.persistence

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Timestamp
import java.sql.Types
import java.time.Clock
import java.time.Instant
import java.util.UUID

/** 원장 한 줄의 종류. amount 는 항상 양수이고 방향은 종류가 정한다 (잔고 = DEPOSIT - WITHDRAW + REFUND). */
enum class LedgerType { DEPOSIT, WITHDRAW, REFUND }

/**
 * 사용자 원화 잔고(wallet)와 append-only 원장(wallet_ledger)의 돈 움직임 SQL 을 한곳에 모은다 (P4-1, ADR-0008).
 * P4-2 에서 차감 방식을 비관적 락 · 낙관적 락(version) · Redis 분산락으로 바꿔 끼울 이음새가 [withdraw] 다.
 *
 * - 차감은 조건부 UPDATE 한 문장(balance >= :amount)이라 행 락 한 번으로 원자적이다. 별도 락이 없다. 음수는 CHECK 제약이 2차로 막는다.
 * - [withdraw] · [refund] 는 호출자(JpaTransferStore)의 트랜잭션 안에서만 부른다: 출금 행 · 상태 전이 · 아웃박스와 함께 커밋되거나 함께 롤백돼야 한다.
 * - 원장의 balance_after 는 방금 바꾼 행(이 트랜잭션이 X 락을 쥔 행)을 다시 읽은 값이라 다른 트랜잭션과 섞이지 않는다.
 * - 시각은 Timestamp 로 넘기고 KST 변환은 JDBC 설정(connectionTimeZone)이 한다 (database.md §2).
 */
class WalletLedger(
    private val jdbc: NamedParameterJdbcTemplate,
    private val tx: TransactionTemplate,
    private val clock: Clock = Clock.systemUTC(),
) {

    /**
     * 충전 (관리 · 테스트용 — 실제로는 은행 입금 통지가 채우고 멱등 처리가 필요하다). 지갑이 없으면 만든다.
     * 자기 트랜잭션을 쓴다. 돌려주는 값 = 충전 뒤 잔고.
     */
    fun deposit(userId: String, amount: Long): Long {
        require(amount > 0) { "deposit amount must be positive, was $amount" }
        return tx.execute {
            val at = Timestamp.from(clock.instant().toDbPrecision())
            jdbc.update(
                "INSERT INTO wallet (user_id, balance, version, created_at, updated_at) VALUES (:userId, :amount, 0, :at, :at) AS new " +
                    "ON DUPLICATE KEY UPDATE balance = wallet.balance + new.balance, version = wallet.version + 1, updated_at = new.updated_at",
                MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", at),
            )
            val balance = lockedBalance(userId)
            appendLedger(userId, LedgerType.DEPOSIT, amount, balance, null, at)
            balance
        }!!
    }

    /**
     * 차감 (출금 접수). 잔고가 [amount] 이상일 때만 빼고 원장에 WITHDRAW 를 남긴다. 지갑이 없거나 모자라면 0건 → false (아무것도 쓰지 않는다).
     * 호출자 트랜잭션 안에서만 부른다 — false 면 호출자가 예외를 던져 출금 INSERT 까지 롤백한다.
     */
    fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
        requireCallerTransaction()
        val ts = Timestamp.from(at.toDbPrecision())
        val updated = jdbc.update(
            "UPDATE wallet SET balance = balance - :amount, version = version + 1, updated_at = :at " +
                "WHERE user_id = :userId AND balance >= :amount",
            MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", ts),
        )
        if (updated == 0) return false
        appendLedger(userId, LedgerType.WITHDRAW, amount, lockedBalance(userId), transferId, ts)
        return true
    }

    /**
     * 환불 (실패 확정). 이 출금의 WITHDRAW 원장이 있으면 그 금액을 돌려주고 REFUND 를 남긴다. 없으면(차감한 적 없는 P3 시절 출금) false.
     * 금액은 출금 행이 아니라 원장에서 읽는다 — 실제로 뺀 돈만 돌려준다.
     * 같은 출금에 REFUND 가 이미 있으면 uk_ledger_transfer_type 위반 → 예외 → 호출자 트랜잭션(상태 전이 포함)이 롤백된다 (이중 환불 2차 방어).
     */
    fun refund(transferId: UUID, at: Instant): Boolean {
        requireCallerTransaction()
        val (userId, amount) = jdbc.query(
            "SELECT user_id, amount FROM wallet_ledger WHERE transfer_id = :transferId AND type = 'WITHDRAW'",
            MapSqlParameterSource().addValue("transferId", transferId.toString()),
            RowMapper { rs, _ -> rs.getString("user_id") to rs.getLong("amount") },
        ).singleOrNull() ?: return false
        val ts = Timestamp.from(at.toDbPrecision())
        val updated = jdbc.update(
            "UPDATE wallet SET balance = balance + :amount, version = version + 1, updated_at = :at WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId).addValue("amount", amount).addValue("at", ts),
        )
        check(updated == 1) { "wallet of $userId is missing although transfer $transferId was debited" }
        appendLedger(userId, LedgerType.REFUND, amount, lockedBalance(userId), transferId, ts)
        return true
    }

    /** 지금 잔고. 지갑이 없으면 0. */
    fun balanceOf(userId: String): Long =
        jdbc.query(
            "SELECT balance FROM wallet WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId),
            RowMapper { rs, _ -> rs.getLong("balance") },
        ).singleOrNull() ?: 0L

    /** 결과가 정해지지 않아 잔고에서 빠진 채 묶인 금액 = 내 출금 중 RECEIVED · SENDING · UNKNOWN 의 합 (idx_transfer_user 로 사용자 범위를 좁힌다). */
    fun pendingOf(userId: String): Long =
        jdbc.queryForObject(
            "SELECT COALESCE(SUM(amount), 0) FROM transfer WHERE user_id = :userId AND status IN ('RECEIVED', 'SENDING', 'UNKNOWN')",
            MapSqlParameterSource().addValue("userId", userId),
            Long::class.java,
        )!!

    private fun lockedBalance(userId: String): Long =
        jdbc.queryForObject(
            "SELECT balance FROM wallet WHERE user_id = :userId",
            MapSqlParameterSource().addValue("userId", userId),
            Long::class.java,
        )!!

    private fun appendLedger(userId: String, type: LedgerType, amount: Long, balanceAfter: Long, transferId: UUID?, at: Timestamp) {
        jdbc.update(
            "INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at) " +
                "VALUES (:userId, :type, :amount, :balanceAfter, :transferId, :at)",
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("type", type.name)
                .addValue("amount", amount)
                .addValue("balanceAfter", balanceAfter)
                .addValue("transferId", transferId?.toString(), Types.CHAR)   // DEPOSIT 은 NULL — 타입을 줘서 드라이버가 추측하지 않게
                .addValue("at", at),
        )
    }

    private fun requireCallerTransaction() =
        check(TransactionSynchronizationManager.isActualTransactionActive()) {
            "must run inside the caller's transaction, together with the transfer row"
        }
}
