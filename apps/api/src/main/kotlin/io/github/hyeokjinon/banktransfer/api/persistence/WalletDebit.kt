package io.github.hyeokjinon.banktransfer.api.persistence

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/**
 * 잔고 차감 전략 (P4-2, ADR-0009). 접수 트랜잭션의 바깥과 안, 두 지점에 끼운다 — 네 전략은 락이 걸리는 위치가 다르다.
 * 낙관적 재시도와 Redis 락은 트랜잭션 **바깥**이어야 한다: 재시도는 매번 새 트랜잭션으로, 락은 커밋이 끝난 뒤 푼다.
 * 조건부 · 비관적은 안에서 끝난다. 환불 · 충전은 전략과 무관하게 [WalletLedger] 의 원자적 UPDATE(balance = balance + ?)다 — 더하기라 음수 위험이 없다.
 * 어느 전략이 틀려도 CHECK (balance >= 0) 이 DB 에서 음수를 막는다.
 */
interface WalletDebit {
    /** 접수 트랜잭션 바깥. 기본은 그대로 실행. 낙관적 = 충돌하면 처음부터 다시, Redis = 사용자 락 안에서 실행하고 커밋 뒤 해제. */
    fun <T> around(userId: String, transaction: () -> T): T = transaction()

    /** 접수 트랜잭션 안. 잔고가 모자라거나 지갑이 없으면 false (아무것도 쓰지 않는다). 성공하면 원장 WITHDRAW 까지 쓴다. */
    fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean
}

/**
 * 기준선 (P4-1 과 같은 SQL): 조건부 UPDATE 한 문장. 행 락 한 번으로 원자적이고 왕복이 가장 적다.
 * 진 쪽은 앞 트랜잭션이 커밋할 때까지 행 락을 기다렸다가 바뀐 잔고로 조건(balance >= :amount)을 다시 본다 → 모자라면 0건.
 */
class ConditionalWalletDebit(
    private val jdbc: NamedParameterJdbcTemplate,
    private val ledger: WalletLedger,
) : WalletDebit {

    override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
        // 트랜잭션 밖이면 UPDATE 가 혼자 커밋돼 버린다 → 쓰기 전에 거부한다
        requireCallerTransaction()
        val updated = jdbc.update(
            "UPDATE wallet SET balance = balance - :amount, version = version + 1, updated_at = :at " +
                "WHERE user_id = :userId AND balance >= :amount",
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("amount", amount)
                .addValue("at", Timestamp.from(at.toDbPrecision())),
        )
        if (updated == 0) return false
        ledger.recordWithdraw(userId, transferId, amount, ledger.lockedBalance(userId), at)
        return true
    }
}

/**
 * 비관적 락: SELECT … FOR UPDATE 로 지갑 행의 X 락을 먼저 잡고, 읽은 잔고로 판단해 계산한 값을 쓴다.
 * "읽고 → 비교 → 값 쓰기"여도 안전하다: 락을 쥔 동안 다른 차감 · 환불 · 충전의 UPDATE 가 커밋까지 기다리므로 덮어쓸 값이 생기지 않는다.
 * 진 쪽은 FOR UPDATE 에서 기다렸다가 바뀐 잔고를 읽는다. 5초(innodb_lock_wait_timeout) 넘게 기다리면 MySQL 1205 → 저장소가 WalletBusy(503).
 * 조건부 UPDATE 와 결과가 같고 왕복이 하나(SELECT … FOR UPDATE) 더 있다 — 그 비용이 P4-2 측정에서 보려는 것이다.
 */
class PessimisticWalletDebit(
    private val jdbc: NamedParameterJdbcTemplate,
    private val ledger: WalletLedger,
) : WalletDebit {

    override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean {
        // 트랜잭션 밖이면 FOR UPDATE 가 문장 하나 끝에 풀려 락의 의미가 없다
        requireCallerTransaction()
        val balance = jdbc.query(
            "SELECT balance FROM wallet WHERE user_id = :userId FOR UPDATE",
            MapSqlParameterSource().addValue("userId", userId),
            RowMapper { rs, _ -> rs.getLong("balance") },
        ).singleOrNull()
        if (balance == null || balance < amount) return false
        val newBalance = balance - amount
        jdbc.update(
            "UPDATE wallet SET balance = :newBalance, version = version + 1, updated_at = :at WHERE user_id = :userId",
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("newBalance", newBalance)
                .addValue("at", Timestamp.from(at.toDbPrecision())),
        )
        ledger.recordWithdraw(userId, transferId, amount, newBalance, at)
        return true
    }
}
