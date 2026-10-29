package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.dao.PessimisticLockingFailureException
import org.springframework.data.domain.Limit
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "transfer")
class TransferEntity(
    @Id @Column(columnDefinition = "char(36)") var id: String = "",
    var userId: String? = null,
    var idempotencyKey: String = "",
    @Column(columnDefinition = "char(20)") var reference: String = "",
    @Column(columnDefinition = "char(64)") var requestHash: String = "",
    var bankCode: String = "",
    var accountNo: String = "",
    var amount: Long = 0,
    var memo: String = "",
    @Enumerated(EnumType.STRING) var status: TransferStatus = TransferStatus.RECEIVED,
    var bankResponseCode: String? = null,
    var sendingAt: Instant? = null,
    var inquiryAttempts: Int = 0,
    var republishCount: Int = 0,
    var republishedAt: Instant? = null,
    var createdAt: Instant = Instant.EPOCH,
    var updatedAt: Instant = Instant.EPOCH,
    /** 낙관적 락. null 이면 Spring Data 가 새 엔티티로 보고 INSERT(persist) 한다. */
    @Version var version: Long? = null,
) {
    fun toDomain() = Transfer(
        id = UUID.fromString(id), userId = userId, idempotencyKey = idempotencyKey, reference = reference, requestHash = requestHash,
        bankCode = bankCode, accountNo = accountNo, amount = amount, memo = memo, status = status,
        bankResponseCode = bankResponseCode, sendingAt = sendingAt, inquiryAttempts = inquiryAttempts,
        republishCount = republishCount, republishedAt = republishedAt, createdAt = createdAt, updatedAt = updatedAt,
    )

    companion object {
        /**
         * 도메인 -> 새 엔티티 (INSERT 전용). version 이 null 이라 save 가 항상 persist 한다.
         * P3-2 의 상태 변경은 이 메서드로 만든 엔티티를 다시 save 하지 말고, 읽어 온 엔티티를 고치거나 조건부 UPDATE 를 쓴다.
         */
        fun from(t: Transfer) = TransferEntity(
            id = t.id.toString(), userId = t.userId, idempotencyKey = t.idempotencyKey, reference = t.reference, requestHash = t.requestHash,
            bankCode = t.bankCode, accountNo = t.accountNo, amount = t.amount, memo = t.memo, status = t.status,
            bankResponseCode = t.bankResponseCode, sendingAt = t.sendingAt?.toDbPrecision(), inquiryAttempts = t.inquiryAttempts,
            republishCount = t.republishCount, republishedAt = t.republishedAt?.toDbPrecision(),
            createdAt = t.createdAt.toDbPrecision(), updatedAt = t.updatedAt.toDbPrecision(),
        )
    }
}

/** 트랜잭셔널 아웃박스. published_at 이 NULL 이면 아직 Kafka 로 나가지 않은 이벤트다 (발행은 P3-2). */
@Entity
@Table(name = "outbox_event")
class OutboxEventEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) var id: Long? = null,
    @Column(columnDefinition = "char(36)") var aggregateId: String = "",
    var eventType: String = "",
    @JdbcTypeCode(SqlTypes.JSON) var payload: String = "{}",
    var createdAt: Instant = Instant.EPOCH,
    var publishedAt: Instant? = null,
)

interface TransferJpaRepository : JpaRepository<TransferEntity, String> {
    /** 멱등 조회는 사용자 범위 (P4-1). uk_transfer_user_idempotency (user_id, idempotency_key) 를 탄다. */
    fun findByUserIdAndIdempotencyKey(userId: String, idempotencyKey: String): TransferEntity?

    fun findByStatusAndSendingAtBeforeOrderBySendingAt(status: TransferStatus, cutoff: Instant, limit: Limit): List<TransferEntity>

    fun findByStatusAndInquiryAttemptsLessThanAndSendingAtBeforeOrderByUpdatedAt(
        status: TransferStatus, maxAttempts: Int, sendingBefore: Instant, limit: Limit,
    ): List<TransferEntity>

    /** 결과조회 시도 +1. 상태가 바뀌었으면(컨슈머가 먼저 확정) 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.inquiryAttempts = t.inquiryAttempts + 1, t.updatedAt = :at, " +
            "t.version = t.version + 1 where t.id = :id and t.status = :status",
    )
    fun incrementInquiryAttempts(@Param("id") id: String, @Param("status") status: TransferStatus, @Param("at") at: Instant): Int

    /**
     * 재발행 대상 (P3-3b). 두 컬럼 비교(sending_at > republished_at)라 메서드 이름 쿼리로는 쓸 수 없다.
     * status 조건이 idx_transfer_status(status, sending_at) 로 범위를 좁힌다.
     */
    @Query(
        "select t from TransferEntity t where t.status = :status and t.updatedAt < :updatedBefore " +
            "and t.sendingAt is not null and (t.republishedAt is null or t.sendingAt > t.republishedAt) order by t.updatedAt",
    )
    fun findRepublishable(
        @Param("status") status: TransferStatus,
        @Param("updatedBefore") updatedBefore: Instant,
        limit: Limit,
    ): List<TransferEntity>

    /** 재발행 횟수 +1 (P3-3b). 상태나 횟수가 기대와 다르면(다른 쪽이 먼저 바꿈) 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.republishCount = t.republishCount + 1, t.republishedAt = :at, t.updatedAt = :at, " +
            "t.version = t.version + 1 where t.id = :id and t.status = :status and t.republishCount = :expected",
    )
    fun incrementRepublishCount(
        @Param("id") id: String,
        @Param("status") status: TransferStatus,
        @Param("expected") expected: Int,
        @Param("at") at: Instant,
    ): Int

    /** 조건부 상태 변경 (SENDING 으로 갈 때 sending_at 도 기록). 현재 상태가 from 이 아니면 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.status = :to, t.sendingAt = :at, t.updatedAt = :at, " +
            "t.bankResponseCode = :code, t.version = t.version + 1 where t.id = :id and t.status = :from",
    )
    fun transitionToSending(
        @Param("id") id: String,
        @Param("from") from: TransferStatus,
        @Param("to") to: TransferStatus,
        @Param("at") at: Instant,
        @Param("code") code: String?,
    ): Int

    /** 조건부 상태 변경 (결과 기록 등). 현재 상태가 from 이 아니면 0 건. */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.status = :to, t.updatedAt = :at, " +
            "t.bankResponseCode = :code, t.version = t.version + 1 where t.id = :id and t.status = :from",
    )
    fun transition(
        @Param("id") id: String,
        @Param("from") from: TransferStatus,
        @Param("to") to: TransferStatus,
        @Param("at") at: Instant,
        @Param("code") code: String?,
    ): Int

    /**
     * [transition] + sending_at 이 [sentFrom, sentUntil) 안일 때만 (P5-1 일마감 대사의 확정). 상태와 같은 문장의 조건이라
     * 읽은 뒤 다시 보내져(RECEIVED → SENDING, sending_at = 지금) 상태가 다시 같아진 출금도 0건이 된다.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        "update TransferEntity t set t.status = :to, t.updatedAt = :at, " +
            "t.bankResponseCode = :code, t.version = t.version + 1 where t.id = :id and t.status = :from " +
            "and t.sendingAt >= :sentFrom and t.sendingAt < :sentUntil",
    )
    fun transitionIfSentWithin(
        @Param("id") id: String,
        @Param("from") from: TransferStatus,
        @Param("to") to: TransferStatus,
        @Param("at") at: Instant,
        @Param("code") code: String?,
        @Param("sentFrom") sentFrom: Instant,
        @Param("sentUntil") sentUntil: Instant,
    ): Int
}

interface OutboxEventJpaRepository : JpaRepository<OutboxEventEntity, Long> {
    fun countByAggregateId(aggregateId: String): Long
}

/**
 * 메시지는 "이 이체를 처리하라"는 신호만 담는다. 컨슈머는 DB 에서 최신 상태를 읽는다.
 * eventId 는 outbox_event.id (INSERT 때 정해짐) 라 여기 넣지 않고 발행할 때(P3-2) 붙인다.
 */
internal fun outboxPayload(event: TransferEvent): String = when (event) {
    is TransferEvent.TransferRequested ->
        """{"transferId":"${event.transferId}","occurredAt":"${event.occurredAt}"}"""
}

/**
 * 이체 저장소 (MySQL). saveWithEvent 는 transfer · 잔고 차감(wallet · wallet_ledger) · outbox_event 를 한 트랜잭션에 쓴다:
 * 모두 커밋되거나 모두 롤백되므로 "돈은 빠졌는데 출금이 없는" 상태나 "이체는 있는데 처리 신호가 없는" 상태가 생기지 않는다 (P4-1).
 * 차감 방식은 [debit] 전략이 정한다 (P4-2, ADR-0009): 트랜잭션 바깥([WalletDebit.around] — 낙관적 재시도 · Redis 락)과 안([WalletDebit.withdraw]).
 * 실패 확정(fail)은 상태 전이와 환불을 한 트랜잭션에 쓴다 — 환불은 전략과 무관하게 [wallets] 의 원자적 UPDATE 다.
 * UNIQUE 위반은 제약 이름을 보고, 락 실패(1205 · 1213)는 예외 계열을 보고 도메인 예외([StoreConflict])로 바꿔 던진다.
 *
 * 바깥 트랜잭션 안에서 [saveWithEvent] 를 부르면 거부한다 (IllegalStateException, P4-2 에서 문서상 전제를 check 로 바꿨다).
 * 이 클래스가 자기 트랜잭션을 열고 닫는 것에 기대는 것이 셋이다: UNIQUE 위반이면 이 트랜잭션이 롤백된 뒤에야 호출자(TransferService)가
 * 이긴 쪽 건을 다시 읽을 수 있고, 낙관적 재시도는 매번 새 트랜잭션이어야 하며, Redis 락은 커밋이 끝난 뒤 풀어야 한다.
 * 바깥 트랜잭션에 합류(REQUIRED)하면 셋 다 깨진다.
 */
class JpaTransferStore(
    private val transfers: TransferJpaRepository,
    private val outbox: OutboxEventJpaRepository,
    private val tx: TransactionTemplate,
    private val wallets: WalletLedger,
    private val debit: WalletDebit,
    private val payloadOf: (TransferEvent) -> String = ::outboxPayload,
) : TransferStore {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 순서: 출금 INSERT(flush) → 잔고 차감 + 원장 WITHDRAW → 아웃박스 INSERT. 하나라도 실패하면 전부 롤백된다.
     * 출금 INSERT 를 먼저 하는 이유: 같은 (사용자, 키)가 동시에 오면 진 쪽이 UNIQUE 에서 멈춰(이긴 쪽 커밋까지 대기) 지갑을 건드리지 않고
     * 기존 건을 돌려받는다. 차감을 먼저 하면 잔고가 한 건 몫뿐일 때 진 쪽이 멱등 응답(200) 대신 잔고 부족(422)을 받는다.
     * 아웃박스를 차감 뒤에 두는 이유: 마지막 INSERT 가 실패해도 차감까지 되돌아간다는 것을 테스트로 보일 수 있다.
     * 락을 못 잡으면(MySQL 1205 락 대기 초과 · 1213 데드락 — Spring 의 PessimisticLockingFailureException 계열) 전략과 무관하게
     * [StoreConflict.WalletBusy] 다: 아무것도 남지 않았고, 다시 보내면 된다(같은 키면 멱등 응답). P4-1 의 "같은 키 3건 이상 동시 + 잔고 부족 → 1213 → 500" 도 이 길로 503 이 된다.
     */
    override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer {
        // 새 출금은 반드시 지갑에서 뺀다. 사용자 없는 출금을 받으면 차감 없이 돈이 나가는 길이 생긴다
        val userId = requireNotNull(transfer.userId) { "a new transfer must have userId (it is debited from the user's wallet)" }
        check(!TransactionSynchronizationManager.isActualTransactionActive()) {
            "saveWithEvent must not join an outer transaction: retries and locks wrap its own transaction"
        }
        return try {
            debit.around(userId) {
                tx.execute {
                    // flush 로 UNIQUE 위반을 여기서 터뜨려야 아래 catch 에서 StoreConflict 로 바꿀 수 있다 (커밋 시점까지 미루지 않는다)
                    val saved = transfers.saveAndFlush(TransferEntity.from(transfer))
                    if (!debit.withdraw(userId, transfer.id, transfer.amount, transfer.createdAt)) {
                        // false = 잔고 부족 또는 지갑 없음. 예외로 트랜잭션 전체(방금 INSERT 한 출금 포함)를 롤백한다
                        throw StoreConflict.InsufficientBalance(userId, transfer.amount)
                    }
                    outbox.saveAndFlush(outboxRow(event))
                    saved.toDomain()
                }!!
            }
        } catch (e: DataIntegrityViolationException) {
            // MySQL 메시지 예: Duplicate entry 'user-1-k1' for key 'transfer.uk_transfer_user_idempotency'
            // V3 에서 이름이 바뀌었다. 옛 이름(uk_transfer_idempotency)은 새 이름의 부분 문자열이 아니라 함께 고쳐야 했다
            val message = e.mostSpecificCause.message.orEmpty()
            when {
                "uk_transfer_user_idempotency" in message -> throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
                "uk_transfer_reference" in message -> throw StoreConflict.DuplicateReference(transfer.reference)
                else -> throw e
            }
        } catch (e: PessimisticLockingFailureException) {
            // 드물고 운영자가 알아야 하는 일이라 WARN (부하 측정은 WARN 으로 돌리므로 요청마다 찍히는 로그는 두지 않는다)
            log.warn("wallet of {} is busy (lock wait timeout or deadlock), answering WALLET_BUSY: {}", userId, e.mostSpecificCause.message)
            throw StoreConflict.WalletBusy(userId)
        }
    }

    override fun findById(id: UUID): Transfer? = transfers.findById(id.toString()).orElse(null)?.toDomain()

    override fun findByIdempotencyKey(userId: String, key: String): Transfer? =
        transfers.findByUserIdAndIdempotencyKey(userId, key)?.toDomain()

    /**
     * 조건부 UPDATE 로만 상태를 바꾼다. TransferEntity.from() 으로 만든 엔티티를 save 하면 INSERT 가 되므로 쓰지 않는다.
     * 한 문장의 UPDATE 라 두 소비자가 동시에 와도 DB 가 한쪽만 1 건으로 통과시킨다.
     * 허용하는 전이는 도메인의 상태 표([TransferStatus.canMoveTo])를 따른다. 표에 없는 전이는 DB 에 가기 전에 거른다 (호출 쪽 버그).
     * FAILED 는 거부한다 — 환불과 한 트랜잭션인 [fail] 로만 간다 (P4-1).
     * SENDING 으로 가는 전이는 sending_at 을 바꾸는 쪽이라 [sentWithin] 과 함께 쓰지 않는다.
     */
    override fun transition(
        id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?, sentWithin: OpenEndRange<Instant>?,
    ): Boolean {
        require(to != TransferStatus.FAILED) { "use fail() for FAILED so the refund is never skipped" }
        require(from.canMoveTo(to)) { "illegal transition $from -> $to" }
        require(sentWithin == null || to != TransferStatus.SENDING) { "a transition to SENDING rewrites sending_at, it cannot be bounded by it" }
        return tx.execute { conditionalUpdate(id, from, to, at.toDbPrecision(), bankResponseCode, sentWithin) == 1 }!!
    }

    private fun conditionalUpdate(
        id: UUID, from: TransferStatus, to: TransferStatus, dbAt: Instant, code: String?, sentWithin: OpenEndRange<Instant>?,
    ): Int = when {
        to == TransferStatus.SENDING -> transfers.transitionToSending(id.toString(), from, to, dbAt, code)
        sentWithin == null -> transfers.transition(id.toString(), from, to, dbAt, code)
        else -> transfers.transitionIfSentWithin(
            id.toString(), from, to, dbAt, code, sentWithin.start.toDbPrecision(), sentWithin.endExclusive.toDbPrecision(),
        )
    }

    /** idx_transfer_status(status, sending_at) 를 탄다. */
    override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> =
        transfers.findByStatusAndSendingAtBeforeOrderBySendingAt(TransferStatus.SENDING, cutoff.toDbPrecision(), Limit.of(limit))
            .map { it.toDomain() }

    /** sending_at 조건은 원거래가 아직 처리 중일 수 있는 유예 기간이다. idx_transfer_status(status, sending_at) 를 탄다. */
    override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> =
        transfers.findByStatusAndInquiryAttemptsLessThanAndSendingAtBeforeOrderByUpdatedAt(
            TransferStatus.UNKNOWN, maxAttempts, sendingBefore.toDbPrecision(), Limit.of(limit),
        ).map { it.toDomain() }

    override fun recordInquiryAttempt(id: UUID, at: Instant): Boolean =
        tx.execute { transfers.incrementInquiryAttempts(id.toString(), TransferStatus.UNKNOWN, at.toDbPrecision()) == 1 }!!

    override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> =
        transfers.findRepublishable(TransferStatus.RECEIVED, updatedBefore.toDbPrecision(), Limit.of(limit)).map { it.toDomain() }

    /**
     * 조건부 UPDATE 와 아웃박스 INSERT 를 한 트랜잭션에 쓴다: INSERT 가 실패하면 횟수 증가도 롤백된다
     * (횟수만 늘고 메시지는 없는 상태가 생기면 한도를 헛되이 쓴다). UPDATE 가 0건이면 INSERT 하지 않는다.
     */
    override fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean =
        tx.execute {
            val dbAt = at.toDbPrecision()
            if (transfers.incrementRepublishCount(id.toString(), TransferStatus.RECEIVED, expectedCount, dbAt) != 1) {
                return@execute false
            }
            outbox.saveAndFlush(outboxRow(TransferEvent.TransferRequested(id, dbAt)))
            true
        }!!

    /**
     * 실패 확정과 환불을 한 트랜잭션에 쓴다 (P4-1). 조건부 전이가 1건일 때만 환불한다 → 컨슈머와 정리 작업이 겹쳐 두 번 불러도 환불은 한 번.
     * 환불 금액은 원장의 WITHDRAW 에서 읽는다 (없으면 = 차감한 적 없는 P3 시절 출금 → 상태만 바꾼다).
     * 환불이 실패하면(원장 UNIQUE 위반 등) 전이도 롤백된다 — "FAILED 인데 돈은 안 돌아온" 상태가 생기지 않는다.
     * [sentWithin] 조건도 같은 UPDATE 에 들어간다 — 0건이면 환불하지 않는다.
     */
    override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String, sentWithin: OpenEndRange<Instant>?): Boolean {
        require(from.canMoveTo(TransferStatus.FAILED)) { "illegal transition $from -> FAILED" }
        return tx.execute {
            val dbAt = at.toDbPrecision()
            if (conditionalUpdate(id, from, TransferStatus.FAILED, dbAt, bankResponseCode, sentWithin) != 1) {
                return@execute false
            }
            wallets.refund(id, dbAt)
            true
        }!!
    }

    /** 아웃박스 한 줄. 접수(saveWithEvent)와 재발행(republish)이 같은 모양을 쓰도록 한곳에서 만든다. */
    private fun outboxRow(event: TransferEvent) = OutboxEventEntity(
        aggregateId = event.transferId.toString(),
        eventType = event::class.simpleName!!,
        payload = payloadOf(event),
        createdAt = event.occurredAt.toDbPrecision(),
    )
}
