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
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.domain.Limit
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "transfer")
class TransferEntity(
    @Id @Column(columnDefinition = "char(36)") var id: String = "",
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
    var createdAt: Instant = Instant.EPOCH,
    var updatedAt: Instant = Instant.EPOCH,
    /** 낙관적 락. null 이면 Spring Data 가 새 엔티티로 보고 INSERT(persist) 한다. */
    @Version var version: Long? = null,
) {
    fun toDomain() = Transfer(
        id = UUID.fromString(id), idempotencyKey = idempotencyKey, reference = reference, requestHash = requestHash,
        bankCode = bankCode, accountNo = accountNo, amount = amount, memo = memo, status = status,
        bankResponseCode = bankResponseCode, sendingAt = sendingAt, inquiryAttempts = inquiryAttempts, createdAt = createdAt, updatedAt = updatedAt,
    )

    companion object {
        /**
         * 도메인 -> 새 엔티티 (INSERT 전용). version 이 null 이라 save 가 항상 persist 한다.
         * P3-2 의 상태 변경은 이 메서드로 만든 엔티티를 다시 save 하지 말고, 읽어 온 엔티티를 고치거나 조건부 UPDATE 를 쓴다.
         */
        fun from(t: Transfer) = TransferEntity(
            id = t.id.toString(), idempotencyKey = t.idempotencyKey, reference = t.reference, requestHash = t.requestHash,
            bankCode = t.bankCode, accountNo = t.accountNo, amount = t.amount, memo = t.memo, status = t.status,
            bankResponseCode = t.bankResponseCode, sendingAt = t.sendingAt?.toDbPrecision(), inquiryAttempts = t.inquiryAttempts,
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
    fun findByIdempotencyKey(idempotencyKey: String): TransferEntity?

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
 * 이체 저장소 (MySQL). saveWithEvent 는 transfer 와 outbox_event 를 한 트랜잭션에 쓴다:
 * 둘 다 커밋되거나 둘 다 롤백되므로 "이체는 있는데 처리 신호가 없는" 상태가 생기지 않는다.
 * UNIQUE 위반은 제약 이름을 보고 도메인 예외([StoreConflict])로 바꿔 던진다.
 *
 * 바깥 트랜잭션 안에서 [saveWithEvent] 를 부르면 안 된다. 이 클래스가 자기 트랜잭션을 열고 닫는 것에 기대기 때문이다:
 * UNIQUE 위반이면 이 트랜잭션이 롤백된 뒤에야 호출자(TransferService)가 이긴 쪽 건을 다시 읽을 수 있다.
 * 바깥 트랜잭션에 합류(REQUIRED)하면 롤백이 호출자 쪽으로 미뤄져 그 재조회가 깨진다.
 * 전파는 지금은 REQUIRED(기본값)로 두고, 호출자가 트랜잭션 밖에서 부른다는 전제를 이 문서로 지킨다.
 */
class JpaTransferStore(
    private val transfers: TransferJpaRepository,
    private val outbox: OutboxEventJpaRepository,
    private val tx: TransactionTemplate,
    private val payloadOf: (TransferEvent) -> String = ::outboxPayload,
) : TransferStore {

    override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer =
        try {
            tx.execute {
                // flush 로 UNIQUE 위반을 여기서 터뜨려야 아래 catch 에서 StoreConflict 로 바꿀 수 있다 (커밋 시점까지 미루지 않는다)
                val saved = transfers.saveAndFlush(TransferEntity.from(transfer))
                outbox.saveAndFlush(
                    OutboxEventEntity(
                        aggregateId = transfer.id.toString(),
                        eventType = event::class.simpleName!!,
                        payload = payloadOf(event),
                        createdAt = event.occurredAt.toDbPrecision(),
                    ),
                )
                saved.toDomain()
            }!!
        } catch (e: DataIntegrityViolationException) {
            // MySQL 메시지 예: Duplicate entry 'k1' for key 'transfer.uk_transfer_idempotency'
            val message = e.mostSpecificCause.message.orEmpty()
            when {
                "uk_transfer_idempotency" in message -> throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
                "uk_transfer_reference" in message -> throw StoreConflict.DuplicateReference(transfer.reference)
                else -> throw e
            }
        }

    override fun findById(id: UUID): Transfer? = transfers.findById(id.toString()).orElse(null)?.toDomain()

    override fun findByIdempotencyKey(key: String): Transfer? = transfers.findByIdempotencyKey(key)?.toDomain()

    /**
     * 조건부 UPDATE 로만 상태를 바꾼다. TransferEntity.from() 으로 만든 엔티티를 save 하면 INSERT 가 되므로 쓰지 않는다.
     * 한 문장의 UPDATE 라 두 소비자가 동시에 와도 DB 가 한쪽만 1 건으로 통과시킨다.
     * 허용하는 전이는 도메인의 상태 표([TransferStatus.canMoveTo])를 따른다. 표에 없는 전이는 DB 에 가기 전에 거른다 (호출 쪽 버그).
     */
    override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?): Boolean {
        require(from.canMoveTo(to)) { "illegal transition $from -> $to" }
        return tx.execute {
            val dbAt = at.toDbPrecision()
            val updated = if (to == TransferStatus.SENDING) {
                transfers.transitionToSending(id.toString(), from, to, dbAt, bankResponseCode)
            } else {
                transfers.transition(id.toString(), from, to, dbAt, bankResponseCode)
            }
            updated == 1
        }!!
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
}
