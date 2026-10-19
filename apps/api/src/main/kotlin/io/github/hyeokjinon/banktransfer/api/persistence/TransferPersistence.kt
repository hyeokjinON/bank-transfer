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
import org.springframework.data.jpa.repository.JpaRepository
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
        bankResponseCode = bankResponseCode, sendingAt = sendingAt, createdAt = createdAt, updatedAt = updatedAt,
    )

    companion object {
        fun from(t: Transfer) = TransferEntity(
            id = t.id.toString(), idempotencyKey = t.idempotencyKey, reference = t.reference, requestHash = t.requestHash,
            bankCode = t.bankCode, accountNo = t.accountNo, amount = t.amount, memo = t.memo, status = t.status,
            bankResponseCode = t.bankResponseCode, sendingAt = t.sendingAt?.toDbPrecision(),
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
}
