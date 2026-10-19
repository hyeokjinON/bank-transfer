package io.github.hyeokjinon.banktransfer.transfer

import java.time.Instant
import java.util.UUID

/** 1회 출금 한도 (원). */
const val MAX_TRANSFER_AMOUNT: Long = 10_000_000

/**
 * 출금이체 상태. 허용 전이는 설계 문서 3장과 같다.
 * 타임아웃은 실패가 아니라 UNKNOWN 이다: 은행이 처리했을 수 있으므로 다시 보내지 않고 결과조회로 확정한다(P3-3).
 */
enum class TransferStatus {
    RECEIVED, SENDING, COMPLETED, FAILED, UNKNOWN;

    val isFinal: Boolean get() = this == COMPLETED || this == FAILED

    fun canMoveTo(next: TransferStatus): Boolean = next in allowedNext()

    private fun allowedNext(): Set<TransferStatus> = when (this) {
        RECEIVED -> setOf(SENDING)
        // SENDING -> RECEIVED 는 "전문을 쓰기도 전에 연결이 없었다"(은행에 안 감)일 때만 쓴다 (P3-3 재시도)
        SENDING -> setOf(RECEIVED, COMPLETED, FAILED, UNKNOWN)
        UNKNOWN -> setOf(COMPLETED, FAILED)
        COMPLETED, FAILED -> emptySet()
    }
}

/** 출금이체 한 건. 서비스 모계좌 -> 1원인증을 마친 사용자 계좌. */
data class Transfer(
    val id: UUID,
    val idempotencyKey: String,
    /** 이용기관 거래번호 20자. 은행에 보내는 업무 키 (전문번호와 다르다). */
    val reference: String,
    /** 같은 키로 다른 내용을 보냈는지 판단하는 해시. */
    val requestHash: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
    val status: TransferStatus,
    val bankResponseCode: String? = null,
    val sendingAt: Instant? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    fun moveTo(next: TransferStatus, at: Instant): Transfer {
        if (!status.canMoveTo(next)) throw TransferException.IllegalTransition(status, next)
        return copy(status = next, updatedAt = at, sendingAt = if (next == TransferStatus.SENDING) at else sendingAt)
    }
}

data class RequestTransfer(
    val idempotencyKey: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
)

/** 접수 결과. created = false 면 같은 키로 이미 접수된 건을 그대로 돌려준 것이다. */
data class TransferResult(val transfer: Transfer, val created: Boolean)

sealed interface TransferEvent {
    val transferId: UUID
    val occurredAt: Instant

    data class TransferRequested(override val transferId: UUID, override val occurredAt: Instant) : TransferEvent
}

/**
 * 이체 저장 포트. 도메인은 "저장과 이벤트 기록은 함께 성공하거나 함께 실패한다"는 계약만 안다.
 * 구현(같은 DB 트랜잭션 + 아웃박스 테이블)은 apps/api 의 어댑터가 맡는다.
 */
interface TransferStore {
    /** UNIQUE 위반이면 [StoreConflict] 를 던진다. */
    fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer

    fun findById(id: UUID): Transfer?

    fun findByIdempotencyKey(key: String): Transfer?
}

/** 저장소가 UNIQUE 제약 위반을 도메인 말로 알려 줄 때 쓴다. */
sealed class StoreConflict(message: String) : RuntimeException(message) {
    class DuplicateIdempotencyKey(key: String) : StoreConflict("idempotency key '$key' already exists")
    class DuplicateReference(reference: String) : StoreConflict("reference '$reference' already exists")
}

/** "이 계좌는 1원인증을 마쳤나?" transfer 모듈은 account 모듈을 import 하지 않는다. 연결은 apps/api 에서 한다. */
fun interface VerifiedAccounts {
    fun isVerified(bankCode: String, accountNo: String): Boolean
}

fun interface ReferenceGenerator {
    fun next(): String
}

sealed class TransferException(message: String) : RuntimeException(message) {
    class IdempotencyKeyRequired : TransferException("Idempotency-Key header is required (1-64 chars)")
    class InvalidAmount(amount: Long) : TransferException("amount must be 1..$MAX_TRANSFER_AMOUNT, was $amount")
    class InvalidMemo : TransferException("memo must fit in 20 bytes of EUC-KR")
    class AccountNotVerified : TransferException("account is not verified")
    class IdempotencyKeyReused : TransferException("idempotency key was already used with a different request")
    class TransferNotFound(id: UUID) : TransferException("transfer $id not found")
    class IllegalTransition(from: TransferStatus, to: TransferStatus) : TransferException("cannot move from $from to $to")
}
