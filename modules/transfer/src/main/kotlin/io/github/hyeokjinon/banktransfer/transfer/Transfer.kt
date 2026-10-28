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
        // RECEIVED -> FAILED 는 정리 작업이 재발행 한도를 다 쓰고 포기할 때만 쓴다 (은행에 간 적 없음, E001 — P3-3b)
        RECEIVED -> setOf(SENDING, FAILED)
        // SENDING -> RECEIVED 는 "전문을 쓰기도 전에 연결이 없었다"(은행에 안 감)일 때만 쓴다 (P3-3 재시도)
        SENDING -> setOf(RECEIVED, COMPLETED, FAILED, UNKNOWN)
        UNKNOWN -> setOf(COMPLETED, FAILED)
        COMPLETED, FAILED -> emptySet()
    }
}

/** 출금이체 한 건. 서비스 모계좌 -> 1원인증을 마친 사용자 계좌. */
data class Transfer(
    val id: UUID,
    /** 출금한 사용자 (P4-1, `X-User-Id`). P3 시절 행은 null — 어떤 사용자에게도 보이지 않고 잔고와도 무관하다(차감한 적이 없다). */
    val userId: String? = null,
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
    /** 결과조회를 시도했는데 은행도 답하지 않은 횟수 (P3-3a). 한도에 닿으면 정리 작업이 멈춘다. */
    val inquiryAttempts: Int = 0,
    /** 정리 작업이 아웃박스로 다시 발행한 횟수 (P3-3b). 한도를 다 쓰고도 은행에 못 보내면 FAILED(E001). */
    val republishCount: Int = 0,
    /** 마지막 재발행 시각. 그 뒤에 sendingAt 이 있어야(은행에 보내려다 못 보냈어야) 다음 재발행을 한다. */
    val republishedAt: Instant? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    fun moveTo(next: TransferStatus, at: Instant): Transfer {
        if (!status.canMoveTo(next)) throw TransferException.IllegalTransition(status, next)
        return copy(status = next, updatedAt = at, sendingAt = if (next == TransferStatus.SENDING) at else sendingAt)
    }

    /** 로그에 계좌번호 · 적요가 그대로 남지 않게 한다 (data class 기본 toString 은 모든 필드를 찍는다). */
    override fun toString(): String =
        "Transfer(id=$id, userId=$userId, reference=$reference, status=$status, bankCode=$bankCode, accountNo=${maskAccountNo(accountNo)}, " +
            "amount=$amount, bankResponseCode=$bankResponseCode)"
}

/** 뒤 4자리만 남긴다: 1002123456789 -> *********6789 */
internal fun maskAccountNo(accountNo: String): String =
    if (accountNo.length <= 4) "*".repeat(accountNo.length) else "*".repeat(accountNo.length - 4) + accountNo.takeLast(4)

/**
 * 은행 이체 결과. "모름"과 "안 보냄"을 구분하는 게 핵심이다:
 * 모르면 은행이 처리했을 수 있으므로 다시 보내지 않고, 안 보냈으면 다시 보내도 된다.
 */
sealed interface BankTransferOutcome {
    /** 은행이 처리했다 (0000). */
    data object Completed : BankTransferOutcome

    /** 은행이 거절했다 (계좌 없음 등). 확정 실패. */
    data class Rejected(val responseCode: String) : BankTransferOutcome

    /** 보냈는데 결과를 모른다 (무응답 · 보낸 뒤 연결 끊김 · 쓰기 실패). */
    data object ResultUnknown : BankTransferOutcome

    /** 보내기 전에 실패했다 (연결 없음). 은행에 가지 않았다. */
    data object NotSent : BankTransferOutcome
}

/** 은행 출금이체 포트. 결과가 나올 때까지 호출 스레드에서 기다린다 (Kafka 컨슈머 스레드가 부른다). */
fun interface BankTransferPort {
    /** [reference] = 이용기관 거래번호. 결과를 모를 때 이 번호로 결과조회한다. */
    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome
}

/** 결과조회 결과. 처리됨 · 처리 안 됨(확정) · 모름(조회에도 답이 없음 등). */
sealed interface InquiryOutcome {
    data object Processed : InquiryOutcome

    /** 은행에 원거래가 없다 = 처리되지 않았다. [responseCode] 는 은행 처리결과 코드 (예: 2001). */
    data class NotProcessed(val responseCode: String) : InquiryOutcome

    data object Unknown : InquiryOutcome

    /** 은행 연결이 없어 조회 전문을 보내지도 못했다 (P3-3b). 은행이 답하지 않은 게 아니므로 시도로 세지 않는다. */
    data object Unavailable : InquiryOutcome
}

/** 은행 결과조회 포트. 이체를 다시 보내지 않고 "처리했나?"만 묻는다. */
fun interface BankInquiryPort {
    fun inquire(reference: String): InquiryOutcome
}

data class RequestTransfer(
    /** 요청한 사용자. 웹 계층이 X-User-Id 에서 꺼내 형식을 검사한 값이다 (인증은 범위 밖, P4-1). */
    val userId: String,
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
    /**
     * 이체 저장 · 잔고 차감 · 이벤트 기록은 함께 성공하거나 함께 실패한다 (P4-1: 차감 추가).
     * [transfer] 의 userId 지갑에서 amount 를 뺀다 — 잔고가 모자라면(지갑이 없어도 잔고 0) [StoreConflict.InsufficientBalance] 이고 아무것도 남지 않는다.
     * 잔고를 지금 잡지 못하면 [StoreConflict.WalletBusy], 동시 변경이 이어져 포기하면 [StoreConflict.ConcurrentUpdate] (P4-2) — 둘 다 아무것도 남지 않는다.
     * UNIQUE 위반이면 [StoreConflict] 를 던진다.
     */
    fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer

    fun findById(id: UUID): Transfer?

    /** 멱등키는 사용자 범위다 (P4-1): 다른 사용자의 같은 키는 다른 출금이다. */
    fun findByIdempotencyKey(userId: String, key: String): Transfer?

    /**
     * 현재 상태가 [from] 일 때만 [to] 로 바꾼다 (조건부 UPDATE). 바뀌었으면 true.
     * 같은 메시지를 두 번 받아도 두 번째는 false 라서 은행에 다시 보내지 않는다 — 도메인 상태가 곧 멱등키다.
     * [to] 가 SENDING 이면 sendingAt 도 [at] 으로 기록한다. bankResponseCode 는 주어진 값으로 덮어쓴다.
     * [to] 가 FAILED 면 거부한다(IllegalArgumentException) — 실패 확정은 환불과 한 트랜잭션인 [fail] 로만 한다 (P4-1).
     */
    fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String? = null): Boolean

    /** status = SENDING 이고 sendingAt 이 [cutoff] 보다 이른 건 (오래된 순, 최대 [limit]). */
    fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer>

    /**
     * status = UNKNOWN 이고 결과조회 시도가 [maxAttempts] 미만이며 sendingAt 이 [sendingBefore] 보다 이른 건
     * (오래 기다린 순, 최대 [limit]).
     * sendingAt 조건이 유예 기간이다: 은행이 조금 느릴 뿐 원거래를 아직 처리 중일 수 있어, 그때 물으면 "처리 안 됨(2001)" 이
     * 돌아와도 믿을 수 없다. 컨슈머가 타임아웃(3초)으로 UNKNOWN 을 적은 건도 같은 유예를 거친다.
     */
    fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer>

    /** UNKNOWN 인 동안에만 결과조회 시도 횟수를 1 늘린다. 늘렸으면 true. */
    fun recordInquiryAttempt(id: UUID, at: Instant): Boolean

    /**
     * 재발행 대상 (P3-3b): status = RECEIVED, updatedAt 이 [updatedBefore] 보다 이르고(재시도 토픽 · DLT 가 끝난 뒤),
     * sendingAt 이 있고 마지막 재발행(republishedAt) 이후인 건 — 은행에 보내려다 못 보낸 흔적이 있는 것만 (오래된 순, 최대 [limit]).
     * Kafka · 컨슈머 장애로 시도조차 못 한 건은 고르지 않는다: 복구되면 밀린 메시지가 처리되고, 횟수를 쓰면 실패로 잘못 확정된다.
     */
    fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer>

    /**
     * RECEIVED 이고 republishCount 가 [expectedCount] 일 때만 횟수 +1, republishedAt · updatedAt = [at] 으로 바꾸고
     * 같은 트랜잭션에서 TransferRequested 이벤트를 하나 더 기록한다 (발행 경로는 아웃박스 하나). 바꿨으면 true.
     * 0건(다른 쪽이 먼저 바꿈)이면 이벤트도 남기지 않는다.
     */
    fun republish(id: UUID, expectedCount: Int, at: Instant): Boolean

    /**
     * 실패 확정 (P4-1). 현재 상태가 [from] 일 때만 FAILED([bankResponseCode]) 로 바꾸고, 같은 트랜잭션에서 차감했던 금액을 지갑에 돌려주며
     * 원장에 REFUND 를 남긴다. 바꿨으면 true. 0건(다른 쪽이 먼저 확정)이면 환불도 하지 않는다 → 두 번 불러도 환불은 한 번.
     * 차감 기록(WITHDRAW)이 없는 P3 시절 출금은 상태만 바꾼다. FAILED 로 가는 길은 이것 하나다 — 새 실패 경로가 생겨도 환불을 빠뜨릴 수 없게.
     */
    fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String): Boolean
}

/** 저장소가 UNIQUE 제약 위반을 도메인 말로 알려 줄 때 쓴다. */
sealed class StoreConflict(message: String) : RuntimeException(message) {
    class DuplicateIdempotencyKey(key: String) : StoreConflict("idempotency key '$key' already exists")
    class DuplicateReference(reference: String) : StoreConflict("reference '$reference' already exists")

    /** 잔고가 출금액보다 적다 (지갑이 없으면 잔고 0). 저장소는 아무것도 남기지 않고 롤백한다 (P4-1). */
    class InsufficientBalance(userId: String, amount: Long) : StoreConflict("balance of '$userId' is less than $amount")

    /**
     * 지금 잔고를 잡지 못했다 — 락 대기 한도 초과 · 데드락 · 분산락 대기 초과 · 분산락 저장소 장애 (P4-2).
     * 어떤 락인지는 저장소만 안다. 아무것도 남기지 않았고, 잠시 뒤 다시 하면 될 수 있다.
     */
    class WalletBusy(userId: String) : StoreConflict("wallet of '$userId' is busy, try again later")

    /** 다른 변경과 계속 부딪혀 저장소가 정한 횟수만큼 다시 하고도 포기했다 (P4-2, 낙관적 락). 아무것도 남기지 않았다. */
    class ConcurrentUpdate(userId: String) : StoreConflict("wallet of '$userId' kept changing concurrently, try again later")
}

/**
 * "이 사용자가 이 계좌의 1원인증을 마쳤나?" (P4-1: 사용자 범위 — 남이 인증한 계좌로는 출금할 수 없다).
 * transfer 모듈은 account 모듈을 import 하지 않는다. 연결은 apps/api 에서 한다.
 */
fun interface VerifiedAccounts {
    fun isVerified(userId: String, bankCode: String, accountNo: String): Boolean
}

fun interface ReferenceGenerator {
    fun next(): String
}

sealed class TransferException(message: String) : RuntimeException(message) {
    class IdempotencyKeyRequired : TransferException("Idempotency-Key header is required (1-64 visible ASCII chars)")
    class InvalidAmount(amount: Long) : TransferException("amount must be 1..$MAX_TRANSFER_AMOUNT, was $amount")
    class InvalidMemo : TransferException("memo must fit in 20 bytes of EUC-KR")
    class InvalidAccount : TransferException("bankCode must be 3 digits and accountNo 10-16 digits")
    class AccountNotVerified : TransferException("account is not verified")
    class InsufficientBalance : TransferException("wallet balance is not enough for this transfer")

    /** 잔고를 지금 잡지 못했다. 요청은 옳다 — 잠시 뒤 다시 보내면 된다 (P4-2). */
    class WalletBusy : TransferException("wallet is busy, retry after a moment")

    /** 동시 변경이 이어져 포기했다. 요청은 옳다 — 잠시 뒤 다시 보내면 된다 (P4-2). */
    class ConcurrentUpdate : TransferException("wallet was updated concurrently, retry after a moment")
    class IdempotencyKeyReused : TransferException("idempotency key was already used with a different request")
    class TransferNotFound(id: UUID) : TransferException("transfer $id not found")
    class IllegalTransition(from: TransferStatus, to: TransferStatus) : TransferException("cannot move from $from to $to")
}
