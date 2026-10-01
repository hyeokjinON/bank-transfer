package io.github.hyeokjinon.banktransfer.account

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture

enum class VerificationStatus { SENT, VERIFIED, EXPIRED, LOCKED }

/**
 * 1원 계좌인증 한 건.
 * 인증코드는 원문으로 저장하지 않고 해시만 보관한다(유출돼도 코드를 알 수 없게).
 */
data class AccountVerification(
    val id: UUID,
    val bankCode: String,
    val accountNo: String,
    val holderName: String,
    val codeHash: String,
    val status: VerificationStatus,
    val failedAttempts: Int,
    val expiresAt: Instant,
    val verifiedAt: Instant? = null,
)

data class RequestVerification(val bankCode: String, val accountNo: String, val holderName: String)

/**
 * 은행 연동 포트. account 모듈은 Netty·전문을 모른다.
 * 실제 구현(어댑터)은 apps/api 에서 bank-gateway 로 연결한다. 테스트에서는 가짜 구현을 쓴다.
 */
interface BankPort {
    /** 예금주명. 계좌가 없으면 null. */
    fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?>

    fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit>
}

interface VerificationRepository {
    fun save(verification: AccountVerification): AccountVerification

    fun findById(id: UUID): AccountVerification?

    /** 원자적으로 바꾼다. 같은 건에 확인 요청이 동시에 와도 시도 횟수가 꼬이지 않게 하려는 것이다. */
    fun update(id: UUID, change: (AccountVerification) -> AccountVerification): AccountVerification?
}

fun interface CodeGenerator {
    fun generate(): String
}

sealed class AccountVerificationException(message: String) : RuntimeException(message) {
    class AccountNotFound : AccountVerificationException("account not found")
    class HolderNameMismatch : AccountVerificationException("holder name does not match")
    class VerificationNotFound(id: UUID) : AccountVerificationException("verification $id not found")
    class Expired : AccountVerificationException("verification expired")
    class Locked : AccountVerificationException("too many failed attempts")
    class WrongCode(val remainingAttempts: Int) : AccountVerificationException("wrong code, $remainingAttempts attempts left")
}
