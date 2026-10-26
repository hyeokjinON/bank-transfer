package io.github.hyeokjinon.banktransfer.account

import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.util.HexFormat
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * 1원 계좌인증 (점유인증).
 *
 * 1. 성명조회로 예금주명이 입력과 같은지 확인한다.
 * 2. 인증코드를 적요에 실어 1원을 보낸다. 사용자는 자기 통장 거래내역에서 코드를 확인한다.
 * 3. 사용자가 코드를 입력하면 확인한다. 유효시간과 실패 횟수 제한이 있다.
 *
 * 은행 호출은 CompletableFuture 로 이어 붙여서, 기다리는 동안 호출 스레드를 붙잡지 않는다.
 */
class AccountVerificationService(
    private val bank: BankPort,
    private val repository: VerificationRepository,
    private val codeGenerator: CodeGenerator = RandomCodeGenerator(),
    private val clock: Clock = Clock.systemUTC(),
    private val ttl: Duration = Duration.ofMinutes(5),
    private val maxAttempts: Int = 5,
    private val memoPrefix: String = "BT",
) {

    fun request(command: RequestVerification): CompletableFuture<AccountVerification> =
        bank.inquireHolderName(command.bankCode, command.accountNo).thenCompose { holderName ->
            when {
                holderName == null ->
                    CompletableFuture.failedFuture(AccountVerificationException.AccountNotFound())
                normalize(holderName) != normalize(command.holderName) ->
                    CompletableFuture.failedFuture(AccountVerificationException.HolderNameMismatch())
                else -> sendOneWon(command, holderName)
            }
        }

    /**
     * 코드 확인. 저장된 사용자와 [userId] 가 다르면 없는 건과 같다(VerificationNotFound) — 남의 인증을 확인하거나
     * 남의 실패 횟수를 써 버려 잠기게 하지 못하게 (P4-1). 판단은 저장소의 원자적 update 안에서 한다.
     */
    fun confirm(userId: String, id: UUID, code: String): AccountVerification {
        val now = clock.instant()
        val updated = repository.update(id) { v ->
            when {
                v.userId != userId -> v   // 남의 건: 아무것도 바꾸지 않는다
                v.status != VerificationStatus.SENT -> v
                !now.isBefore(v.expiresAt) -> v.copy(status = VerificationStatus.EXPIRED)
                hash(v.id, code) == v.codeHash -> v.copy(status = VerificationStatus.VERIFIED, verifiedAt = now)
                else -> {
                    val failed = v.failedAttempts + 1
                    v.copy(
                        failedAttempts = failed,
                        status = if (failed >= maxAttempts) VerificationStatus.LOCKED else VerificationStatus.SENT,
                    )
                }
            }
        } ?: throw AccountVerificationException.VerificationNotFound(id)
        if (updated.userId != userId) throw AccountVerificationException.VerificationNotFound(id)

        return when (updated.status) {
            VerificationStatus.VERIFIED -> updated
            VerificationStatus.EXPIRED -> throw AccountVerificationException.Expired()
            VerificationStatus.LOCKED -> throw AccountVerificationException.Locked()
            VerificationStatus.SENT -> throw AccountVerificationException.WrongCode(maxAttempts - updated.failedAttempts)
        }
    }

    private fun sendOneWon(command: RequestVerification, holderName: String): CompletableFuture<AccountVerification> {
        val id = UUID.randomUUID()
        val code = codeGenerator.generate()
        return bank.transfer(command.bankCode, command.accountNo, 1, memoPrefix + code).thenApply {
            repository.save(
                AccountVerification(
                    id = id,
                    userId = command.userId,
                    bankCode = command.bankCode,
                    accountNo = command.accountNo,
                    holderName = holderName,
                    codeHash = hash(id, code),
                    status = VerificationStatus.SENT,
                    failedAttempts = 0,
                    expiresAt = clock.instant().plus(ttl),
                ),
            )
        }
    }

    private fun normalize(name: String) = name.filterNot { it.isWhitespace() }

    private fun hash(id: UUID, code: String): String =
        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("$id:$code".toByteArray()))
}

/** 4자리 숫자. 예측할 수 없도록 SecureRandom 을 쓴다. */
class RandomCodeGenerator(private val random: SecureRandom = SecureRandom()) : CodeGenerator {
    override fun generate(): String = "%04d".format(random.nextInt(10_000))
}

/** 도메인 테스트용 저장소. 앱은 MySQL 저장소(apps/api JpaVerificationRepository)를 쓴다. */
class InMemoryVerificationRepository : VerificationRepository {
    private val store = ConcurrentHashMap<UUID, AccountVerification>()

    override fun save(verification: AccountVerification): AccountVerification =
        verification.also { store[it.id] = it }

    override fun findById(id: UUID): AccountVerification? = store[id]

    override fun update(id: UUID, change: (AccountVerification) -> AccountVerification): AccountVerification? =
        store.computeIfPresent(id) { _, current -> change(current) }

    override fun existsVerified(userId: String, bankCode: String, accountNo: String): Boolean =
        store.values.any {
            it.userId == userId && it.bankCode == bankCode && it.accountNo == accountNo && it.status == VerificationStatus.VERIFIED
        }
}
