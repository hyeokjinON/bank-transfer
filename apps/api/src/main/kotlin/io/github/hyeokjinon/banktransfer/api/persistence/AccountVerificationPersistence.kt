package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.LockModeType
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/** DATETIME(6) 은 마이크로초까지만 담는다. 저장 전에 맞춰 잘라야 저장한 값과 다시 읽은 값이 같다. */
internal fun Instant.toDbPrecision(): Instant = truncatedTo(ChronoUnit.MICROS)

@Entity
@Table(name = "account_verification")
class AccountVerificationEntity(
    @Id @Column(columnDefinition = "char(36)") var id: String = "",
    var bankCode: String = "",
    var accountNo: String = "",
    var holderName: String = "",
    @Column(columnDefinition = "char(64)") var codeHash: String = "",
    @Enumerated(EnumType.STRING) var status: VerificationStatus = VerificationStatus.SENT,
    var failedAttempts: Int = 0,
    var expiresAt: Instant = Instant.EPOCH,
    var verifiedAt: Instant? = null,
    var createdAt: Instant = Instant.EPOCH,
) {
    fun toDomain() = AccountVerification(
        id = UUID.fromString(id), bankCode = bankCode, accountNo = accountNo, holderName = holderName,
        codeHash = codeHash, status = status, failedAttempts = failedAttempts, expiresAt = expiresAt, verifiedAt = verifiedAt,
    )

    /** 바뀔 수 있는 값만 옮긴다. id · 계좌 · 코드 해시는 만들 때 정해지고 바뀌지 않는다. */
    fun applyChanges(v: AccountVerification) {
        status = v.status
        failedAttempts = v.failedAttempts
        expiresAt = v.expiresAt.toDbPrecision()
        verifiedAt = v.verifiedAt?.toDbPrecision()
    }

    companion object {
        fun from(v: AccountVerification, createdAt: Instant) = AccountVerificationEntity(
            id = v.id.toString(), bankCode = v.bankCode, accountNo = v.accountNo, holderName = v.holderName,
            codeHash = v.codeHash, status = v.status, failedAttempts = v.failedAttempts,
            expiresAt = v.expiresAt.toDbPrecision(), verifiedAt = v.verifiedAt?.toDbPrecision(),
            createdAt = createdAt.toDbPrecision(),
        )
    }
}

interface AccountVerificationJpaRepository : JpaRepository<AccountVerificationEntity, String> {

    /** SELECT … FOR UPDATE. 트랜잭션이 끝날 때까지 다른 요청은 이 행을 기다린다. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select v from AccountVerificationEntity v where v.id = :id")
    fun findForUpdate(@Param("id") id: String): AccountVerificationEntity?

    fun existsByBankCodeAndAccountNoAndStatus(bankCode: String, accountNo: String, status: VerificationStatus): Boolean
}

/**
 * 1원인증 저장소 (MySQL). update 는 행을 FOR UPDATE 로 잠근 채 읽고-바꾸고-쓴다:
 * 같은 건에 확인 요청이 동시에 와도 한 요청씩 순서대로 시도 횟수를 갱신한다 (메모리 저장소의 computeIfPresent 와 같은 보장).
 */
class JpaVerificationRepository(
    private val jpa: AccountVerificationJpaRepository,
    private val tx: TransactionTemplate,
    private val clock: Clock,
) : VerificationRepository {

    override fun save(verification: AccountVerification): AccountVerification =
        tx.execute { jpa.save(AccountVerificationEntity.from(verification, clock.instant())).toDomain() }!!

    override fun findById(id: UUID): AccountVerification? = jpa.findById(id.toString()).orElse(null)?.toDomain()

    override fun update(id: UUID, change: (AccountVerification) -> AccountVerification): AccountVerification? =
        tx.execute {
            val entity = jpa.findForUpdate(id.toString()) ?: return@execute null
            entity.applyChanges(change(entity.toDomain()))
            entity.toDomain()   // 커밋 때 변경 감지(dirty checking)로 UPDATE 된다
        }

    override fun existsVerified(bankCode: String, accountNo: String): Boolean =
        jpa.existsByBankCodeAndAccountNoAndStatus(bankCode, accountNo, VerificationStatus.VERIFIED)
}
