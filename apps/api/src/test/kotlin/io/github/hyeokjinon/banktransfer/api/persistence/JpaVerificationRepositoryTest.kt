package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JpaVerificationRepositoryTest : ApiIntegrationTest() {

    @Autowired
    private lateinit var repository: VerificationRepository

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    private fun newVerification(
        accountNo: String = "77" + Random.nextLong(10_000_000_000, 99_999_999_999),
        expiresAt: Instant = Instant.parse("2026-10-19T00:05:00.123456789Z"),
    ) = AccountVerification(
        id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
        codeHash = "a".repeat(64), status = VerificationStatus.SENT, failedAttempts = 0, expiresAt = expiresAt,
    )

    @Test
    fun `저장한 값과 다시 읽은 값이 같다 (마이크로초 단위)`() {
        val saved = repository.save(newVerification())

        assertEquals(saved, repository.findById(saved.id))
        assertEquals(Instant.parse("2026-10-19T00:05:00.123456Z"), saved.expiresAt)   // DATETIME(6) 에 맞춰 자른다
    }

    @Test
    fun `시각은 KST 벽시계로 저장된다`() {
        val saved = repository.save(newVerification(expiresAt = Instant.parse("2026-10-19T00:00:00Z")))

        val raw = jdbc.queryForObject(
            "SELECT DATE_FORMAT(expires_at, '%Y-%m-%d %H:%i:%s') FROM account_verification WHERE id = ?",
            String::class.java, saved.id.toString(),
        )
        assertEquals("2026-10-19 09:00:00", raw)
    }

    @Test
    fun `같은 건을 동시에 update 해도 시도 횟수가 정확하다 (FOR UPDATE)`() {
        val saved = repository.save(newVerification())
        val pool = Executors.newFixedThreadPool(10)
        val start = CountDownLatch(1)
        repeat(10) {
            pool.execute {
                start.await()
                repository.update(saved.id) { it.copy(failedAttempts = it.failedAttempts + 1) }
            }
        }
        start.countDown()
        pool.shutdown()
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))

        assertEquals(10, repository.findById(saved.id)!!.failedAttempts)
    }

    @Test
    fun `VERIFIED 가 된 계좌만 existsVerified`() {
        val saved = repository.save(newVerification())
        assertFalse(repository.existsVerified("090", saved.accountNo))

        repository.update(saved.id) { it.copy(status = VerificationStatus.VERIFIED, verifiedAt = Instant.now()) }

        assertTrue(repository.existsVerified("090", saved.accountNo))
        assertFalse(repository.existsVerified("004", saved.accountNo))
    }

    @Test
    fun `없는 id 는 update 가 null`() {
        assertEquals(null, repository.update(UUID.randomUUID()) { it })
    }
}
