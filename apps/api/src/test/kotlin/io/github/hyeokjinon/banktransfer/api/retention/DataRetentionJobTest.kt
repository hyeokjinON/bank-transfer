package io.github.hyeokjinon.banktransfer.api.retention

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.job.Job
import org.springframework.batch.core.job.JobExecution
import org.springframework.batch.core.job.SimpleJob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.interceptor.TransactionAttribute
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** 보존 배치 잡 (P5-2, 설계 §4 · §6). 실제 Spring Batch · Testcontainers MySQL. */
class DataRetentionJobTest : RetentionTestSupport() {

    @Autowired @Qualifier(DataRetention.JOB_NAME) private lateinit var job: Job
    @Autowired private lateinit var verifications: VerificationRepository

    private fun start(day: LocalDate, target: Job = job): JobExecution = jobOperator.start(target, DataRetention.parameters(day))

    private fun stepOf(execution: JobExecution, name: String) = checkNotNull(jobRepository.getLastStepExecution(execution.jobInstance, name)) { "step $name is missing" }

    private fun postTransfer(user: String, account: String): Int =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 1_000, "memo" to "보존"))
            .exchange { _, response -> response.statusCode.value() }

    @Test
    fun `핵심 - expires_at 이 지난 VERIFIED 는 남고, 잡 뒤에도 그 사용자의 출금이 202 다`() {
        val day = RetentionTestDates.next()
        val user = fundedUser()
        val account = unregisteredAccount()   // RECEIVED 를 남긴다 — 나중에 처리돼도 공용 계좌에 입금이 생기지 않게
        val verified = verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), userId = user, bankCode = "090", accountNo = account, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = verificationCutoff(day).minus(Duration.ofDays(1)), verifiedAt = Instant.now(),
            ),
        )
        assertEquals(202, postTransfer(user, account))   // 잡 전에도 202 — 아래 202 가 다른 이유로 달라지지 않는지 먼저 본다

        assertEquals(BatchStatus.COMPLETED, start(day).status)

        assertNotNull(verifications.findById(verified.id))
        assertTrue(verifications.existsVerified(user, "090", account))
        assertEquals(202, postTransfer(user, account))
    }

    @Test
    fun `대상만 지운다 - 표마다 자기 보관 기간, 미발행 · 기준 뒤 · VERIFIED 는 남는다`() {
        val day = RetentionTestDates.next()
        val outboxCut = outboxCutoff(day)
        val verificationCut = verificationCutoff(day)
        val oldPublished = outboxRow(outboxCut.minusSeconds(1))
        val recentPublished = outboxRow(outboxCut)
        val oldUnpublished = outboxRow(publishedAt = null, createdAt = verificationCut.minus(Duration.ofDays(1)))
        val oldVerifications = listOf("SENT", "EXPIRED", "LOCKED").map { verificationRow(it, verificationCut.minusSeconds(1)) }
        val withinThirtyDays = verificationRow("SENT", outboxCut.minusSeconds(1))   // 7일은 지났지만 30일은 아니다 — 아웃박스 기간으로 지우면 여기서 잡힌다
        val oldVerified = verificationRow("VERIFIED", verificationCut.minus(Duration.ofDays(1)))

        val execution = start(day)

        assertEquals(BatchStatus.COMPLETED, execution.status)
        assertEquals(1L, stepOf(execution, DataRetention.PURGE_OUTBOX).writeCount)
        assertEquals(3L, stepOf(execution, DataRetention.PURGE_VERIFICATIONS).writeCount)
        assertFalse(outboxExists(oldPublished))
        assertTrue(outboxExists(recentPublished))
        assertTrue(outboxExists(oldUnpublished))
        oldVerifications.forEach { assertFalse(verificationExists(it)) }
        assertTrue(verificationExists(withinThirtyDays))
        assertTrue(verificationExists(oldVerified))
    }

    @Test
    fun `2,500행 + 묶음 1,000 → 3번 커밋 · 건수 2,500`() {
        val day = RetentionTestDates.next()
        assertEquals(1000, props.batchSize)                                  // 실제 잡 빈 · application.yml 값으로 잰다
        outboxRows(2_500, outboxCutoff(day).minusSeconds(1))

        val execution = start(day)

        val step = stepOf(execution, DataRetention.PURGE_OUTBOX)
        assertEquals(2_500L to 3L, step.writeCount to step.commitCount)
        val other = stepOf(execution, DataRetention.PURGE_VERIFICATIONS)
        assertEquals(0L to 1L, other.writeCount to other.commitCount)        // 지울 게 없어도 한 번은 부른다
    }

    @Test
    fun `지운 수가 묶음의 배수면 한 번 더 불러 0건으로 끝낸다`() {
        val day = RetentionTestDates.next()
        outboxRows(20, outboxCutoff(day).minusSeconds(1))
        val small = buildDataRetentionJob(
            jobRepository, transactionManager, props.copy(batchSize = 10), retention::deleteOutboxEvents, retention::deleteVerifications,
        )

        val execution = start(day, small)

        val step = stepOf(execution, DataRetention.PURGE_OUTBOX)
        assertEquals(20L to 3L, step.writeCount to step.commitCount)         // 10 · 10 · 0
    }

    @Test
    fun `단계 트랜잭션 - 두 단계 모두 JPA 트랜잭션 관리자 · READ COMMITTED · 모든 예외에 롤백`() {
        // Batch 6 은 트랜잭션 관리자를 빼먹어도 조용히 트랜잭션 없이 돈다 — 설정 자체를 고정한다 (계획 "버전 · API 확인 결과" 2 · 3 · 5). 필드 이름은 6.0.5 소스 기준
        assertIs<JpaTransactionManager>(transactionManager)
        for (name in listOf(DataRetention.PURGE_OUTBOX, DataRetention.PURGE_VERIFICATIONS)) {
            val step = checkNotNull((job as SimpleJob).getStep(name)) { "step $name is missing" }
            assertSame(transactionManager, ReflectionTestUtils.getField(step, "transactionManager"), name)
            val attribute = ReflectionTestUtils.getField(step, "transactionAttribute") as TransactionAttribute
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, attribute.isolationLevel, name)
            // rollbackOn 이 false 면 검사 예외를 삼키고 CONTINUABLE 로 끝없이 돈다
            assertTrue(attribute.rollbackOn(Exception("checked")), name)
        }
    }
}
