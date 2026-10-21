package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 결과를 모르는 출금을 결과조회로 확정하는 흐름 (P3-3a). 은행에는 다시 보내지 않는다.
 * 모의 은행: 9990000000002 = 처리하고 응답만 안 함, 9990000000001 = 처리도 응답도 안 함.
 */
class ReconcileFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var reconciler: TransferReconciler

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun verify(accountNo: String) {
        verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
    }

    private fun request(accountNo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to "R" + Random.nextInt(100_000, 999_999)))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun awaitStatus(id: UUID, expected: TransferStatus) {
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(expected, store.findById(id)!!.status)
        }
    }

    @Test
    fun `처리하고 응답만 안 한 은행 - UNKNOWN 뒤 결과조회로 COMPLETED, 은행 입금은 1건`() {
        val account = "9990000000002"
        verify(account)
        val id = request(account)
        awaitStatus(id, TransferStatus.UNKNOWN)

        reconciler.reconcile()

        val after = store.findById(id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals("0000", after.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })   // 다시 보내지 않았다
    }

    @Test
    fun `처리도 응답도 안 한 은행 - UNKNOWN 뒤 결과조회로 FAILED 2001`() {
        val account = "9990000000001"
        verify(account)
        val id = request(account)
        awaitStatus(id, TransferStatus.UNKNOWN)

        reconciler.reconcile()

        assertEquals(TransferStatus.FAILED, store.findById(id)!!.status)
        assertEquals("2001", store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `30초 넘게 SENDING 에 멈춘 건 - UNKNOWN 으로 바꾸고 결과조회로 확정`() {
        // 컨슈머가 SENDING 커밋 뒤 은행에 보내기 전에 죽은 상황: 은행은 이 거래번호를 모른다
        val now = Instant.now()
        val transfer = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = RandomReferenceGenerator().next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "9990000000001", amount = 1_000, memo = "stuck",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, now))
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(60))

        val result = reconciler.reconcile()

        assertEquals(true, result.markedUnknown >= 1)
        assertEquals(TransferStatus.FAILED, store.findById(transfer.id)!!.status)
        assertEquals("2001", store.findById(transfer.id)!!.bankResponseCode)
    }
}
