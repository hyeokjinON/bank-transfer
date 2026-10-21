package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
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
import java.time.Clock
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

    /**
     * 시계를 60초 앞으로 돌린 정리 작업. 컨슈머가 방금 적은 UNKNOWN 은 30초 유예 안쪽이라 진짜 시계로는 묻지 않으므로,
     * 30초를 실제로 기다리는 대신 시계만 앞당겨 "유예가 지난 뒤" 를 만든다. 나머지는 [reconciler] 와 같은 구성이다.
     */
    private val reconcilerAfterGrace by lazy {
        TransferReconciler(store, BankInquiryAdapter(gateway), Clock.offset(Clock.systemUTC(), Duration.ofSeconds(60)))
    }

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

        reconcilerAfterGrace.reconcile()

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

        reconcilerAfterGrace.reconcile()

        assertEquals(TransferStatus.FAILED, store.findById(id)!!.status)
        assertEquals("2001", store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `방금 UNKNOWN 이 된 건은 30초 유예 안에서는 묻지 않는다 - 느린 은행의 2001 로 FAILED 가 되지 않게`() {
        // 회귀 방지: 유예가 없으면 컨슈머가 타임아웃으로 적은 UNKNOWN 을 다음 주기에 바로 물어,
        // 아직 처리 중인 은행이 2001 을 답하면 돈이 나가는 중인 건이 FAILED 로 확정된다
        val account = "9990000000002"
        verify(account)
        val id = request(account)
        awaitStatus(id, TransferStatus.UNKNOWN)

        reconciler.reconcile()   // 진짜 시계

        val fresh = store.findById(id)!!
        assertEquals(TransferStatus.UNKNOWN, fresh.status)
        assertEquals(0, fresh.inquiryAttempts)

        reconcilerAfterGrace.reconcile()   // 유예가 지난 뒤에는 묻는다

        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
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
