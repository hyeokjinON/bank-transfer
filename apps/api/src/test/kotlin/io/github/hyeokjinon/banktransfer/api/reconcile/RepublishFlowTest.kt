package io.github.hyeokjinon.banktransfer.api.reconcile

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.core.ConsumerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 은행에 보내지 못한 출금의 끝: DLT 뒤 정리 작업이 아웃박스로 다시 발행하고, 3번을 다 쓰면 FAILED E001 (P3-3b).
 * 모의 은행 1002123456789 = 정상 계좌. 은행 장애는 withBankDown (pause/resume).
 */
class RepublishFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired private lateinit var props: ReconcileProperties

    private val abandoned = CopyOnWriteArrayList<Transfer>()

    /**
     * 재발행 대기만 1ms 로 줄인 정리 작업 (5분을 기다리는 대신). 나머지는 운영 구성과 같다.
     * 시계를 앞당기지 않는 이유: republished_at 이 미래 시각으로 적히면 컨슈머가 실제 시계로 적는 sending_at 이 그보다 이르게 되어
     * 2 · 3회차의 재발행 기준(sending_at > republished_at)이 깨진다. 그래서 각 회차는 DLT 도착을 기다린 뒤에 돌린다.
     * 배치 크기를 키운 이유: 1ms 면 다른 테스트 클래스가 남긴 RECEIVED 도 후보라, 기본 100건 배치에서 대상 이체가 밀려날 수 있다.
     */
    private val reconciler by lazy {
        TransferReconciler(
            store, BankInquiryAdapter(gateway), Clock.systemUTC(),
            republishAfter = Duration.ofMillis(1), batchSize = 1_000, maxRepublish = props.maxRepublish, onAbandon = { abandoned += it },
        )
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
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to "P" + Random.nextInt(100_000, 999_999)))
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
    fun `설정 기본값 - 재발행은 마지막 변경 5분 뒤, 3회까지`() {
        assertEquals(Duration.ofMinutes(5), props.republishAfter)   // 재시도 체인(약 95초)과 겹치지 않게
        assertEquals(3, props.maxRepublish)
    }

    @Test
    fun `은행이 DLT 뒤에 살아나면 정리 작업이 재발행해 COMPLETED, 은행 입금은 1건`() {
        val account = "1002123456789"
        verify(account)
        val id = withBankDown {
            val id = request(account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString())
            assertEquals(TransferStatus.RECEIVED, store.findById(id)!!.status)
            id
        }
        // withBankDown 이 끝나면 모의 은행이 같은 포트로 다시 떠 있고 게이트웨이도 붙어 있다

        val result = reconciler.reconcile()

        assertTrue(result.republished >= 1, result.toString())
        assertEquals(1, store.findById(id)!!.republishCount)
        awaitStatus(id, TransferStatus.COMPLETED)
        val after = store.findById(id)!!
        assertEquals("0000", after.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })   // 은행 호출은 한 번
    }

    @Test
    fun `은행이 계속 내려가 있으면 재발행 3회 뒤 FAILED E001, 은행 입금은 0건`() {
        val account = "1002123456789"
        verify(account)
        withBankDown {
            val id = request(account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString(), count = 1)

            for (round in 1..props.maxRepublish) {
                reconciler.reconcile()
                assertEquals(round, store.findById(id)!!.republishCount)
                relay.publishPending()
                // 재발행한 메시지도 재시도 토픽을 다 거쳐 DLT 로 간다 → 그 뒤에야 "재발행 이후 시도 흔적" 이 생긴다
                consumerFactory.awaitRecords(TransferTopics.DLT, id.toString(), count = round + 1)
            }

            val result = reconciler.reconcile()

            val after = store.findById(id)!!
            assertTrue(result.abandoned >= 1, result.toString())
            assertEquals(TransferStatus.FAILED, after.status)
            assertEquals("E001", after.bankResponseCode)
            assertEquals(props.maxRepublish, after.republishCount)
            assertTrue(abandoned.any { it.id == id })
            assertEquals(0, bank.depositsOf(account).count { it.reference == after.reference })   // 돈이 움직이지 않았다
        }
    }
}
