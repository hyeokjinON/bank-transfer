package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 접수(HTTP) → 아웃박스 → 발행(relay) → 컨슈머 → 은행(모의, TCP) → 상태 기록 까지 한 번에.
 * 모의 은행: 1002123456789 는 처리(0000), 등록 안 된 계좌는 1001, 9990000000001 은 무응답.
 */
class TransferProcessingFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>

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

    /** 접수하고 이체 id 를 돌려준다. 적요는 테스트마다 달라서 모의 은행 입금 내역에서 이 건만 셀 수 있다. */
    private fun request(accountNo: String, memo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 30_000, "memo" to memo))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun awaitStatus(id: UUID, expected: TransferStatus) {
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(expected, store.findById(id)!!.status)
        }
    }

    private fun uniqueMemo() = "F" + Random.nextInt(100_000, 999_999)

    @Test
    fun `인증 계좌로 접수하면 은행 처리 후 COMPLETED, 은행 입금은 1건`() {
        val account = "1002123456789"
        verify(account)
        val memo = uniqueMemo()

        val id = request(account, memo)
        awaitStatus(id, TransferStatus.COMPLETED)

        assertEquals("0000", store.findById(id)!!.bankResponseCode)
        assertEquals(1, bank.depositsOf(account).count { it.memo == memo })
    }

    @Test
    fun `은행에 없는 계좌면 FAILED 와 응답코드 1001`() {
        val account = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)
        verify(account)

        val id = request(account, uniqueMemo())
        awaitStatus(id, TransferStatus.FAILED)

        assertEquals("1001", store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `은행이 응답하지 않으면 UNKNOWN 이고 다시 보내지 않는다`() {
        val account = "9990000000001"
        verify(account)

        val id = request(account, uniqueMemo())
        awaitStatus(id, TransferStatus.UNKNOWN)

        assertNull(store.findById(id)!!.bankResponseCode)
    }

    @Test
    fun `같은 메시지가 두 번 와도 은행에는 한 번만 보낸다`() {
        val account = "1002123456789"
        verify(account)
        val memo = uniqueMemo()
        val id = request(account, memo)
        awaitStatus(id, TransferStatus.COMPLETED)

        // 발행기가 커밋 직전에 죽어 같은 메시지를 다시 보낸 상황을 흉내 낸다
        kafka.send(TransferTopics.REQUESTED, id.toString(), """{"transferId":"$id"}""").get(5, TimeUnit.SECONDS)

        await.during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)) untilAsserted {
            assertEquals(1, bank.depositsOf(account).count { it.memo == memo })
        }
        assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
    }
}
