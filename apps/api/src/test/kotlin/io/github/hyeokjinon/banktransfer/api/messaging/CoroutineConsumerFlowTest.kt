package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.api.support.committedOffset
import io.github.hyeokjinon.banktransfer.api.support.readAll
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 코루틴 모드 흐름 (P6-1, 설계 §7.6): 아웃박스 → Kafka → suspend 리스너 → 모의 은행(TCP) → 상태 기록, 블로킹 흐름 테스트와 같은 결과인지.
 * 리스너가 설정으로 갈려 이 클래스만 별도 컨텍스트다(컨테이너도 따로) — 끝나면 닫는다. 모의 은행은 공유 은행(ApiIntegrationTest.bank)이다.
 * "보내지 못함" 은 은행 점검 시간(보내기 문)으로 만든다: 연결 없음과 같은 BankNotSentException 길이고, 공유 은행을 내리지 않아 블로킹 컨텍스트의 게이트웨이를 끊지 않는다.
 */
@TestPropertySource(properties = ["transfer.consumer.mode=coroutine"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CoroutineConsumerFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var kafkaAdmin: KafkaAdmin
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired private lateinit var context: ApplicationContext

    private val references = RandomReferenceGenerator()
    private val user by lazy { fundedUser(FUNDED) }

    /** 접수 API 를 거치지 않고 RECEIVED 이체 + 아웃박스 행을 만든다 (Kafka 경로만 본다). saveWithEvent 가 잔고도 뺀다. */
    private fun receivedTransfer(accountNo: String, amount: Long = 1_000): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), userId = user, idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = accountNo, amount = amount, memo = "co",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        return store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, now))
    }

    private fun awaitStatus(id: UUID, expected: TransferStatus, timeout: Duration = Duration.ofSeconds(20)) {
        await.atMost(timeout) untilAsserted {
            relay.publishPending()
            assertEquals(expected, store.findById(id)!!.status)
        }
    }

    /** 오늘(KST) 23:55 — 은행 점검 중. 게이트웨이가 보내지 않고 BankNotSentException (연결 없음과 같은 길). */
    private fun duringMaintenance(): Instant = LocalDate.now(SEOUL).atTime(23, 55).atZone(SEOUL).toInstant()

    @Test
    fun `코루틴 모드로 뜬다 - 코루틴 리스너 하나만, actuator info 의 mode 는 coroutine`() {
        assertEquals(0, context.getBeanNamesForType(TransferRequestedListener::class.java).size)
        assertEquals(1, context.getBeanNamesForType(SuspendingTransferRequestedListener::class.java).size)

        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"mode\":\"coroutine\"" in info, info)
    }

    @Test
    fun `완료 - 은행 처리 후 COMPLETED 0000, 은행에는 한 번`() {
        val t = receivedTransfer(REGISTERED)

        awaitStatus(t.id, TransferStatus.COMPLETED)

        assertEquals("0000", store.findById(t.id)!!.bankResponseCode)
        assertEquals(1, bank.depositsOf(REGISTERED).count { it.reference == t.reference })
    }

    @Test
    fun `거절 - FAILED 1001 과 같은 트랜잭션의 환불, 잔고 = 원장 합`() {
        val t = receivedTransfer(unregisteredAccount())

        awaitStatus(t.id, TransferStatus.FAILED)

        assertEquals("1001", store.findById(t.id)!!.bankResponseCode)
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(t.id))
        assertEquals(FUNDED, wallets.balanceOf(user))
        assertEquals(wallets.balanceOf(user), ledgerSum(user))
    }

    @Test
    fun `시간 초과 - UNKNOWN 이고 다시 보내지 않는다, 돈은 보류로 묶인다`() {
        val t = receivedTransfer(SILENT)

        awaitStatus(t.id, TransferStatus.UNKNOWN)

        assertNull(store.findById(t.id)!!.bankResponseCode)
        assertEquals(t.amount, wallets.pendingOf(user))
    }

    @Test
    fun `보내지 못함이 이어지면 재시도 토픽 3개를 거쳐 DLT - 출금은 RECEIVED 로 남고 은행에는 가지 않았다`() {
        val t = receivedTransfer(unregisteredAccount())   // 혹시 나중에 처리돼도 거절 → 공용 계좌에 입금이 생기지 않게
        val key = t.id.toString()
        testClock.fix(duringMaintenance())

        relay.publishPending()
        val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()

        assertEquals(RetryLaterException::class.java.name, dlt.headers().lastHeader(KafkaHeaders.EXCEPTION_CAUSE_FQCN)?.value()?.toString(Charsets.UTF_8))
        for (topic in listOf(TransferTopics.RETRY_0, TransferTopics.RETRY_1, TransferTopics.RETRY_2)) {
            assertEquals(1, consumerFactory.readAll(topic).count { it.key() == key }, topic)
        }
        val after = store.findById(t.id)!!
        assertEquals(TransferStatus.RECEIVED, after.status)   // 정리 작업의 재발행이 이어받는다
        assertNotNull(after.sendingAt)
        assertFalse(bank.hasProcessed(t.reference))
    }

    @Test
    fun `보내지 못함 - 재시도 토픽으로 넘어갔다가 보낼 수 있게 되면 COMPLETED, 은행에는 한 번`() {
        val t = receivedTransfer(REGISTERED)
        testClock.fix(duringMaintenance())

        relay.publishPending()
        consumerFactory.awaitRecords(TransferTopics.RETRY_0, t.id.toString())   // 첫 시도가 보내지 못해 재시도 토픽으로 넘어갔다
        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val now = store.findById(t.id)!!
            assertEquals(TransferStatus.RECEIVED, now.status)
            assertNotNull(now.sendingAt)
        }
        testClock.release()   // 점검이 끝났다: 남은 재시도(0.5 · 1 · 2초 뒤) 중 하나가 보낸다

        awaitStatus(t.id, TransferStatus.COMPLETED)
        assertEquals(1, bank.depositsOf(REGISTERED).count { it.reference == t.reference })
    }

    @Test
    fun `key 가 UUID 가 아닌 메시지는 재시도 없이 바로 DLT, 원래 토픽도 막히지 않는다`() {
        val key = "not-a-uuid-" + UUID.randomUUID()
        val sent = kafka.send(TransferTopics.REQUESTED, key, "{}").get(5, TimeUnit.SECONDS).recordMetadata

        val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()

        assertEquals(InvalidTransferKeyException::class.java.name, dlt.headers().lastHeader(KafkaHeaders.EXCEPTION_CAUSE_FQCN)?.value()?.toString(Charsets.UTF_8))
        assertTrue(consumerFactory.readAll(TransferTopics.RETRY_0).none { it.key() == key })
        await.atMost(Duration.ofSeconds(15)) untilAsserted {
            val committed = kafkaAdmin.committedOffset(GROUP, TopicPartition(TransferTopics.REQUESTED, sent.partition()))
            assertTrue(committed != null && committed > sent.offset())
        }
    }

    @Test
    fun `은행이 느리면 컨슈머 스레드 3개보다 많은 출금이 함께 은행 응답을 기다린다 - 거래번호마다 은행에 한 번`() {
        bank.responseDelay = Duration.ofMillis(200)   // 테스트 응답 시간 초과(500ms) 안
        try {
            bank.resetPeakPendingResponses()
            val transfers = (1..30).map { receivedTransfer(REGISTERED, amount = 1) }

            await.atMost(Duration.ofSeconds(30)) untilAsserted {
                relay.publishPending()
                assertTrue(transfers.all { store.findById(it.id)!!.status == TransferStatus.COMPLETED })
            }

            val peak = bank.peakPendingResponses()
            assertTrue(peak > TransferTopics.PARTITIONS, "모의 은행의 최대 동시 대기 $peak — 블로킹이면 컨슈머 수(3)를 넘지 못한다")
            transfers.forEach { t -> assertEquals(1, bank.depositsOf(REGISTERED).count { it.reference == t.reference }, t.reference) }
        } finally {
            bank.responseDelay = Duration.ZERO
        }
    }

    private companion object {
        const val FUNDED = 1_000_000L
        const val REGISTERED = "1002123456789"   // 모의 은행 기본 계좌 (0000)
        const val SILENT = "9990000000001"       // ApiIntegrationTest 의 모의 은행: 무응답 · 처리 안 함
        const val GROUP = "transfer-processor"
        val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
