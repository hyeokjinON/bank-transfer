package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.api.support.committedOffset
import io.github.hyeokjinon.banktransfer.api.support.exceptionClassNames
import io.github.hyeokjinon.banktransfer.api.support.readAll
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 은행에 보내지도 못한 메시지의 재시도 토픽 · DLT 흐름 (P3-3b). 재시도 간격은 ApiIntegrationTest 가 0.5 · 1 · 2초로 줄인다.
 * 은행 장애는 모의 은행 pause/resume 으로 만든다 (withBankDown).
 */
class RetryTopicFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var kafkaAdmin: KafkaAdmin
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val references = RandomReferenceGenerator()

    /** 접수 API 를 거치지 않고 RECEIVED 이체 + 아웃박스 행을 만든다 (Kafka 경로만 본다). */
    private fun receivedTransfer(): Transfer {
        val now = Instant.now()
        val t = Transfer(
            id = UUID.randomUUID(), idempotencyKey = UUID.randomUUID().toString(), reference = references.next(),
            requestHash = "h".repeat(64), bankCode = "090", accountNo = "1002123456789", amount = 1_000, memo = "retry",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        return store.saveWithEvent(t, TransferEvent.TransferRequested(t.id, now))
    }

    @Test
    fun `은행 연결이 없으면 재시도 토픽 3개를 거쳐 DLT 로 가고, 넘긴 레코드마다 그 단계의 오프셋이 커밋된다`() {
        withBankDown {
            val t = receivedTransfer()
            val key = t.id.toString()
            relay.publishPending()

            val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()
            assertTrue(RetryLaterException::class.java.name in dlt.exceptionClassNames(), dlt.exceptionClassNames().toString())

            // 위험 확인 (설계 §4): manual_immediate 에서 다음 토픽으로 넘긴 레코드의 오프셋을 커밋하지 않으면,
            // 재시작 뒤 같은 메시지가 그 단계에서 다시 시작된다. 단계마다 그룹이 다르다 (원래 그룹 id + 토픽 접미사).
            val stages = listOf(
                TransferTopics.REQUESTED to GROUP,
                TransferTopics.RETRY_0 to "$GROUP-retry-0",
                TransferTopics.RETRY_1 to "$GROUP-retry-1",
                TransferTopics.RETRY_2 to "$GROUP-retry-2",
                TransferTopics.DLT to "$GROUP-dlt",
            )
            for ((topic, group) in stages) {
                val record = consumerFactory.readAll(topic).single { it.key() == key }
                val partition = TopicPartition(topic, record.partition())
                await.atMost(Duration.ofSeconds(15)) untilAsserted {
                    val committed = kafkaAdmin.committedOffset(group, partition)
                    assertTrue(committed != null && committed > record.offset(), "$group 가 $topic@${record.offset()} 를 커밋하지 않았다: $committed")
                }
            }

            val after = store.findById(t.id)!!
            assertEquals(TransferStatus.RECEIVED, after.status)   // DLT 로 가도 이체는 RECEIVED — 정리 작업의 재발행이 이어받는다
            assertNotNull(after.sendingAt)                         // 은행에 보내려 한 흔적 (재발행 기준)
            assertEquals(0, bank.depositsOf(t.accountNo).count { it.reference == t.reference })
        }
    }

    @Test
    fun `재시도 중인 메시지가 같은 파티션의 뒤 메시지를 막지 않는다`() {
        withBankDown {
            val stuck = receivedTransfer()
            val key = stuck.id.toString()
            relay.publishPending()
            // 발행기는 key 로 파티션을 고른다 (기본 파티셔너 = murmur2). 같은 파티션에 뒤 메시지를 바로 붙이려고 미리 계산한다
            val partition = Utils.toPositive(Utils.murmur2(key.toByteArray(Charsets.UTF_8))) % PARTITIONS

            // 뒤 메시지: 없는 이체 id 라 은행 없이 NOT_FOUND 로 끝나고 ack 된다
            val next = kafka.send(ProducerRecord(TransferTopics.REQUESTED, partition, UUID.randomUUID().toString(), "{}"))
                .get(5, TimeUnit.SECONDS).recordMetadata

            await.atMost(Duration.ofSeconds(3)) untilAsserted {
                val committed = kafkaAdmin.committedOffset(GROUP, TopicPartition(TransferTopics.REQUESTED, partition))
                assertTrue(committed != null && committed > next.offset(), "뒤 메시지(offset ${next.offset()})가 아직 처리되지 않았다: $committed")
            }
            // 그때 앞 메시지는 아직 재시도 중이다 (간격 합 0.5 + 1 + 2 = 3.5초). 블로킹 재시도라면 뒤 메시지가 앞 메시지의 포기를 기다린다
            assertTrue(consumerFactory.readAll(TransferTopics.DLT).none { it.key() == key }, "앞 메시지가 벌써 DLT 에 있다")
            assertEquals(partition, consumerFactory.readAll(TransferTopics.REQUESTED).single { it.key() == key }.partition())

            consumerFactory.awaitRecords(TransferTopics.DLT, key)
        }
    }

    @Test
    fun `key 가 UUID 가 아닌 메시지는 재시도 없이 바로 DLT`() {
        val key = "not-a-uuid-" + UUID.randomUUID()
        val sent = kafka.send(TransferTopics.REQUESTED, key, "{}").get(5, TimeUnit.SECONDS).recordMetadata

        val dlt = consumerFactory.awaitRecords(TransferTopics.DLT, key).single()

        assertTrue(InvalidTransferKeyException::class.java.name in dlt.exceptionClassNames(), dlt.exceptionClassNames().toString())
        assertTrue(consumerFactory.readAll(TransferTopics.RETRY_0).none { it.key() == key })   // 재시도 토픽을 거치지 않았다
        await.atMost(Duration.ofSeconds(15)) untilAsserted {
            val committed = kafkaAdmin.committedOffset(GROUP, TopicPartition(TransferTopics.REQUESTED, sent.partition()))
            assertTrue(committed != null && committed > sent.offset())   // 원래 토픽도 막히지 않고 넘어갔다
        }
    }

    private companion object {
        const val GROUP = "transfer-processor"   // application.yml spring.kafka.consumer.group-id
        const val PARTITIONS = 3
    }
}
