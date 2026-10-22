package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.BackOff
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.util.UUID

/** 은행에 보내지 못해 나중에 다시 처리해야 한다. 재시도 토픽으로 넘어간다 (P3-3b). */
class RetryLaterException(transferId: UUID) : RuntimeException("bank not reachable, transfer $transferId will be retried")

/** key 가 이체 id(UUID)가 아니다. 몇 번 다시 해도 같은 결과라 재시도 없이 DLT 로 보낸다 (@RetryableTopic exclude). */
class InvalidTransferKeyException(key: String?) : RuntimeException("message key is not a transfer id (UUID): $key")

/**
 * transfer.requested 컨슈머. key = 이체 id. 처리가 끝난 뒤에만 오프셋을 커밋한다 (수동 ack):
 * 처리 중에 죽으면 같은 메시지를 다시 받고, 조건부 상태 변경이 중복 처리를 막는다.
 *
 * 은행에 보내지 못한 경우(RETRY_LATER)는 예외를 던져 **재시도 토픽**으로 넘긴다 (P3-3b, 논블로킹):
 * 원래 토픽의 같은 파티션 뒤 메시지는 기다리지 않는다. 간격 5s → 30s → 60s 뒤에도 안 되면 DLT 로 가고,
 * 이체는 RECEIVED 로 남아 정리 작업의 재발행이 이어받는다. 넘긴 레코드의 오프셋은 프레임워크가 커밋한다(commitRecovered).
 * 그룹 · 동시성은 application.yml 한 곳에서 정한다 (groupId 는 그 값을 가리킬 뿐이다 — 재시도 · DLT 그룹이 접미사로 나뉘게).
 */
class TransferRequestedListener(private val process: (UUID) -> ProcessResult) {
    private val log = LoggerFactory.getLogger(javaClass)

    @RetryableTopic(
        attempts = "\${transfer.retry.attempts:4}",
        backOff = BackOff(
            delayString = "\${transfer.retry.delay-ms:5000}",
            multiplierString = "\${transfer.retry.multiplier:6.0}",
            maxDelayString = "\${transfer.retry.max-delay-ms:60000}",
        ),
        // 브로커 자동 생성은 꺼져 있다. DLT 로 보낼 때 원래 파티션 번호를 그대로 쓰므로 원래 토픽과 같은 수
        numPartitions = TransferTopics.PARTITIONS_STR,
        replicationFactor = "1",
        // -retry-0/1/2: 간격 설정을 바꿔도 토픽 이름이 같다 (기본값은 간격 값을 이름에 붙인다)
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
        exclude = [InvalidTransferKeyException::class],
    )
    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = "\${spring.kafka.consumer.group-id}")
    fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val transferId = transferIdOf(record.key())
        val result = process(transferId)
        if (result == ProcessResult.RETRY_LATER) throw RetryLaterException(transferId)
        if (result == ProcessResult.CONFLICT) {
            // 재시도로 풀 수 없다: 은행이 확정한 결과가 이미 기록된 최종 상태와 다르다. 오프셋은 넘기고 사람이 확인하게 경고만 남긴다
            log.warn("transfer {} conflict: bank result differs from the recorded final status — needs manual check", transferId)
        }
        log.info("transfer {} processed: {} ({} partition {}, offset {})", transferId, result, record.topic(), record.partition(), record.offset())
        ack.acknowledge()
    }

    /**
     * 재시도를 다 쓴 메시지 · 재시도하지 않는 메시지. 경고만 남기고 ack 한다 (안 하면 DLT 오프셋이 커밋되지 않아 재시작마다 다시 온다).
     * 이체는 RECEIVED 그대로다 — 은행에 보내려 한 흔적이 있으면 5분 뒤 정리 작업이 아웃박스로 다시 발행한다. 알림 채널은 P5.
     * 메시지 값(계좌번호는 없지만)은 남기지 않는다.
     */
    @DltHandler
    fun onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        log.warn(
            "transfer message moved to DLT, stays RECEIVED until reconcile republishes it: key={} exception={} message={} original={}-{}@{}",
            record.key(),
            record.headerText(KafkaHeaders.EXCEPTION_CAUSE_FQCN) ?: record.headerText(KafkaHeaders.EXCEPTION_FQCN),
            record.headerText(KafkaHeaders.EXCEPTION_MESSAGE),
            record.headerText(KafkaHeaders.ORIGINAL_TOPIC),
            record.headerInt(KafkaHeaders.ORIGINAL_PARTITION),
            record.headerLong(KafkaHeaders.ORIGINAL_OFFSET),
        )
        ack.acknowledge()
    }

    private fun transferIdOf(key: String?): UUID {
        if (key == null) throw InvalidTransferKeyException(null)
        return try {
            UUID.fromString(key)
        } catch (e: IllegalArgumentException) {
            throw InvalidTransferKeyException(key)
        }
    }
}

private fun ConsumerRecord<*, *>.headerText(name: String): String? =
    headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

private fun ConsumerRecord<*, *>.headerInt(name: String): Int? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Int.SIZE_BYTES }?.let { ByteBuffer.wrap(it).int }

private fun ConsumerRecord<*, *>.headerLong(name: String): Long? =
    headers().lastHeader(name)?.value()?.takeIf { it.size == Long.SIZE_BYTES }?.let { ByteBuffer.wrap(it).long }
