package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
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
import java.util.UUID

/**
 * transfer.requested 컨슈머 — 블로킹 판, 기본 (transfer.consumer.mode=blocking). key = 이체 id. 처리가 끝난 뒤에만 오프셋을 커밋한다 (수동 ack):
 * 처리 중에 죽으면 같은 메시지를 다시 받고, 조건부 상태 변경이 중복 처리를 막는다.
 *
 * 은행에 보내지 못한 경우(RETRY_LATER)는 예외를 던져 **재시도 토픽**으로 넘긴다 (P3-3b, 논블로킹):
 * 원래 토픽의 같은 파티션 뒤 메시지는 기다리지 않는다. 간격 5s → 30s → 60s 뒤에도 안 되면 DLT 로 가고,
 * 이체는 RECEIVED 로 남아 정리 작업의 재발행이 이어받는다. 넘긴 레코드의 오프셋은 프레임워크가 커밋한다(commitRecovered).
 * 은행 응답은 이 컨슈머 스레드가 기다린다(최대 3초) — 동시에 기다릴 수 있는 출금은 컨슈머 수(3)다. 코루틴 판은 SuspendingTransferRequestedListener (P6-1).
 * key 읽기 · 결과 마무리(CONFLICT 경고 · 알림) · DLT 는 코루틴 판과 같은 [TransferRequestedHandling] 을 쓴다. 재시도 · 그룹 값은 [TransferListenerSettings].
 */
class TransferRequestedListener(
    /** 사람이 봐야 하는 경고를 알린다 (P5-1). 기본은 아무것도 하지 않는다 — 테스트 · 조립 전 기본값. 앞에 두어 `TransferRequestedListener { … }` 가 그대로 컴파일된다. */
    alerts: AlertNotifier = AlertNotifier { },
    private val process: (UUID) -> ProcessResult,
) {
    private val handling = TransferRequestedHandling(alerts, LoggerFactory.getLogger(javaClass))

    @RetryableTopic(
        attempts = TransferListenerSettings.ATTEMPTS,
        backOff = BackOff(
            delayString = TransferListenerSettings.DELAY_MS,
            multiplierString = TransferListenerSettings.MULTIPLIER,
            maxDelayString = TransferListenerSettings.MAX_DELAY_MS,
        ),
        // 브로커 자동 생성은 꺼져 있다. DLT 로 보낼 때 원래 파티션 번호를 그대로 쓰므로 원래 토픽과 같은 수
        numPartitions = TransferTopics.PARTITIONS_STR,
        replicationFactor = "1",
        // -retry-0/1/2: 간격 설정을 바꿔도 토픽 이름이 같다 (기본값은 간격 값을 이름에 붙인다)
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
        exclude = [InvalidTransferKeyException::class],
    )
    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = TransferListenerSettings.GROUP_ID)
    fun on(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val transferId = handling.transferIdOf(record.key())
        handling.conclude(record, transferId, process(transferId))   // RETRY_LATER 면 여기서 던진다 → ack 하지 않는다
        ack.acknowledge()
    }

    /** DLT: 로그 · 알림은 [TransferRequestedHandling.onDlt]. 항상 ack 한다 (안 하면 DLT 오프셋이 커밋되지 않아 재시작마다 다시 온다). */
    @DltHandler
    fun onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        handling.onDlt(record)
        ack.acknowledge()
    }
}
