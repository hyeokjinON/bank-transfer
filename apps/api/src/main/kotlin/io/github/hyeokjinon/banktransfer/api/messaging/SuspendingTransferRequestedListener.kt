package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.ProcessResult
import io.github.hyeokjinon.banktransfer.transfer.SuspendBankTransferPort
import io.github.hyeokjinon.banktransfer.transfer.TransferProcessor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
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
 * transfer.requested 컨슈머 — 코루틴 판 (P6-1, ADR-0012). transfer.consumer.mode=coroutine 일 때만 빈이 된다 (기본은 블로킹 [TransferRequestedListener]).
 *
 * 블로킹 판과 다른 것은 은행 응답을 기다리는 방법 하나다: 컨슈머 스레드가 `.get()` 으로 최대 3초 막히는 대신 `await()` 로 코루틴만 멈춘다.
 * 그래서 동시에 기다리는 출금 수가 컨슈머 스레드 수(3)에 묶이지 않는다. 대신
 * - 은행에 동시에 보내는 수는 [bankCalls] 로 묶는다 (transfer.consumer.coroutine.max-in-flight-bank-calls, 기본 64 — 실제 은행도 기관별 동시 요청을 제한한다).
 *   퍼밋은 begin(RECEIVED → SENDING) 부터 송금 응답까지 잡는다 — 기다리는 동안 SENDING 이면 정리 작업이 먼저 환불할 수 있다 (process 주석).
 *   처리 중인 메시지 수 자체는 Spring Kafka 비동기 ack 의 poll 묶음 단위 정지로 컨슈머 3 × max-poll-records 50 에 묶인다 (Task 1 실험 ②).
 * - DB 호출(begin · finish)과 결과 마무리(CONFLICT 알림 = 텔레그램 HTTP)는 [db] 에서 돈다. Spring 7 은 suspend 리스너를 Dispatchers.Unconfined 로 부르므로
 *   은행 응답 뒤의 코드는 그 응답을 완료한 Netty 이벤트 루프 스레드(게이트웨이에 하나뿐)에서 이어진다 — 거기서 JDBC 를 기다리면 모든 은행 응답이 멈춘다 (실험 ③).
 *   세마포어를 놓으면 기다리던 코루틴이 놓은 스레드에서 이어지지만, 그 구간은 전문 쓰기뿐이라 막지 않는다.
 * - ack 는 프레임워크가 한다: 함수가 정상으로 끝나면 ack, 예외를 던지면 오류 처리기 → 재시도 토픽 (블로킹 판의 수동 ack 와 같은 결과, 실험 ①).
 * - 같은 파티션 안 메시지의 처리 순서를 지키지 않는다. 출금은 서로 독립이고 상태 전이가 조건부라 영향이 없다 (ADR-0012).
 * 판정은 [TransferProcessor] 의 begin · finish, key 읽기 · 결과 마무리 · DLT 는 [TransferRequestedHandling] — 블로킹 판과 같은 코드다. DLT 처리는 블로킹 그대로(드물고 짧다).
 */
class SuspendingTransferRequestedListener(
    private val processor: TransferProcessor,
    private val bank: SuspendBankTransferPort,
    /** 은행 동시 호출 상한. 원래 토픽 · 재시도 토픽 컨테이너가 이 빈 하나를 같이 쓴다. 테스트가 작은 값으로 바꿔 끼운다. */
    private val bankCalls: Semaphore,
    /** DB · 알림처럼 스레드를 막는 호출을 돌릴 곳. 기본 Dispatchers.IO, 테스트는 이름 붙인 스레드로 바꿔 어디서 돌았는지 본다. */
    private val db: CoroutineDispatcher = Dispatchers.IO,
    alerts: AlertNotifier = AlertNotifier { },
) {
    private val handling = TransferRequestedHandling(alerts, LoggerFactory.getLogger(javaClass))

    @RetryableTopic(
        attempts = TransferListenerSettings.ATTEMPTS,
        backOff = BackOff(
            delayString = TransferListenerSettings.DELAY_MS,
            multiplierString = TransferListenerSettings.MULTIPLIER,
            maxDelayString = TransferListenerSettings.MAX_DELAY_MS,
        ),
        numPartitions = TransferTopics.PARTITIONS_STR,
        replicationFactor = "1",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
        exclude = [InvalidTransferKeyException::class],
    )
    @KafkaListener(topics = [TransferTopics.REQUESTED], groupId = TransferListenerSettings.GROUP_ID)
    suspend fun on(record: ConsumerRecord<String, String>) {
        val transferId = handling.transferIdOf(record.key())
        val result = process(transferId)
        // RETRY_LATER 면 여기서 던진다 → 프레임워크가 재시도 토픽으로. CONFLICT 알림(텔레그램 HTTP)도 막는 호출이라 db 에서
        withContext(db) { handling.conclude(record, transferId, result) }
    }

    private suspend fun process(transferId: UUID): ProcessResult {
        // begin 도 퍼밋 안에서 한다: 퍼밋을 기다리는 동안 행이 이미 SENDING 이면, 30초 넘게 기다릴 때(상한이 작고 은행이 느릴 때) 정리 작업이
        // UNKNOWN → 결과조회 "미처리" → FAILED + 환불로 확정한 뒤에 이 코루틴이 퍼밋을 받아 송금한다 → 환불과 입금이 둘 다 일어난다 (CONFLICT).
        // 블로킹 판은 begin 과 송금 사이가 거의 0 이다. 기다리는 동안은 RECEIVED 로 두고, 퍼밋을 받은 뒤에야 SENDING 으로 바꾼다.
        // 퍼밋이 begin 의 짧은 DB 호출 하나를 함께 덮는 값을 치른다. finish · conclude 는 퍼밋 밖(자리를 오래 잡지 않는다).
        val outcome = bankCalls.withPermit {
            val transfer = when (val begun = withContext(db) { processor.begin(transferId) }) {
                TransferProcessor.Begin.NotFound -> return ProcessResult.NOT_FOUND
                TransferProcessor.Begin.AlreadyHandled -> return ProcessResult.ALREADY_HANDLED
                is TransferProcessor.Begin.Started -> begun.transfer
            }
            try {
                bank.transfer(transfer.bankCode, transfer.accountNo, transfer.amount, transfer.memo, transfer.reference)
            } catch (e: CancellationException) {
                // 은행에 갔는지 모른다: 아무것도 기록하지 않는다 → SENDING 으로 남아 정리 작업이 30초 뒤 UNKNOWN → 결과조회 (설계 §4). NotSent 로 다시 보내면 이중 지급
                throw e
            } catch (e: Exception) {
                // 블로킹 process() 와 같다: 포트가 분류하지 못한 예외 = 받았는지 모른다 → UNKNOWN
                BankTransferOutcome.ResultUnknown
            }
        }
        return withContext(db) { processor.finish(transferId, outcome) }
    }

    /** DLT: 블로킹 판과 같다 (로그 · 알림은 [TransferRequestedHandling.onDlt]). 항상 ack 한다. */
    @DltHandler
    fun onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        handling.onDlt(record)
        ack.acknowledge()
    }
}
