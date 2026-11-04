package io.github.hyeokjinon.banktransfer.api.messaging.experiment

import io.github.hyeokjinon.banktransfer.api.support.KafkaTestContainer
import io.github.hyeokjinon.banktransfer.api.support.committedOffset
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Configuration
import org.springframework.boot.testcontainers.service.connection.ServiceConnectionAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.kafka.annotation.BackOff
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.test.annotation.DirtiesContext
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * P6-1 실험 (설계 §7.1, Task 1 — 진행 조건): Spring Kafka 4.1.1 의 suspend @KafkaListener 가 이 저장소의 쓰임새대로 동작하는지 진짜 브로커로 본다.
 * 문서에 명시되지 않은 세 가지 (설계 §8):
 * ① @RetryableTopic: await 뒤에 던진 예외가 재시도 토픽 → DLT 로 가는가, 단계마다 몇 번 불리는가, DLT 원인 헤더가 원래 예외인가 (dltKind 가 이 헤더로 가른다)
 * ② 비동기 ack 의 poll 묶음 단위 정지: 처리 중인 레코드가 컨슈머 3 × max-poll-records 50 = 150 에 묶이는가
 * ③ 스레드: await 뒤 코드가 future 를 완료한 스레드(게이트웨이의 Netty 이벤트 루프 자리)에서 이어지는가 — 코루틴 리스너의 withContext(db) 근거
 * 앱 컨텍스트와 섞이지 않게 Kafka 자동 설정만 있는 작은 컨텍스트와 실험용 토픽 · 그룹(p61x.*)을 쓴다. spring.kafka.* 는 application.yml 그대로 (concurrency 3 · max-poll-records 50 · manual_immediate).
 * 설정 클래스는 테스트 클래스 안의 중첩 @Configuration 이다 — 앱의 컴포넌트 스캔에서 제외된다. @TestConfiguration 으로 두면 Boot 가 앱(@SpringBootApplication)까지 끌어와 빈이 겹친다.
 */
@SpringBootTest(classes = [SuspendListenerExperimentTest.ExperimentConfig::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)   // 이 클래스만 쓰는 컨텍스트 — 끝나면 Kafka 컨테이너까지 내린다
class SuspendListenerExperimentTest {

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration(ServiceConnectionAutoConfiguration::class, KafkaAutoConfiguration::class)
    @Import(KafkaTestContainer::class)
    class ExperimentConfig {
        @Bean fun retryExperimentTopic(): NewTopic = TopicBuilder.name(RETRY_TOPIC).partitions(3).replicas(1).build()
        @Bean fun inFlightExperimentTopic(): NewTopic = TopicBuilder.name(IN_FLIGHT_TOPIC).partitions(3).replicas(1).build()
        @Bean fun threadExperimentTopic(): NewTopic = TopicBuilder.name(THREAD_TOPIC).partitions(1).replicas(1).build()

        @Bean fun retryExperimentListener() = RetryExperimentListener()
        @Bean fun inFlightExperimentListener() = InFlightExperimentListener()
        @Bean fun threadExperimentListener() = ThreadExperimentListener()

        /** 앱(MessagingConfiguration)과 같이 재시도 토픽 전용 스케줄러를 둔다. */
        @Bean
        fun retryTopicSchedulerWrapper() = RetryTopicSchedulerWrapper(
            ThreadPoolTaskScheduler().apply {
                poolSize = 1
                setThreadNamePrefix("x-retry-topic-")
            },
        )
    }

    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var kafkaAdmin: KafkaAdmin
    @Autowired private lateinit var registry: KafkaListenerEndpointRegistry
    @Autowired private lateinit var retry: RetryExperimentListener
    @Autowired private lateinit var inFlight: InFlightExperimentListener
    @Autowired private lateinit var thread: ThreadExperimentListener

    @Test
    fun `① await 뒤에 던진 예외는 재시도 토픽 2개를 한 번씩 거쳐 DLT 로 간다 - DLT 원인 헤더는 원래 예외`() {
        val key = "fail-" + UUID.randomUUID()
        kafka.send(RETRY_TOPIC, key, "{}").get(5, TimeUnit.SECONDS)

        await.atMost(Duration.ofSeconds(30)) untilAsserted { assertTrue(retry.dlt.containsKey(key), "DLT 에 오지 않았다. 호출: ${retry.callsOf(key)}") }
        Thread.sleep(1_000)   // 늦게 오는 중복 호출이 있는지 조금 더 본다 (설계 §4 "실패 레코드를 한 번 더 받는다")

        println("[P6-1 실험 ①] 토픽별 호출 = ${retry.callsOf(key)}, DLT 원인 = ${retry.dlt.getValue(key).header(KafkaHeaders.EXCEPTION_CAUSE_FQCN)}")
        assertEquals(mapOf(RETRY_TOPIC to 1, "$RETRY_TOPIC-retry-0" to 1, "$RETRY_TOPIC-retry-1" to 1), retry.callsOf(key))
        assertEquals(ExperimentRetryException::class.java.name, retry.dlt.getValue(key).header(KafkaHeaders.EXCEPTION_CAUSE_FQCN))
    }

    @Test
    fun `① exclude 예외는 재시도 없이 바로 DLT`() {
        val key = "fatal-" + UUID.randomUUID()
        kafka.send(RETRY_TOPIC, key, "{}").get(5, TimeUnit.SECONDS)

        await.atMost(Duration.ofSeconds(30)) untilAsserted { assertTrue(retry.dlt.containsKey(key), "DLT 에 오지 않았다. 호출: ${retry.callsOf(key)}") }
        Thread.sleep(1_000)

        assertEquals(mapOf(RETRY_TOPIC to 1), retry.callsOf(key))
        assertEquals(ExperimentFatalException::class.java.name, retry.dlt.getValue(key).header(KafkaHeaders.EXCEPTION_CAUSE_FQCN))
    }

    @Test
    fun `① 정상으로 끝난 레코드는 한 번 불리고 오프셋이 커밋된다 (프레임워크가 ack)`() {
        val key = "ok-" + UUID.randomUUID()
        val sent = kafka.send(RETRY_TOPIC, key, "{}").get(5, TimeUnit.SECONDS).recordMetadata
        val partition = TopicPartition(RETRY_TOPIC, sent.partition())

        await.atMost(Duration.ofSeconds(15)) untilAsserted {
            val committed = kafkaAdmin.committedOffset(RETRY_GROUP, partition)
            assertTrue(committed != null && committed > sent.offset(), "커밋 = $committed, 레코드 offset = ${sent.offset()}")
        }
        assertEquals(mapOf(RETRY_TOPIC to 1), retry.callsOf(key))
    }

    @Test
    fun `② 처리 중인 레코드는 poll 묶음 단위로 묶인다 - 컨슈머 3 × max-poll-records 50 = 150 에서 멈추고 문을 열면 300건이 한 번씩 처리된다`() {
        val container = registry.getListenerContainer(IN_FLIGHT_ID)!! as ConcurrentMessageListenerContainer<*, *>
        container.start()
        // 컨슈머 3개가 각자 파티션 1개를 받을 때까지 기다린다: 합류가 늦어 리밸런스가 레코드 처리 중에 나면 안 받은 레코드가 다른 컨슈머로 다시 가
        // 두 번 처리되고 처리 중 수가 150 을 넘는다 (부하가 큰 전체 빌드에서 가끔 깨진 원인)
        await.atMost(Duration.ofSeconds(60)) until { container.containers.size == 3 && container.containers.all { it.assignedPartitions?.size == 1 } }
        // 레코드를 넣는 동안 poll 을 멈춘다: 조금씩 도착하는 레코드를 받으면 첫 묶음이 50건보다 작아져 150 에 닿지 않는다.
        // 다 넣은 뒤 다시 시작하면 첫 poll 이 파티션마다 50건을 한 번에 받는다
        container.pause()
        await.atMost(Duration.ofSeconds(30)) until { container.isContainerPaused }
        (0 until PER_PARTITION * 3)
            .map { i -> kafka.send(ProducerRecord(IN_FLIGHT_TOPIC, i % 3, "k-$i", "{}")) }
            .forEach { it.get(5, TimeUnit.SECONDS) }
        container.resume()

        // 문이 닫혀 있어 아무것도 끝나지 않는다 — 컨슈머마다 50건을 받아 150 이 되면 더 poll 하지 않고 멈춘다
        await.atMost(Duration.ofSeconds(60)) until { inFlight.inFlight.get() == 3 * MAX_POLL_RECORDS }
        Thread.sleep(2_000)   // 150 에서 더 늘지 않는지 (poll 묶음 단위로 멈췄는지) 조금 더 본다
        println("[P6-1 실험 ②] 문을 닫은 채 처리 중 = ${inFlight.inFlight.get()}, 최대 = ${inFlight.maxInFlight.get()} (150 = 컨슈머 3 × 50, poll 묶음 단위로 멈춤)")
        assertEquals(3 * MAX_POLL_RECORDS, inFlight.inFlight.get(), "멈춘 동안 처리 중 수")
        assertEquals(3 * MAX_POLL_RECORDS, inFlight.maxInFlight.get(), "멈춘 동안 처리 중 최대")

        inFlight.gate.complete(Unit)
        await.atMost(Duration.ofSeconds(60)) untilAsserted { assertEquals(PER_PARTITION * 3, inFlight.done.size) }
        assertTrue(inFlight.done.values.all { it.get() == 1 }, "같은 레코드를 두 번 처리했다")
        assertEquals(3 * MAX_POLL_RECORDS, inFlight.maxInFlight.get(), "문을 연 뒤 최대")
        for (p in 0 until 3) {
            await.atMost(Duration.ofSeconds(15)) untilAsserted {
                assertEquals(PER_PARTITION.toLong(), kafkaAdmin.committedOffset(IN_FLIGHT_GROUP, TopicPartition(IN_FLIGHT_TOPIC, p)))
            }
        }
    }

    @Test
    fun `③ await 뒤 코드는 future 를 완료한 스레드에서 이어진다 - Spring 은 Dispatchers Unconfined 로 부른다`() {
        kafka.send(THREAD_TOPIC, "t-" + UUID.randomUUID(), "{}").get(5, TimeUnit.SECONDS)

        val (before, after) = thread.threads.get(30, TimeUnit.SECONDS)

        println("[P6-1 실험 ③] await 전 = $before, await 뒤 = $after")
        // 코루틴 디버그 모드(테스트는 -ea)가 스레드 이름에 " @coroutine#N" 을 덧붙인다 — 스레드 자체만 비교한다
        assertTrue("-C-" in before, "await 전은 컨슈머 스레드여야 한다: $before")
        assertEquals(FAKE_BANK_THREAD, after.substringBefore(" @coroutine"))
    }

    companion object {
        const val RETRY_TOPIC = "p61x.retry"
        const val RETRY_GROUP = "p61x-retry"
        const val IN_FLIGHT_TOPIC = "p61x.inflight"
        const val IN_FLIGHT_GROUP = "p61x-inflight"
        const val IN_FLIGHT_ID = "p61x-inflight"
        const val THREAD_TOPIC = "p61x.thread"
        const val FAKE_BANK_THREAD = "x-fake-bank-loop"
        const val PER_PARTITION = 100
        const val MAX_POLL_RECORDS = 50   // application.yml 의 spring.kafka.consumer.max-poll-records
    }
}

/** 재시도 토픽으로 넘길 예외 (실제 코드의 RetryLaterException 자리). */
class ExperimentRetryException : RuntimeException("experiment: retry later")

/** 재시도하지 않는 예외 (실제 코드의 InvalidTransferKeyException 자리 — @RetryableTopic exclude). */
class ExperimentFatalException : RuntimeException("experiment: not retryable")

private fun namedLoop(name: String): ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r -> Thread(r, name).apply { isDaemon = true } }

/** 은행 응답처럼 다른 스레드가 조금 뒤에 완료하는 future. 이미 완료된 future 는 await 가 멈추지 않고 그 자리에서 이어 가 실험이 되지 않는다. */
private fun later(loop: ScheduledExecutorService): CompletableFuture<String> =
    CompletableFuture<String>().also { f -> loop.schedule(Runnable { f.complete("bank") }, 50, TimeUnit.MILLISECONDS) }

private fun ConsumerRecord<*, *>.header(name: String): String? = headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

/** ① 실제 리스너와 같은 모양: key 검사(멈추기 전) → 은행 응답 기다림(다른 스레드) → 결과에 따라 예외. DLT 는 블로킹 핸들러. */
class RetryExperimentListener {
    private val calls = ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicInteger>>()
    val dlt = ConcurrentHashMap<String, ConsumerRecord<String, String>>()
    private val loop = namedLoop("x-retry-bank")

    fun callsOf(key: String): Map<String, Int> = calls[key].orEmpty().mapValues { it.value.get() }

    @RetryableTopic(
        attempts = "3",
        backOff = BackOff(delayString = "200"),
        numPartitions = "3",
        replicationFactor = "1",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
        exclude = [ExperimentFatalException::class],
    )
    @KafkaListener(topics = [SuspendListenerExperimentTest.RETRY_TOPIC], groupId = SuspendListenerExperimentTest.RETRY_GROUP)
    suspend fun on(record: ConsumerRecord<String, String>) {
        calls.computeIfAbsent(record.key()) { ConcurrentHashMap() }.computeIfAbsent(record.topic()) { AtomicInteger() }.incrementAndGet()
        if (record.key().startsWith("fatal-")) throw ExperimentFatalException()
        later(loop).await()
        if (record.key().startsWith("fail-")) throw ExperimentRetryException()
    }

    @DltHandler
    fun onDlt(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        dlt[record.key()] = record
        ack.acknowledge()
    }
}

/** ② 테스트가 문을 열 때까지 모든 호출이 기다린다. 동시에 처리 중인 수의 최댓값을 센다. */
class InFlightExperimentListener {
    val gate = CompletableFuture<Unit>()
    val inFlight = AtomicInteger()
    val maxInFlight = AtomicInteger()
    val done = ConcurrentHashMap<String, AtomicInteger>()

    @KafkaListener(
        id = SuspendListenerExperimentTest.IN_FLIGHT_ID,
        topics = [SuspendListenerExperimentTest.IN_FLIGHT_TOPIC],
        groupId = SuspendListenerExperimentTest.IN_FLIGHT_GROUP,
        autoStartup = "false",
    )
    suspend fun on(record: ConsumerRecord<String, String>) {
        maxInFlight.accumulateAndGet(inFlight.incrementAndGet()) { a, b -> maxOf(a, b) }
        try {
            gate.await()
        } finally {
            inFlight.decrementAndGet()
        }
        done.computeIfAbsent(record.key()) { AtomicInteger() }.incrementAndGet()
    }
}

/** ③ await 전 · 뒤의 스레드 이름. */
class ThreadExperimentListener {
    val threads = CompletableFuture<Pair<String, String>>()
    private val loop = namedLoop(SuspendListenerExperimentTest.FAKE_BANK_THREAD)

    @KafkaListener(topics = [SuspendListenerExperimentTest.THREAD_TOPIC], groupId = "p61x-thread")
    suspend fun on(record: ConsumerRecord<String, String>) {
        val before = Thread.currentThread().name
        later(loop).await()
        threads.complete(before to Thread.currentThread().name)
    }
}
