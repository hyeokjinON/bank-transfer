package io.github.hyeokjinon.banktransfer.api.experiment

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.send
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import jakarta.servlet.Filter
import jakarta.servlet.http.HttpServletRequest
import jdk.jfr.consumer.RecordedEvent
import jdk.jfr.consumer.RecordingStream
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.tomcat.autoconfigure.TomcatVirtualThreadsWebServerFactoryCustomizer
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * P7 진행 조건 실험 V1 · V2 (설계 §4). `spring.threads.virtual.enabled=true` 컨텍스트 — 설정이 달라 새 컨텍스트(컨테이너도 새로)가 뜬다.
 * V1: Tomcat 요청 · 앱 실행기 · 예약 작업 · Kafka 리스너가 가상 스레드인지. 예약 작업 · 리스너는 테스트용 탐침이다 — 앱 것과 같은 Boot 스케줄러 ·
 *     컨테이너 팩토리를 쓴다 (앱에 직접 만든 팩토리가 없다). Tomcat 실행기는 웹 판과 상관없이 같아 기본 판(async) 요청으로 본다.
 * V2: JFR jdk.VirtualThreadPinned(JDK 21 은 모니터를 쥔 채 park 할 때만 남긴다 — synchronized 입장 대기는 세지 않는다)를 기준 0ms 로 켜고
 *     요청을 한 건씩 실제 경로로 돌린다 (경합은 만들지 않는다). 블로킹 판은 컨트롤러의 서비스 호출(service.request(...).join())을
 *     가상 스레드에서 그대로 부른다 (Tomcat 요청 스레드 = 가상 스레드인 것은 V1 이 본다). 대조 테스트는 검출이 실제로 동작하는지 본다.
 *     확인된 1회성 pinning: 첫 요청의 DispatcherServlet 초기화(Tomcat StandardWrapper.allocate 의 synchronized 안)에서 로그가 출력 잠금을 기다리면
 *     캐리어를 붙잡는다 (Gradle 테스트 출력 잠금에서 재현). 공유 Kafka 프로듀서의 첫 생성(AppInfoParser.registerAppInfo, static synchronized
 *     안의 버전 로그)도 같은 모양이다 (CI 에서 확인). 그래서 V2 는 녹화 전에 요청 한 번 · 파티션 조회 한 번으로 둘 다 끝내 둔다. 운영에서는
 *     spring.mvc.servlet.load-on-startup 으로 기동 때 초기화해 피할 수 있다 (P7 범위 밖, ADR-0014).
 *     가상 스레드 리스너의 컨슈머 시작(그룹 합류 · 오프셋 초기화)도 Kafka 컨슈머의 synchronized 안 로그로 pinning 을 만든다 (캐리어 4개에서 재현).
 *     요청 경로가 아니라서 V2 는 리스너가 모두 파티션을 받고 첫 fetch 를 보낸 뒤 녹화한다.
 * 이 컨텍스트(가상 스레드 리스너 약 16개 + 자기 Kafka 컨테이너)는 클래스가 끝나면 닫는다 — 캐리어가 적은 CI(4코어)에서 남은 테스트 내내
 * 가상 스레드 리스너가 돌지 않게 (P7 머지 뒤 CI 가 이 클래스 다음에서 멈춘 일이 있다 — 원인 미확인).
 * 결과는 ADR-0014 · 결과 문서에 옮긴다.
 */
@TestPropertySource(properties = ["spring.threads.virtual.enabled=true"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(VirtualThreadExperimentTest.Probes::class)
class VirtualThreadExperimentTest : ApiIntegrationTest() {

    /** 곳마다 처음 본 스레드. 이 클래스만 쓰는 컨텍스트라 다른 테스트와 섞이지 않는다. */
    object ThreadProbe {
        val seen = ConcurrentHashMap<String, Thread>()
        fun record(where: String) {
            seen.putIfAbsent(where, Thread.currentThread())
        }
    }

    class ScheduledProbe {
        @Scheduled(fixedDelay = 100)
        fun tick() = ThreadProbe.record("scheduled")
    }

    class ListenerProbe {
        @KafkaListener(topics = [PROBE_TOPIC], groupId = PROBE_GROUP)
        fun on(@Suppress("UNUSED_PARAMETER") value: String) = ThreadProbe.record("kafka")
    }

    @TestConfiguration(proxyBeanMethods = false)
    class Probes {
        @Bean
        fun vtProbeFilter(): Filter = Filter { req, res, chain ->
            if ((req as HttpServletRequest).requestURI == "/api/v1/account-verifications") ThreadProbe.record("tomcat")
            chain.doFilter(req, res)
        }

        @Bean
        fun vtProbeScheduled() = ScheduledProbe()

        @Bean
        fun vtProbeListener() = ListenerProbe()
    }

    companion object {
        const val PROBE_TOPIC = "p7.vt.probe"
        const val PROBE_GROUP = "p7-vt-probe"
        private const val ACCOUNT = "1002123456789"   // 모의 은행 기본 계좌 (예금주 홍길동)
    }

    @Autowired private lateinit var context: ApplicationContext
    @Autowired @Qualifier("applicationTaskExecutor") private lateinit var executor: Executor
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var service: AccountVerificationService
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var registry: KafkaListenerEndpointRegistry

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `V1 설정 하나로 Tomcat 요청 · 앱 실행기 · 예약 작업 · Kafka 리스너가 가상 스레드에서 돈다`() {
        assertFalse(context.getBeansOfType(TomcatVirtualThreadsWebServerFactoryCustomizer::class.java).isEmpty())
        assertEquals(201, requestVerification(newUserId()).status)
        val executorVirtual = CompletableFuture.supplyAsync({ Thread.currentThread().isVirtual }, executor).get(5, TimeUnit.SECONDS)
        await.atMost(Duration.ofSeconds(30)).ignoreExceptions() untilAsserted {
            kafka.send(PROBE_TOPIC, "ping").get(5, TimeUnit.SECONDS)   // 리스너가 붙기 전에 보낸 것은 earliest 로 다시 읽는다
            assertNotNull(ThreadProbe.seen["kafka"])
        }
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertNotNull(ThreadProbe.seen["scheduled"]) }
        val seen = listOf("tomcat", "scheduled", "kafka").associateWith { ThreadProbe.seen[it]!! }
        println("[P7 실험 V1] 앱 실행기 virtual=$executorVirtual, " + seen.entries.joinToString { (k, t) -> "$k virtual=${t.isVirtual} ($t)" })
        assertTrue(executorVirtual, "applicationTaskExecutor 가 플랫폼 스레드다")
        seen.forEach { (where, t) -> assertTrue(t.isVirtual, "$where 가 플랫폼 스레드다: $t") }
    }

    @Test
    fun `V2 1원인증 두 판과 블로킹 출금 한 건을 끝까지 돌려도 pinning 이 없다 (기준 0ms)`() {
        // 첫 요청의 DispatcherServlet 지연 초기화는 Tomcat StandardWrapper.allocate 의 synchronized 안에서 돈다 —
        // 그 안의 로그가 출력 잠금에서 기다리면 pinning 이 잡힌다 (1회성, 이 실험에서 확인). 정상 상태의 요청 경로만 보려고 녹화 전에 한 번 끝내 둔다.
        assertEquals(201, requestVerification(newUserId()).status)
        // 공유 Kafka 프로듀서도 녹화 전에 만든다 — 첫 생성은 kafka-clients AppInfoParser.registerAppInfo(static synchronized) 안에서
        // "Kafka version" 로그를 남겨, 출력 잠금을 기다리면 같은 1회성 pinning 이 잡힌다 (CI 에서 2ms 1건, 이 클래스는 V2 가 V1 보다 먼저 돈다).
        // 메시지는 보내지 않는다 — 없는 토픽에 보내면 자동 생성이 리밸런스를 부른다. 있는 토픽의 파티션 정보만 묻는다.
        kafka.execute { it.partitionsFor(TransferTopics.REQUESTED) }
        // 가상 스레드 리스너가 리밸런스 중이면 Kafka 코디네이터의 synchronized 안 로그가 출력 잠금을 기다리며 pinned 된다
        // (캐리어 4개로 재현 — AbstractCoordinator.requestRejoin). 요청 경로가 아니라 컨슈머 시작 과정이라(그룹 합류 · 오프셋 초기화), 모두 시작을 끝낸 뒤 녹화한다.
        awaitStableAssignment()
        val pinned = recordPinned {
            assertEquals(201, requestVerification(newUserId()).status)   // async 판: Tomcat 요청 · 앱 실행기 뒤처리
            inVirtualThread {   // blocking 판 컨트롤러(BlockingVerificationRequestController)의 서비스 호출 부분 — HTTP 응답 직렬화는 거치지 않는다
                service.request(RequestVerification(newUserId(), "090", ACCOUNT, "홍길동")).join()
            }
            val user = fundedUser()
            val account = verifiedBy(user, ACCOUNT)
            val id = postTransfer(user, account)
            await.atMost(Duration.ofSeconds(20)) untilAsserted {
                inVirtualThread { relay.publishPending() }   // 운영에서는 예약 작업(가상 스레드)이 부른다
                assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
            }
        }
        println("[P7 실험 V2] pinning ${pinned.size}건" + pinned.joinToString("") { "\n" + describe(it) })
        assertEquals(0, pinned.size, pinned.joinToString("\n") { describe(it) })
    }

    @Test
    fun `V2 대조 - synchronized 안에서 sleep 하면 pinning 이 잡힌다 (검출이 동작하는지)`() {
        val lock = Any()
        val pinned = recordPinned { inVirtualThread { synchronized(lock) { Thread.sleep(30) } } }
        println("[P7 실험 V2 대조] pinning ${pinned.size}건")
        assertTrue(pinned.isNotEmpty(), "synchronized 안 sleep 이 잡히지 않았다 — 녹화 설정을 확인한다")
        assertTrue(
            pinned.any { e -> e.stackTrace?.frames?.any { it.method.type.name.contains("VirtualThreadExperimentTest") } == true },
            pinned.joinToString("\n") { describe(it) },
        )
    }

    private fun requestVerification(user: String) = send(
        client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("bankCode" to "090", "accountNo" to ACCOUNT, "holderName" to "홍길동")),
    )

    private fun postTransfer(user: String, account: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "P7"))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    /**
     * 이 컨텍스트의 리스너 컨슈머가 모두 시작을 끝낼 때까지: 파티션을 받고(그룹 합류 · 리밸런스 끝) 오프셋 초기화 뒤 첫 fetch 까지.
     * 둘 다 Kafka 컨슈머의 synchronized(AbstractCoordinator · SubscriptionState) 안에서 로그를 남겨 pinning 을 만든다 (캐리어 4개로 재현).
     * 컨테이너마다 동시성 3 · 파티션 3 이라 자식 컨슈머마다 1개씩 받는다. 탐침 리스너(V1 전용 토픽, 처음엔 토픽이 없다)는 뺀다.
     */
    private fun awaitStableAssignment() {
        await.atMost(Duration.ofSeconds(60)) untilAsserted {
            registry.allListenerContainers
                .filter { it.groupId != PROBE_GROUP }
                .flatMap { (it as ConcurrentMessageListenerContainer<*, *>).containers }
                .forEach {
                    assertFalse(it.assignedPartitions.isNullOrEmpty(), "${it.listenerId} 가 아직 파티션을 받지 못했다")
                    assertTrue(fetchCount(it) > 0, "${it.listenerId} 가 아직 첫 fetch 를 하지 않았다 (오프셋 초기화 중)")
                }
        }
    }

    private fun fetchCount(container: MessageListenerContainer): Double =
        container.metrics().values.flatMap { it.entries }
            .filter { (name, _) -> name.group() == "consumer-fetch-manager-metrics" && name.name() == "fetch-total" && name.tags().size == 1 }
            .sumOf { (_, metric) -> (metric.metricValue() as? Double) ?: 0.0 }

    /** [block] 을 도는 동안의 jdk.VirtualThreadPinned (기준 0ms, 스택 포함). stop() 은 그때까지의 이벤트를 모두 처리한 뒤 돌아온다 (JDK 20+). */
    private fun recordPinned(block: () -> Unit): List<RecordedEvent> {
        val events = CopyOnWriteArrayList<RecordedEvent>()
        RecordingStream().use { rs ->
            rs.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace()
            rs.onEvent("jdk.VirtualThreadPinned") { events += it }
            rs.startAsync()
            block()
            rs.stop()
        }
        return events
    }

    private fun <T> inVirtualThread(block: () -> T): T {
        var result: Result<T>? = null
        Thread.ofVirtual().start { result = runCatching(block) }.join()
        return result!!.getOrThrow()
    }

    private fun describe(e: RecordedEvent): String =
        "${e.duration.toMillis()}ms\n" + (e.stackTrace?.frames?.take(40)?.joinToString("\n") { "    ${it.method.type.name}.${it.method.name}:${it.lineNumber}" } ?: "    (스택 없음)")
}
