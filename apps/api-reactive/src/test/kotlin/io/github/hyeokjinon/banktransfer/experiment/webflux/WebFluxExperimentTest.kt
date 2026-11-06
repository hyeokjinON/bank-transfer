package io.github.hyeokjinon.banktransfer.experiment.webflux

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryResponse
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.client.RestClient
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.server.adapter.WebHttpHandlerBuilder
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServer
import java.net.Socket
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * P6-2 진행 조건 실험 (설계 §4-1). 문서만으로 확신할 수 없는 네 가지를 진짜 모의 은행 · 게이트웨이로 본다:
 * E1 await 뒤 스레드, E2 클라이언트 끊김 → 취소, E3 취소가 게이트웨이 future 까지 가는지, E4 예외가 벗겨져 오는지.
 * 결과는 ADR-0013 에 옮긴다. 남겨 둔다 — 프레임워크를 올렸을 때 같은 동작인지 다시 본다.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebFluxExperimentTest {

    /** 컨트롤러가 본 것. 테스트마다 [reset]. */
    class Probe {
        val threadAfterAwait = AtomicReference<String?>()
        val error = AtomicReference<Throwable?>()
        val future = AtomicReference<CompletableFuture<*>?>()
        @Volatile var cancelled = CountDownLatch(1)
        fun reset() { threadAfterAwait.set(null); error.set(null); future.set(null); cancelled = CountDownLatch(1) }
    }

    @RestController
    class ExperimentController(private val client: BankGatewayClient, private val probe: Probe) {

        @GetMapping("/coroutine/name")
        suspend fun coroutineName(@RequestParam account: String): String {
            val f = client.inquireName("090", account).also { probe.future.set(it) }
            try {
                val res = f.await()
                probe.threadAfterAwait.set(Thread.currentThread().name)
                return res.holderName
            } catch (e: CancellationException) {
                probe.cancelled.countDown()
                throw e
            } catch (e: Exception) {
                probe.error.set(e)
                throw e
            }
        }

        @GetMapping("/reactor/name")
        fun reactorName(@RequestParam account: String): Mono<String> =
            Mono.fromFuture { client.inquireName("090", account).also { probe.future.set(it) } }
                .map { res: NameInquiryResponse -> probe.threadAfterAwait.set(Thread.currentThread().name); res.holderName }
                .doOnCancel { probe.cancelled.countDown() }
                .doOnError { probe.error.set(it) }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebFlux
    class ExperimentConfig {
        @Bean fun probe() = Probe()
        @Bean fun controller(client: BankGatewayClient, probe: Probe) = ExperimentController(client, probe)
    }

    private val bank = MockBankServer(silentAccounts = setOf(SILENT)).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val context = AnnotationConfigApplicationContext().apply {
        beanFactory.registerSingleton("bankGatewayClient", gateway)
        register(ExperimentConfig::class.java)
        refresh()
    }
    private val server = HttpServer.create().port(0).handle(ReactorHttpHandlerAdapter(WebHttpHandlerBuilder.applicationContext(context).build())).bindNow()
    private val probe = context.getBean(Probe::class.java)
    private val http = RestClient.create("http://localhost:${server.port()}")

    @BeforeEach
    fun setUp() {
        probe.reset()
        bank.responseDelay = Duration.ZERO
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { server.disposeNow(); context.close(); gateway.close(); bank.close() }

    /** 요청을 보낸 뒤 응답을 읽지 않고 [closeAfterMs] 뒤 연결을 끊는다 (브라우저 닫힘 · 클라이언트 시간 초과와 같다). */
    private fun requestAndDisconnect(path: String, closeAfterMs: Long) {
        Socket("localhost", server.port()).use { s ->
            s.getOutputStream().write("GET $path HTTP/1.1\r\nHost: localhost\r\n\r\n".toByteArray())
            s.getOutputStream().flush()
            Thread.sleep(closeAfterMs)
        }
    }

    private fun threadOnly(name: String?) = name!!.substringBefore(" @coroutine")   // 코루틴 디버그 모드(-ea)가 붙이는 꼬리를 뗀다

    /**
     * E1 은 두 경우로 나뉜다 (2026-11-06, CI 에서 관찰): Spring 은 suspend 핸들러를 Dispatchers.Unconfined 로 부르므로
     * - 은행 응답이 await(구독) **뒤에** 오면 → 응답을 완료한 게이트웨이 이벤트 루프에서 이어진다 (은행 지연을 둬서 결정적으로 만든다)
     * - 응답이 이미 와 있으면(localhost 라 아주 빠를 때) → 멈추지 않고 부른 스레드, 곧 HTTP 루프(reactor-http-…)에서 그대로 이어진다
     * 어느 쪽이든 **이벤트 루프**다 — 그래서 JDBC 는 늘 IO 디스패처 · boundedElastic 으로 넘긴다 (설계 §4-2).
     */
    private fun isEventLoop(thread: String) = thread.startsWith("multiThreadIoEventLoopGroup") || thread.startsWith("reactor-http")

    @Test
    fun `E1 코루틴 - 응답이 기다리는 중에 오면 await 뒤 코드는 응답을 완료한 게이트웨이 이벤트 루프에서 이어진다`() {
        bank.responseDelay = Duration.ofMillis(100)
        assertEquals("홍길동", http.get().uri("/coroutine/name?account=1002123456789").retrieve().body(String::class.java))
        val after = threadOnly(probe.threadAfterAwait.get())
        println("[P6-2 실험 E1 코루틴] await 뒤 = $after")
        assertTrue(after.startsWith("multiThreadIoEventLoopGroup"), "게이트웨이 루프(Netty 기본 이름)여야 한다: $after")
    }

    @Test
    fun `E1 Reactor - 응답이 기다리는 중에 오면 fromFuture 뒤 map 은 게이트웨이 이벤트 루프에서 돈다`() {
        bank.responseDelay = Duration.ofMillis(100)
        assertEquals("홍길동", http.get().uri("/reactor/name?account=1002123456789").retrieve().body(String::class.java))
        val after = threadOnly(probe.threadAfterAwait.get())
        println("[P6-2 실험 E1 Reactor] map = $after")
        assertTrue(after.startsWith("multiThreadIoEventLoopGroup"), "게이트웨이 루프여야 한다: $after")
    }

    @Test
    fun `E1 지연 없음 - 어느 판이든 기다린 뒤 코드는 이벤트 루프(게이트웨이 또는 HTTP)에서 돈다`() {
        for (path in listOf("/coroutine/name", "/reactor/name")) {
            probe.reset()
            assertEquals("홍길동", http.get().uri("$path?account=1002123456789").retrieve().body(String::class.java))
            val after = threadOnly(probe.threadAfterAwait.get())
            println("[P6-2 실험 E1 지연 없음] $path → $after")
            assertTrue(isEventLoop(after), "이벤트 루프여야 한다 (JDBC 를 여기서 기다리면 안 되는 이유): $after")
        }
    }

    @Test
    fun `E2 E3 코루틴 - 응답 전에 끊으면 코루틴이 취소되고 게이트웨이 future 도 취소된다`() {
        bank.responseDelay = Duration.ofMillis(400)
        requestAndDisconnect("/coroutine/name?account=1002123456789", closeAfterMs = 100)
        assertTrue(probe.cancelled.await(5, TimeUnit.SECONDS), "끊김이 코루틴 취소로 전파돼야 한다")
        assertTrue(probe.future.get()!!.isCancelled, "await 가 취소될 때 받은 future 도 취소해야 한다")
        println("[P6-2 실험 E2 · E3 코루틴] 취소됨, future.isCancelled = true")
    }

    @Test
    fun `E2 E3 Reactor - 응답 전에 끊으면 구독이 취소되고 게이트웨이 future 도 취소된다`() {
        bank.responseDelay = Duration.ofMillis(400)
        requestAndDisconnect("/reactor/name?account=1002123456789", closeAfterMs = 100)
        assertTrue(probe.cancelled.await(5, TimeUnit.SECONDS), "끊김이 구독 취소로 전파돼야 한다")
        assertTrue(probe.future.get()!!.isCancelled, "fromFuture 가 취소될 때 future 도 취소해야 한다 (suppressCancel = false)")
        println("[P6-2 실험 E2 · E3 Reactor] 취소됨, future.isCancelled = true")
    }

    @Test
    fun `E4 코루틴 - 은행 시간 초과는 CompletionException 에 싸이지 않고 원래 예외로 온다`() {
        runCatching { http.get().uri("/coroutine/name?account=$SILENT").retrieve().toBodilessEntity() }
        assertIs<BankTimeoutException>(probe.error.get(), "await 는 CompletionException 을 벗긴다")
    }

    @Test
    fun `E4 Reactor - 은행 시간 초과는 CompletionException 에 싸이지 않고 원래 예외로 온다`() {
        runCatching { http.get().uri("/reactor/name?account=$SILENT").retrieve().toBodilessEntity() }
        assertIs<BankTimeoutException>(probe.error.get(), "fromFuture 는 CompletionException 을 벗긴다")
    }

    private companion object {
        const val SILENT = "9990000000001"   // 성명조회부터 무응답 (silentAccounts)
    }
}
