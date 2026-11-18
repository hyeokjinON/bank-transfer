package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.api.support.service
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import io.micrometer.tracing.Tracer
import io.opentelemetry.api.trace.SpanKind
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** P9b 자동 구간 (스펙 §3 · §5): MVC 서버 · KafkaTemplate · 리스너는 설정만으로 구간이 되고 traceparent 헤더로 이어진다. @Scheduled 는 구간을 만들지 않는다. */
class AutoSpansTest : ApiIntegrationTest() {
    @Autowired private lateinit var spans: TestSpans
    @Autowired private lateinit var kafka: KafkaTemplate<String, String>
    @Autowired private lateinit var tracer: Tracer
    @Autowired private lateinit var observations: ObservationRegistry

    @Test
    fun `HTTP 요청은 SERVER 구간 - 서비스 이름은 spring application name`() {
        spans.clear()
        val user = fundedUser(1_000)
        client.get().uri("/api/v1/wallet").header(USER_ID_HEADER, user).retrieve().toBodilessEntity()

        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            val server = assertNotNull(spans.all().singleOrNull { it.kind == SpanKind.SERVER && it.attr("uri") == "/api/v1/wallet" })
            assertEquals("http get /api/v1/wallet", server.name)
            assertEquals("bank-transfer", server.service())
        }
    }

    @Test
    fun `KafkaTemplate 발행 · 리스너 소비가 구간이 되고 traceparent 헤더로 이어진다`() {
        spans.clear()
        val parent = tracer.nextSpan().name("test-parent").start()
        val key = UUID.randomUUID().toString()   // 없는 이체 id — 컨슈머는 NOT_FOUND 로 끝낸다 (돈 영향 없음)
        tracer.withSpan(parent).use { kafka.send(TransferTopics.REQUESTED, key, """{"transferId":"$key"}""").get(5, TimeUnit.SECONDS) }
        parent.end()

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val trace = spans.ofTrace(parent.context().traceId())
            val send = assertNotNull(trace.singleOrNull { it.kind == SpanKind.PRODUCER })
            val process = assertNotNull(trace.singleOrNull { it.kind == SpanKind.CONSUMER })
            assertEquals("transfer.requested send", send.name)
            assertEquals("transfer.requested process", process.name)
            assertEquals(parent.context().spanId(), send.parentSpanId)
            assertEquals(send.spanId, process.parentSpanId)
        }
    }

    @Test
    fun `@Scheduled 실행 관측은 꺼져 있다 - 발행기 0점5초마다 빈 trace 가 쌓이지 않게`() {
        assertTrue(Observation.createNotStarted("tasks.scheduled.execution", observations).isNoop)
        assertFalse(Observation.createNotStarted("http.server.requests", observations).isNoop)
    }
}
