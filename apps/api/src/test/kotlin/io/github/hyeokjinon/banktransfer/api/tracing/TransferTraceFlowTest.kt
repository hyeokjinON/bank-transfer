package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.trace.data.SpanData
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 출금 trace — 한 프로세스(직결) 안 (P9b 스펙 §8 통합 1 · 2, E10). 게이트웨이 HTTP 구간은 별도 JVM 흐름 테스트(HttpGatewayFlowTest)가 본다.
 * 접수 server → outbox publish(저장한 traceparent) → Kafka send → process 가 같은 traceId 로 부모-자식, 재시도 토픽 · DLT 처리도 같은 trace.
 */
@ExtendWith(OutputCaptureExtension::class)
class TransferTraceFlowTest : ApiIntegrationTest() {
    @Autowired private lateinit var spans: TestSpans
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>

    private val user by lazy { fundedUser(1_000_000) }

    private fun serverSpanOf(id: UUID): SpanData =
        await.atMost(Duration.ofSeconds(5)) untilNotNull {
            spans.all().singleOrNull { it.kind == SpanKind.SERVER && it.attr(TraceKeys.TRANSFER_ID) == id.toString() }
        }

    private fun awaitCompleted(id: UUID) {
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
    }

    @Test
    fun `출금 1건 - 접수 server → outbox publish → Kafka send → process 가 trace 하나, 부모-자식 순서, 로그 줄에 traceId`(output: CapturedOutput) {
        spans.clear()
        val id = postTransfer(user, verifiedBy(user, "1002123456789"))
        val server = serverSpanOf(id)
        val stored = jdbc.queryForObject("SELECT trace_parent FROM outbox_event WHERE aggregate_id = ?", String::class.java, id.toString())
        assertNotNull(stored)
        assertEquals("00-${server.traceId}-${server.spanId}-", stored.take(53))
        assertEquals(1, stored.takeLast(2).toInt(16) and 1)   // sampled 비트 (끝 깃발은 OTel 판에 따라 01 또는 03)

        awaitCompleted(id)

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val trace = spans.ofTrace(server.traceId)
            val publish = assertNotNull(trace.singleOrNull { it.name == OutboxTracing.SPAN_NAME })
            val send = assertNotNull(trace.singleOrNull { it.kind == SpanKind.PRODUCER })
            val process = assertNotNull(trace.singleOrNull { it.kind == SpanKind.CONSUMER })
            assertEquals(server.spanId, publish.parentSpanId)
            assertEquals(publish.spanId, send.parentSpanId)
            assertEquals(send.spanId, process.parentSpanId)
            assertEquals("transfer.requested process", process.name)
            assertEquals(id.toString(), publish.attr(TraceKeys.TRANSFER_ID))
            assertEquals(id.toString(), process.attr(TraceKeys.TRANSFER_ID))
        }
        // 로그 상관 (스펙 §3): 컨슈머의 처리 로그 한 줄에 같은 traceId — Boot 기본 상관 패턴
        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            assertTrue(output.out.lines().any { "transfer $id processed" in it && server.traceId in it }, "처리 로그 줄에 traceId 가 없다")
        }
    }

    @Test
    fun `은행 끊김 - 재시도 토픽 3단 · DLT 처리 구간도 접수와 같은 trace`() {
        spans.clear()
        val account = verifiedBy(user, "1002123456789")
        val id = withBankDown {
            val id = postTransfer(user, account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString())
            id
        }
        val server = serverSpanOf(id)

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val processed = spans.ofTrace(server.traceId).filter { it.kind == SpanKind.CONSUMER }.map { it.name }.toSet()
            assertEquals(
                setOf(
                    "transfer.requested process", "transfer.requested-retry-0 process", "transfer.requested-retry-1 process",
                    "transfer.requested-retry-2 process", "transfer.requested-dlt process",
                ),
                processed,
            )
        }
    }

    @Test
    fun `아웃박스 trace_parent 가 틀려도 발행 · 처리는 그대로 - outbox publish 는 새 trace (E10)`() {
        spans.clear()
        val id = postTransfer(user, verifiedBy(user, "1002123456789"))
        val server = serverSpanOf(id)
        jdbc.update("UPDATE outbox_event SET trace_parent = 'not-a-traceparent' WHERE aggregate_id = ?", id.toString())

        awaitCompleted(id)

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val publish = assertNotNull(spans.all().singleOrNull { it.name == OutboxTracing.SPAN_NAME && it.attr(TraceKeys.TRANSFER_ID) == id.toString() })
            assertNotEquals(server.traceId, publish.traceId)
            assertFalse(publish.parentSpanContext.isValid)
        }
    }
}
