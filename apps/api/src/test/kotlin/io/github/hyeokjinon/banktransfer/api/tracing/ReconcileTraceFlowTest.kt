package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.messaging.TransferTopics
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.api.support.awaitRecords
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.SpanKind
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.kafka.core.ConsumerFactory
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 정리 작업 trace (P9b 스펙 §4-2 · §8 통합 3, 결정 E7): 실행 1회 = 새 root `transfer.reconcile`. 재발행 · 결과조회 구간에 transfer.id —
 * 처음 trace 와 정리 작업 trace 를 transfer.id 한 번 검색으로 둘 다 찾는다 (성공 기준 3).
 */
class ReconcileTraceFlowTest : ApiIntegrationTest() {
    @Autowired private lateinit var spans: TestSpans
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired private lateinit var reconcileTracing: ReconcileTracing

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}
    private val user by lazy { fundedUser(1_000_000) }

    private fun request(accountNo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 10_000, "memo" to "RC" + Random.nextInt(100_000, 999_999)))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    /** 운영 조립과 같은 건별 범위. 대기만 줄인다 (RepublishFlowTest 와 같은 이유 — 다른 테스트의 RECEIVED 도 후보라 배치를 키운다). */
    private fun reconciler(clock: Clock, republishAfter: Duration = Duration.ofMinutes(5)) = TransferReconciler(
        store, BankInquiryAdapter(gateway), clock, republishAfter = republishAfter, batchSize = 1_000, itemScope = reconcileTracing,
    )

    private fun tracesWith(id: UUID): Set<String> = spans.all().filter { it.attr(TraceKeys.TRANSFER_ID) == id.toString() }.map { it.traceId }.toSet()

    @Test
    fun `재발행 - 정리 작업 실행이 새 trace, transfer id 로 처음 trace 와 정리 작업 trace 를 둘 다 찾는다`() {
        spans.clear()
        val account = verifiedBy(user, "1002123456789")
        val id = withBankDown {
            val id = request(account)
            relay.publishPending()
            consumerFactory.awaitRecords(TransferTopics.DLT, id.toString())
            id
        }

        reconcileTracing.reconcile { reconciler(Clock.systemUTC(), republishAfter = Duration.ofMillis(1)).reconcile() }
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val traces = tracesWith(id)
            assertEquals(2, traces.size, "처음 trace + 정리 작업 trace")
            val rootTrace = traces.firstOrNull { t -> spans.ofTrace(t).any { it.name == ReconcileTracing.ROOT } }
            assertNotNull(rootTrace, "정리 작업 trace")
            val trace = spans.ofTrace(rootTrace)
            val root = trace.single { it.name == ReconcileTracing.ROOT }
            assertFalse(root.parentSpanContext.isValid)   // 처음 trace 에 이어 붙이지 않는다 (E7)
            val republish = trace.singleOrNull { it.name == ReconcileTracing.REPUBLISH && it.attr(TraceKeys.TRANSFER_ID) == id.toString() }
            assertNotNull(republish)
            assertEquals(root.spanId, republish.parentSpanId)
            val publish = trace.singleOrNull { it.name == OutboxTracing.SPAN_NAME && it.attr(TraceKeys.TRANSFER_ID) == id.toString() }
            assertNotNull(publish)
            assertEquals(republish.spanId, publish.parentSpanId)
            assertTrue(trace.any { it.kind == SpanKind.CONSUMER && it.attr(TraceKeys.TRANSFER_ID) == id.toString() })
        }
    }

    @Test
    fun `결과조회 - 정리 작업 trace 안에 transfer inquiry 구간 (transfer id · 거래번호)`() {
        spans.clear()
        val id = request(verifiedBy(user, "9990000000001"))   // 모의 은행 무응답 · 처리 안 함 → 컨슈머 UNKNOWN
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.UNKNOWN, store.findById(id)!!.status)
        }

        reconcileTracing.reconcile { reconciler(Clock.offset(Clock.systemUTC(), Duration.ofSeconds(31))).reconcile() }

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val inquiry = spans.all().singleOrNull { it.name == ReconcileTracing.INQUIRY && it.attr(TraceKeys.TRANSFER_ID) == id.toString() }
            assertNotNull(inquiry)
            assertEquals(store.findById(id)!!.reference, inquiry.attr(TraceKeys.TRANSFER_REFERENCE))
            val root = spans.ofTrace(inquiry.traceId).singleOrNull { it.name == ReconcileTracing.ROOT }
            assertNotNull(root)
            assertEquals(root.spanId, inquiry.parentSpanId)
            // 건별 구간 수 — 빈 실행 거르기가 자식 있는 root 를 지우지 않게 하는 값 (m4). 공유 DB 라 다른 건이 같이 잡힐 수 있어 하한만 본다
            val items = root.attributes.get(AttributeKey.longKey("reconcile.items"))
            assertNotNull(items)
            assertTrue(items >= 1, "reconcile.items=$items")
        }
    }
}
