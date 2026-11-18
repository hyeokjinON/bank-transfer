package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.api.tracing.TraceKeys
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.http.GatewayPaths
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.opentelemetry.api.trace.SpanKind
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/** mode=http + 코루틴 컨슈머 (P6-1 판): suspend 어댑터가 HTTP 판 future 를 await 해도 같은 결과. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayCoroutineFlowTest : ApiIntegrationTest() {

    companion object {
        // Lazy 를 따로 쥔다: 정리에서 "띄운 것만" 닫고, 정리하려고 게이트웨이를 새로 띄우지 않는다
        private val gatewayHolder = lazy { GatewayProcess.start(bank.boundPort, "--bank.gateway.response-timeout=500ms") }
        private val gatewayApp: GatewayProcess get() = gatewayHolder.value

        @JvmStatic
        @DynamicPropertySource
        fun httpGateway(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.mode") { "http" }
            registry.add("bank.gateway.http.base-url") { gatewayApp.baseUrl }
            registry.add("bank.gateway.http.response-timeout") { "2s" }
            registry.add("transfer.consumer.mode") { "coroutine" }
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() { if (gatewayHolder.isInitialized()) gatewayHolder.value.close() }
    }

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var spans: TestSpans

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @Test
    fun `코루틴 컨슈머도 게이트웨이 앱을 거쳐 COMPLETED`() {
        val user = fundedUser(100_000)
        val account = verifiedBy(user, "1002123456789")
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "HC1"))
            .retrieve().body(object : ParameterizedTypeReference<Map<String, Any?>>() {})!!
        val id = UUID.fromString(body["id"] as String)
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
    }

    @Test
    fun `코루틴 컨슈머 - suspend 뒤 db 스레드에서 연 게이트웨이 호출도 접수 trace 에 (TraceContextElement)`() {
        spans.clear()
        val user = fundedUser(100_000)
        val account = verifiedBy(user, "1002123456789")
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "HC2"))
            .retrieve().body(object : ParameterizedTypeReference<Map<String, Any?>>() {})!!
        val id = UUID.fromString(body["id"] as String)
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            val server = assertNotNull(spans.all().singleOrNull { it.kind == SpanKind.SERVER && it.attr(TraceKeys.TRANSFER_ID) == id.toString() })
            val trace = spans.ofTrace(server.traceId)
            val process = assertNotNull(trace.singleOrNull { it.kind == SpanKind.CONSUMER })
            assertEquals(id.toString(), process.attr(TraceKeys.TRANSFER_ID))   // 코루틴 리스너의 태그 (Task 3 에서 빠졌던 검증)
            val clientSpan = assertNotNull(trace.singleOrNull { it.kind == SpanKind.CLIENT && it.attr("uri") == GatewayPaths.TRANSFERS })
            assertEquals(process.spanId, clientSpan.parentSpanId)
        }
    }
}
