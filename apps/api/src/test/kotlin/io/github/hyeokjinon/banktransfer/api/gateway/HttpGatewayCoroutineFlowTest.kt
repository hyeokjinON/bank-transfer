package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
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
}
