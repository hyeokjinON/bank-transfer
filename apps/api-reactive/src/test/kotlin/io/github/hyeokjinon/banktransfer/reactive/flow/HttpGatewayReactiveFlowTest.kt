package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.reactive.support.ReactiveIntegrationTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import kotlin.test.assertEquals

/** api-reactive mode=http (P8-1): 코루틴 판 1원인증이 게이트웨이 앱을 거쳐 SENT. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayReactiveFlowTest : ReactiveIntegrationTest() {

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
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() { if (gatewayHolder.isInitialized()) gatewayHolder.value.close() }
    }

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @Test
    fun `1원인증 요청이 게이트웨이 앱을 거쳐 201 SENT`() {
        val userId = newUserId()
        val (status, body) = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header("X-User-Id", userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() to (res.bodyTo(object : ParameterizedTypeReference<Map<String, Any?>>() {}) ?: emptyMap()) }
        assertEquals(201, status)
        assertEquals("SENT", body["status"])
        assertEquals(1, rowsOf(userId))
    }
}
