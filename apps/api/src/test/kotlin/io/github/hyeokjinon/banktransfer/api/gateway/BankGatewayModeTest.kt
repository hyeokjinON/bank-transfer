package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.config.BankGatewayProperties
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BankGatewayModeTest {
    @Test
    fun `기본은 direct, http 설정 기본값은 스펙 §6`() {
        val p = BankGatewayProperties()
        assertEquals("direct", p.mode)
        assertEquals("http://localhost:8082", p.http.baseUrl)
        assertEquals("webclient", p.http.client)
        assertEquals(500, p.http.maxConnections)
        assertEquals(Duration.ofSeconds(5), p.http.toSettings().responseTimeout)
    }

    @Test
    fun `모드 · 클라이언트는 소문자 목록 안에서만`() {
        assertFailsWith<IllegalArgumentException> { BankGatewayProperties(mode = "HTTP") }
        assertFailsWith<IllegalArgumentException> { BankGatewayProperties.Http(client = "okhttp") }
        assertEquals("jdk", BankGatewayProperties.Http(client = "jdk").client)
    }
}
