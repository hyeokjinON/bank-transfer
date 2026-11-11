package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.config.AppConfiguration
import io.github.hyeokjinon.banktransfer.api.config.BankGatewayProperties
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * P8-2 (스펙 §3-7 · §7-1): 설정 없이 뜨면 mode=http — 게이트웨이 앱을 부르는 HttpBankGateway 하나, Netty 직결 클라이언트는 없다.
 * application.yml 을 그대로 읽고(ConfigDataApplicationContextInitializer) AppConfiguration 의 빈 선택만 본다.
 * 나머지 빈(JPA 저장소 · 실행기 …)은 만들지 않게 지연 초기화한다 — 조건 평가는 빈 정의를 등록할 때 끝나므로 선택 결과는 실제 앱과 같다.
 */
class BankGatewayDefaultModeTest {

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(BankGatewayProperties::class)
    class PropertiesOnly

    private val runner = ApplicationContextRunner()
        .withInitializer(ConfigDataApplicationContextInitializer())
        .withBean(LazyInitializationBeanFactoryPostProcessor::class.java)
        .withUserConfiguration(PropertiesOnly::class.java, AppConfiguration::class.java)

    @Test
    fun `설정 없이 뜨면 http - HttpBankGateway 하나, BankGatewayClient 없음`() {
        runner.run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals("http", ctx.getBean(BankGatewayProperties::class.java).mode)
            assertNotNull(ctx.getBean(HttpBankGateway::class.java))
            assertEquals(0, ctx.getBeanNamesForType(BankGatewayClient::class.java).size)
        }
    }

    @Test
    fun `direct 를 명시하면 BankGatewayClient 만 - 테스트 공통 기반 · 측정이 쓰는 길`() {
        runner.withPropertyValues("bank.gateway.mode=direct").run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(1, ctx.getBeanNamesForType(BankGatewayClient::class.java).size)
            assertEquals(0, ctx.getBeanNamesForType(HttpBankGateway::class.java).size)
        }
    }
}
