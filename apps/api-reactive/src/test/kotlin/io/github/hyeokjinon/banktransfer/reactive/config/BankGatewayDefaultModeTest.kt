package io.github.hyeokjinon.banktransfer.reactive.config

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

/** apps/api 의 BankGatewayDefaultModeTest 와 같다 (P8-2): 리액티브 앱도 설정 없이 뜨면 mode=http. */
class BankGatewayDefaultModeTest {

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(BankGatewayProperties::class)
    class PropertiesOnly

    private val runner = ApplicationContextRunner()
        .withInitializer(ConfigDataApplicationContextInitializer())
        .withBean(LazyInitializationBeanFactoryPostProcessor::class.java)
        .withUserConfiguration(PropertiesOnly::class.java, ReactiveConfiguration::class.java)

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
    fun `direct 를 명시하면 BankGatewayClient 만`() {
        runner.withPropertyValues("bank.gateway.mode=direct").run { ctx ->
            assertNull(ctx.startupFailure)
            assertEquals(1, ctx.getBeanNamesForType(BankGatewayClient::class.java).size)
            assertEquals(0, ctx.getBeanNamesForType(HttpBankGateway::class.java).size)
        }
    }
}
