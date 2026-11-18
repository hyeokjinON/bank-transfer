package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.tracing.ScrubbingSpanExporter
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.http.client.ReactorResourceFactory

/**
 * 같은 JVM 에 앱을 여러 개 띄우는 테스트 전용 설정 — Boot 기본은 reactor-netty 의 전역 이벤트 루프를 서버가 같이 쓰는데,
 * 컨텍스트 하나를 닫으면 그 전역 자원이 같이 내려가 다른 앱의 서버까지 죽는다(확인함). 앱마다 자기 자원을 갖게 한다.
 */
@TestConfiguration(proxyBeanMethods = false)
class OwnReactorResources {
    @Bean
    fun reactorResourceFactory(): ReactorResourceFactory = ReactorResourceFactory().apply { isUseGlobalResources = false }
}

/** 계약 테스트용: 게이트웨이 앱을 판(서버 타입)별로 같은 JVM 에 임의 포트로 띄운다. */
object GatewayApps {
    /** 뒤에 오는 같은 키의 인자가 앞의 기본값을 덮는다 (명령행 인자를 같은 키로 두 번 주면 값이 "a,b" 로 합쳐져 바인딩이 깨진다). */
    fun start(server: WebApplicationType, bankPort: Int, vararg args: String): ConfigurableApplicationContext = run(server, bankPort, args, null)

    /** P9b: 끝낸 구간을 [exporter] 로 바로 넘기는 처리기를 단 채로 (운영과 같은 지우개를 지난다). Boot 의 spanProcessors 가 이 싱글톤도 모은다. */
    fun startTraced(server: WebApplicationType, bankPort: Int, exporter: SpanExporter, vararg args: String): ConfigurableApplicationContext =
        run(server, bankPort, args) { ctx ->
            ctx.beanFactory.registerSingleton("testSpanProcessor", SimpleSpanProcessor.create(ScrubbingSpanExporter(exporter)))
        }

    private fun run(
        server: WebApplicationType,
        bankPort: Int,
        args: Array<out String>,
        init: ((ConfigurableApplicationContext) -> Unit)?,
    ): ConfigurableApplicationContext {
        val merged = linkedMapOf(
            "server.port" to "0",
            "spring.main.web-application-type" to server.name.lowercase(),
            "bank.gateway.host" to "127.0.0.1",
            "bank.gateway.port" to "$bankPort",
            "bank.gateway.response-timeout" to "500ms",
            "bank.maintenance.enabled" to "false",
        )
        args.forEach { merged[it.removePrefix("--").substringBefore('=')] = it.substringAfter('=') }
        val builder = SpringApplicationBuilder(BankGatewayApplication::class.java, OwnReactorResources::class.java).web(server)
        if (init != null) builder.initializers(ApplicationContextInitializer<ConfigurableApplicationContext> { init(it) })
        return builder.run(*merged.map { (k, v) -> "--$k=$v" }.toTypedArray())
    }
}

fun ConfigurableApplicationContext.port(): Int = environment.getProperty("local.server.port")!!.toInt()

/** 지금 리더의 은행 클라이언트 (P8-3b — 클라이언트는 빈이 아니라 Owner 가 임기마다 만든다). */
fun ConfigurableApplicationContext.bankClient(): BankGatewayClient =
    getBean(BankConnectionOwner::class.java).current() ?: error("this gateway is not the bank connection leader")
