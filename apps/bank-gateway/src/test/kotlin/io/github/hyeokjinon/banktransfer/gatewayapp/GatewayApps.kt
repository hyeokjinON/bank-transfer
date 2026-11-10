package io.github.hyeokjinon.banktransfer.gatewayapp

import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.TestConfiguration
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
    fun start(server: WebApplicationType, bankPort: Int, vararg args: String): ConfigurableApplicationContext {
        val merged = linkedMapOf(
            "server.port" to "0",
            "spring.main.web-application-type" to server.name.lowercase(),
            "bank.gateway.host" to "127.0.0.1",
            "bank.gateway.port" to "$bankPort",
            "bank.gateway.response-timeout" to "500ms",
            "bank.maintenance.enabled" to "false",
        )
        args.forEach { merged[it.removePrefix("--").substringBefore('=')] = it.substringAfter('=') }
        return SpringApplicationBuilder(BankGatewayApplication::class.java, OwnReactorResources::class.java)
            .web(server)
            .run(*merged.map { (k, v) -> "--$k=$v" }.toTypedArray())
    }
}

fun ConfigurableApplicationContext.port(): Int = environment.getProperty("local.server.port")!!.toInt()
