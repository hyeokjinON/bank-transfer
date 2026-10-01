package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.BankPort
import io.github.hyeokjinon.banktransfer.account.InMemoryVerificationRepository
import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture

@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

@ConfigurationProperties("account.verification")
data class VerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val maxAttempts: Int = 5,
)

/** 은행이 처리를 거절한 경우(응답코드가 0000 이 아님). */
class BankRejectedException(val responseCode: String) : RuntimeException("bank rejected with code $responseCode")

/**
 * 조립(wiring)은 여기서만 한다. account 모듈은 BankPort 인터페이스만 알고,
 * 실제로 Netty 게이트웨이를 붙이는 건 이 어댑터다 (포트-어댑터 구조).
 */
class BankGatewayAdapter(private val client: BankGatewayClient) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApply { res ->
            when (res.header.responseCode) {
                ResponseCodes.OK -> res.holderName
                ResponseCodes.ACCOUNT_NOT_FOUND -> null
                else -> throw BankRejectedException(res.header.responseCode)
            }
        }

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo).thenApply { res ->
            if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
        }
}

@Configuration(proxyBeanMethods = false)
class AppConfiguration {

    @Bean(destroyMethod = "close")
    fun bankGatewayClient(props: BankGatewayProperties): BankGatewayClient =
        BankGatewayClient(
            BankGatewayConfig(
                host = props.host,
                port = props.port,
                responseTimeout = props.responseTimeout,
                pollInterval = props.pollInterval,
            ),
        ).start()

    @Bean
    fun bankPort(client: BankGatewayClient): BankPort = BankGatewayAdapter(client)

    @Bean
    fun verificationRepository(): VerificationRepository = InMemoryVerificationRepository()

    @Bean
    fun accountVerificationService(
        bankPort: BankPort,
        repository: VerificationRepository,
        props: VerificationProperties,
    ) = AccountVerificationService(
        bank = bankPort,
        repository = repository,
        codeGenerator = RandomCodeGenerator(),
        clock = Clock.systemUTC(),
        ttl = props.ttl,
        maxAttempts = props.maxAttempts,
    )
}
