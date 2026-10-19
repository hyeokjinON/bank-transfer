package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.BankPort
import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.api.persistence.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

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
 *
 * 은행 응답 future 는 Netty 이벤트 루프가 완료한다. 그 뒤의 처리(DB 저장 등 블로킹)가 이벤트 루프에서 돌지 않도록
 * thenApplyAsync 로 앱 실행기에 넘긴다. 이벤트 루프가 막히면 같은 루프의 모든 은행 통신이 멈춘다.
 */
class BankGatewayAdapter(private val client: BankGatewayClient, private val executor: Executor) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res ->
            when (res.header.responseCode) {
                ResponseCodes.OK -> res.holderName
                ResponseCodes.ACCOUNT_NOT_FOUND -> null
                else -> throw BankRejectedException(res.header.responseCode)
            }
        }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo).thenApplyAsync({ res ->
            if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
        }, executor)
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
    fun bankPort(client: BankGatewayClient, @Qualifier("applicationTaskExecutor") executor: Executor): BankPort =
        BankGatewayAdapter(client, executor)

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate): VerificationRepository =
        JpaVerificationRepository(jpa, tx, Clock.systemUTC())

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
