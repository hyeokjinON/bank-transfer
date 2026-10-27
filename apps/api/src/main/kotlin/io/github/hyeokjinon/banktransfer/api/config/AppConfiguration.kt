package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.BankPort
import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.api.persistence.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.JpaTransferStore
import io.github.hyeokjinon.banktransfer.api.persistence.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.api.persistence.OutboxEventJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.TransferJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.TransferService
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.github.hyeokjinon.banktransfer.transfer.VerifiedAccounts
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
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
class BankGatewayAdapter(
    private val client: BankGatewayClient,
    private val executor: Executor,
    /** 1원인증 이체의 이용기관 거래번호 (V 접두). 결과조회 대상이 아니라 저장하지 않는다. */
    private val references: ReferenceGenerator = RandomReferenceGenerator(prefix = 'V'),
) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res ->
            when (res.header.responseCode) {
                ResponseCodes.OK -> res.holderName
                ResponseCodes.ACCOUNT_NOT_FOUND -> null
                else -> throw BankRejectedException(res.header.responseCode)
            }
        }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo, references.next()).thenApplyAsync({ res ->
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

    /** 잔고 SQL 한 곳 (P4-1). 실패 확정 트랜잭션 안에서 JpaTransferStore 가 환불을 부르고, 충전 · 조회는 WalletController 가 부른다. */
    @Bean
    fun walletLedger(jdbc: NamedParameterJdbcTemplate, tx: TransactionTemplate) = WalletLedger(jdbc, tx, Clock.systemUTC())

    @Bean
    fun transferStore(
        transfers: TransferJpaRepository,
        outbox: OutboxEventJpaRepository,
        tx: TransactionTemplate,
        wallets: WalletLedger,
        debit: WalletDebit,
    ): TransferStore = JpaTransferStore(transfers, outbox, tx, wallets, debit)

    /** transfer 모듈은 account 모듈을 모른다. "인증된 계좌인가?"를 1원인증 저장소에 연결하는 곳은 여기뿐이다. */
    @Bean
    fun transferService(store: TransferStore, verifications: VerificationRepository) = TransferService(
        store = store,
        verifiedAccounts = VerifiedAccounts { userId, bankCode, accountNo -> verifications.existsVerified(userId, bankCode, accountNo) },
        referenceGenerator = RandomReferenceGenerator(prefix = 'T'),
        clock = Clock.systemUTC(),
    )

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
