package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.account.AccountVerificationService
import io.github.hyeokjinon.banktransfer.account.BankPort
import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.infra.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.account.infra.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.account.infra.holderNameOf
import io.github.hyeokjinon.banktransfer.account.infra.requireOk
import io.github.hyeokjinon.banktransfer.api.persistence.JpaTransferStore
import io.github.hyeokjinon.banktransfer.api.persistence.OutboxEventJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.TransferJpaRepository
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.TransferService
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.github.hyeokjinon.banktransfer.transfer.VerifiedAccounts
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

/**
 * 은행 게이트웨이 (P8-1). mode: direct(기본 — 이 프로세스가 Netty 로 FEP 에 직결) | http(게이트웨이 앱을 HTTP 로 부름, apps/bank-gateway).
 * host · port · response-timeout · poll-interval 은 direct 일 때만 쓴다. http.* 는 http 일 때만 (스펙 §6).
 */
@ConfigurationProperties("bank.gateway")
data class BankGatewayProperties(
    val mode: String = DIRECT,
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
    val http: Http = Http(),
) {
    init {
        require(mode in MODES) { "bank.gateway.mode must be one of $MODES (lower case): '$mode'" }
    }

    data class Http(
        val baseUrl: String = "http://localhost:8082",
        val client: String = HttpBankGateway.WEBCLIENT,
        val maxConnections: Int = 500,
        val connectTimeout: Duration = Duration.ofSeconds(1),
        val pendingAcquireTimeout: Duration = Duration.ofSeconds(1),
        val responseTimeout: Duration = Duration.ofSeconds(5),
    ) {
        init {
            require(client in CLIENTS) { "bank.gateway.http.client must be one of $CLIENTS (lower case): '$client'" }
        }

        fun toSettings() = HttpGatewaySettings(baseUrl, maxConnections, connectTimeout, pendingAcquireTimeout, responseTimeout)
    }

    companion object {
        const val DIRECT = "direct"
        const val HTTP = "http"
        val MODES = listOf(DIRECT, HTTP)
        val CLIENTS = listOf(HttpBankGateway.WEBCLIENT)
    }
}

/**
 * 은행 점검 시간 (P5-1). KST 벽시계 "HH:mm" — 시작 포함 · 끝 제외, 시작 > 끝이면 자정을 넘는다.
 * 문자열로 받아 LocalTime.parse 로 읽는다. application.yml 에서는 따옴표가 필요하다: YAML 1.1 은 23:50 을 60진수 정수 1430 으로 읽는다.
 * 점검 길이는 재발행 포기(E001)까지 걸리는 약 26분보다 짧게 둔다 — 길면 점검 때문에 포기하는 출금이 생긴다.
 * 시간대는 서버 기본 시간대(UTC)가 아니라 늘 Asia/Seoul 이다.
 */
@ConfigurationProperties("bank.maintenance")
data class BankMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

@ConfigurationProperties("account.verification")
data class VerificationProperties(
    val ttl: Duration = Duration.ofMinutes(5),
    val maxAttempts: Int = 5,
    /**
     * 1원인증 요청 판 (P6-2, ADR-0013): async(기본, 서블릿 비동기) | blocking(대조군 — 요청 스레드가 은행을 끝까지 기다린다, 측정 전용).
     * 소문자 그대로만 받는다: @ConditionalOnProperty 는 대소문자를 무시해 "Blocking" 도 블로킹 컨트롤러를 띄우지만, info · 측정 이름과 어긋난다.
     */
    val webMode: String = ASYNC,
) {
    init {
        require(webMode in WEB_MODES) { "account.verification.web-mode must be one of $WEB_MODES (lower case): '$webMode'" }
    }

    companion object {
        const val ASYNC = "async"
        const val BLOCKING = "blocking"
        val WEB_MODES = listOf(ASYNC, BLOCKING)
    }
}

/**
 * 조립(wiring)은 여기서만 한다. account 모듈은 BankPort 인터페이스만 알고,
 * BankPort 를 은행 게이트웨이(BankGateway — direct: Netty 직결, http: 게이트웨이 앱)에 붙이는 건 이 어댑터다 (포트-어댑터 구조).
 *
 * 은행 응답 future 는 Netty(direct) · Reactor(http) 이벤트 루프가 완료한다. 그 뒤의 처리(DB 저장 등 블로킹)가 이벤트 루프에서 돌지 않도록
 * thenApplyAsync 로 앱 실행기에 넘긴다. 이벤트 루프가 막히면 같은 루프의 모든 은행 통신이 멈춘다.
 */
class BankGatewayAdapter(
    private val client: BankGateway,
    private val executor: Executor,
    /** 1원인증 이체의 이용기관 거래번호 (V 접두). 결과조회 대상이 아니라 저장하지 않는다. */
    private val references: ReferenceGenerator = RandomReferenceGenerator(prefix = 'V'),
) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res -> holderNameOf(res) }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo, references.next()).thenApplyAsync({ res -> requireOk(res) }, executor)
}

@Configuration(proxyBeanMethods = false)
class AppConfiguration {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 앱 전체가 보는 시계 하나 (P5-1). 점검 시간 판단 · 상태 시각(sending_at · updated_at) · 대사가 같은 "지금" 을 보게 한다 —
     * 따로 Clock.systemUTC() 를 쓰면 테스트가 시각을 옮겨도 일부만 따라온다. 테스트는 @Primary TestClock 으로 바꾼다.
     */
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun bankMaintenanceWindow(props: BankMaintenanceProperties): BankMaintenanceWindow = props.toWindow()

    @Bean
    fun bankMaintenanceGuard(window: BankMaintenanceWindow, clock: Clock) = BankMaintenanceGuard(window, clock)

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.DIRECT, matchIfMissing = true)
    fun bankGatewayClient(props: BankGatewayProperties, maintenance: BankMaintenanceWindow, clock: Clock): BankGatewayClient =
        BankGatewayClient(
            BankGatewayConfig(
                host = props.host,
                port = props.port,
                responseTimeout = props.responseTimeout,
                pollInterval = props.pollInterval,
            ),
            // 점검 중에는 이체 · 결과조회 · 1원인증 전문을 보내지 않는다 (P5-1). "연결 없음" 과 같은 길이라
            // 이체는 RECEIVED 로 돌아가 재시도 · 재발행, 결과조회는 횟수를 세지 않는다. 웹에서 먼저 막혀도 여기가 마지막 보장이다
            sendGate = { if (maintenance.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()

    /**
     * mode=http (P8-1): 은행 연결은 게이트웨이 앱이 가진다. 이 프로세스는 HTTP 로만 부른다.
     * 응답 시간 초과가 게이트웨이의 은행 응답 시간 초과(3s) + 여유보다 짧으면 "모름" 이 쓸데없이 는다 — 경고만 (측정 · 테스트가 일부러 줄일 수 있게).
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.HTTP)
    fun httpBankGateway(props: BankGatewayProperties): HttpBankGateway {
        if (props.http.responseTimeout < Duration.ofSeconds(5)) {
            log.warn("bank.gateway.http.response-timeout={} is shorter than 5s (gateway bank timeout 3s + margin) — more results will be unknown", props.http.responseTimeout)
        }
        log.info("bank gateway mode=http client={} baseUrl={}", props.http.client, props.http.baseUrl)
        return HttpBankGateway.create(props.http.client, props.http.toSettings())
    }

    @Bean
    fun bankPort(client: BankGateway, @Qualifier("applicationTaskExecutor") executor: Executor, clock: Clock): BankPort =
        BankGatewayAdapter(client, executor, RandomReferenceGenerator(prefix = 'V', clock = clock))

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate, clock: Clock): VerificationRepository =
        JpaVerificationRepository(jpa, tx, clock)

    /** 잔고 SQL 한 곳 (P4-1). 실패 확정 트랜잭션 안에서 JpaTransferStore 가 환불을 부르고, 충전 · 조회는 WalletController 가 부른다. */
    @Bean
    fun walletLedger(jdbc: NamedParameterJdbcTemplate, tx: TransactionTemplate, clock: Clock) = WalletLedger(jdbc, tx, clock)

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
    fun transferService(store: TransferStore, verifications: VerificationRepository, clock: Clock) = TransferService(
        store = store,
        verifiedAccounts = VerifiedAccounts { userId, bankCode, accountNo -> verifications.existsVerified(userId, bankCode, accountNo) },
        referenceGenerator = RandomReferenceGenerator(prefix = 'T', clock = clock),
        clock = clock,
    )

    @Bean
    fun accountVerificationService(
        bankPort: BankPort,
        repository: VerificationRepository,
        props: VerificationProperties,
        clock: Clock,
    ) = AccountVerificationService(
        bank = bankPort,
        repository = repository,
        codeGenerator = RandomCodeGenerator(),
        clock = clock,
        ttl = props.ttl,
        maxAttempts = props.maxAttempts,
    )
}
