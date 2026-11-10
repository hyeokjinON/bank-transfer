package io.github.hyeokjinon.banktransfer.reactive.config

import io.github.hyeokjinon.banktransfer.account.RandomCodeGenerator
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.account.infra.AccountVerificationJpaRepository
import io.github.hyeokjinon.banktransfer.account.infra.JpaVerificationRepository
import io.github.hyeokjinon.banktransfer.gateway.BankGateway
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceGuard
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.HttpGatewaySettings
import io.github.hyeokjinon.banktransfer.reactive.flow.CoroutineVerificationFlow
import io.github.hyeokjinon.banktransfer.reactive.flow.ReactorVerificationFlow
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.ReferenceGenerator
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration

/** 조립은 여기서만 (apps/api 의 AppConfiguration 과 같은 원칙). 은행 · 점검 · 저장소는 apps/api 와 같은 클래스를 같은 설정으로 쓴다. */
@Configuration(proxyBeanMethods = false)
class ReactiveConfiguration {

    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun bankMaintenanceWindow(props: BankMaintenanceProperties): BankMaintenanceWindow = props.toWindow()

    @Bean
    fun bankMaintenanceGuard(window: BankMaintenanceWindow, clock: Clock) = BankMaintenanceGuard(window, clock)

    /** 점검 중에는 전문을 보내지 않는다 (P5-1) — 웹 가드가 먼저 막아도 여기가 마지막 보장이다 (apps/api 와 같다). */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.DIRECT, matchIfMissing = true)
    fun bankGatewayClient(props: BankGatewayProperties, maintenance: BankMaintenanceWindow, clock: Clock): BankGatewayClient =
        BankGatewayClient(
            BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval),
            sendGate = { if (maintenance.isActive(clock.instant())) "bank maintenance window" else null },
        ).start()

    /**
     * mode=http (P8-1): 은행 연결은 게이트웨이 앱이 가진다. 이 프로세스는 HTTP 로만 부른다.
     * 응답 시간 초과가 게이트웨이의 은행 응답 시간 초과(3s) + 여유보다 짧으면 "모름" 이 쓸데없이 는다 — 경고만 (측정 · 테스트가 일부러 줄일 수 있게).
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway", name = ["mode"], havingValue = BankGatewayProperties.HTTP)
    fun httpBankGateway(props: BankGatewayProperties): HttpBankGateway {
        if (props.http.responseTimeout < HttpGatewaySettings.RECOMMENDED_MIN_RESPONSE_TIMEOUT) {
            log.warn("bank.gateway.http.response-timeout={} is shorter than {} (gateway bank timeout 3s + margin) — more results will be unknown", props.http.responseTimeout, HttpGatewaySettings.RECOMMENDED_MIN_RESPONSE_TIMEOUT)
        }
        log.info("bank gateway mode=http client={} baseUrl={}", props.http.client, props.http.baseUrl)
        return HttpBankGateway.create(props.http.client, props.http.toSettings())
    }

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate, clock: Clock): VerificationRepository =
        JpaVerificationRepository(jpa, tx, clock)

    @Bean
    fun verificationRequestRules(props: ReactiveVerificationProperties, clock: Clock) =
        VerificationRequestRules(codeGenerator = RandomCodeGenerator(), clock = clock, ttl = props.ttl)

    /** 1원인증 이체의 이용기관 거래번호 (V 접두, 저장 안 함) — apps/api 와 같은 생성기. */
    @Bean
    fun verificationReferences(clock: Clock): ReferenceGenerator = RandomReferenceGenerator(prefix = 'V', clock = clock)

    @Bean
    fun coroutineVerificationFlow(
        client: BankGateway,
        rules: VerificationRequestRules,
        repository: VerificationRepository,
        references: ReferenceGenerator,
    ) = CoroutineVerificationFlow(client, rules, repository, references)

    @Bean
    fun reactorVerificationFlow(
        client: BankGateway,
        rules: VerificationRequestRules,
        repository: VerificationRepository,
        references: ReferenceGenerator,
    ) = ReactorVerificationFlow(client, rules, repository, references)
}
