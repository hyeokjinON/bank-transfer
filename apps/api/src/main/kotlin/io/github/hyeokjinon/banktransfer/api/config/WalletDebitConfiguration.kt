package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.persistence.ConditionalWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.OptimisticWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.PessimisticWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.RedisWalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletDebit
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import io.github.hyeokjinon.banktransfer.api.persistence.redissonConfig
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Duration

/** 잔고 차감 전략 (P4-2, ADR-0009). 설정값은 소문자(conditional …)로 쓴다 — enum 바인딩은 대소문자를 가리지 않고, 모르는 이름이면 기동 실패. */
enum class DebitStrategy { CONDITIONAL, PESSIMISTIC, OPTIMISTIC, REDIS }

@ConfigurationProperties("wallet")
data class WalletProperties(
    val debitStrategy: DebitStrategy = DebitStrategy.CONDITIONAL,
    val optimistic: Optimistic = Optimistic(),
    val lock: Lock = Lock(),
) {
    data class Optimistic(
        val maxAttempts: Int = 5,
        val backoffMin: Duration = Duration.ofMillis(10),
        val backoffMax: Duration = Duration.ofMillis(50),
    )

    data class Lock(
        val waitTime: Duration = Duration.ofSeconds(3),
        val redisAddress: String = "redis://localhost:6379",
    )
}

/**
 * 차감 전략 조립 (P4-2). 기동 때 [WalletProperties.debitStrategy] 하나만 만든다 — 운영처럼 한 번에 한 전략.
 * RedissonClient 는 redis 전략일 때만 만든다: 다른 전략으로 띄우면 Redis 가 없어도 기동된다.
 * (WalletProperties 는 @ConfigurationPropertiesScan 도 등록하지만, 이 클래스만 떼어 쓰는 테스트를 위해 여기서도 켠다 — 같은 이름이라 한 번만 등록된다.)
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WalletProperties::class)
class WalletDebitConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "wallet", name = ["debit-strategy"], havingValue = "redis")
    fun redissonClient(props: WalletProperties): RedissonClient = Redisson.create(redissonConfig(props.lock.redisAddress))

    @Bean
    fun walletDebit(
        props: WalletProperties,
        jdbc: NamedParameterJdbcTemplate,
        ledger: WalletLedger,
        redisson: ObjectProvider<RedissonClient>,
    ): WalletDebit {
        val conditional = ConditionalWalletDebit(jdbc, ledger)
        return when (props.debitStrategy) {
            DebitStrategy.CONDITIONAL -> conditional
            DebitStrategy.PESSIMISTIC -> PessimisticWalletDebit(jdbc, ledger)
            DebitStrategy.OPTIMISTIC -> OptimisticWalletDebit(
                jdbc, ledger, props.optimistic.maxAttempts, props.optimistic.backoffMin, props.optimistic.backoffMax,
            )
            // 안쪽은 기준선과 같은 조건부 UPDATE — Redis 락은 줄 세우기일 뿐, 정확성은 DB 가 지킨다
            DebitStrategy.REDIS -> RedisWalletDebit(redisson.getObject(), conditional, props.lock.waitTime)
        }
    }
}
