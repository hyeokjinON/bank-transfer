package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.api.persistence.redissonConfig
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/** infra/docker-compose.yml 과 같은 redis:7.4-alpine. Testcontainers 2.0 에는 Redis 전용 모듈이 없어 GenericContainer 로 띄운다. */
class RedisContainer : GenericContainer<RedisContainer>(DockerImageName.parse("redis:7.4-alpine")) {
    init {
        withExposedPorts(PORT)
    }

    /** Redisson 주소 (redis://호스트:매핑된포트). 컨테이너가 뜬 뒤에만 부른다. */
    val address: String get() = "redis://$host:${getMappedPort(PORT)}"

    private companion object {
        const val PORT = 6379
    }
}

/**
 * 통합 테스트용 Redis (P4-2). 앱의 기본 전략은 conditional 이라 앱 자체는 Redis 를 쓰지 않는다 —
 * Redis 전략 테스트가 이 클라이언트로 RedisWalletDebit 을 직접 만든다 (전략마다 컨텍스트를 띄우지 않게).
 * Spring Data Redis 를 쓰지 않아 @ServiceConnection 대신 클라이언트를 직접 만든다. 설정은 앱과 같은 redissonConfig().
 */
@TestConfiguration(proxyBeanMethods = false)
class RedisTestContainer {

    @Bean
    fun redis(): RedisContainer = RedisContainer()

    @Bean(destroyMethod = "shutdown")
    fun testRedisson(redis: RedisContainer): RedissonClient = Redisson.create(redissonConfig(redis.address))
}
