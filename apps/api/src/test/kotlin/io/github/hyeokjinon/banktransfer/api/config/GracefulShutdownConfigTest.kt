package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.context.LifecycleProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.web.server.Shutdown
import org.springframework.boot.web.server.autoconfigure.ServerProperties
import org.springframework.core.io.ClassPathResource
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * P8-2 (스펙 §3-10 · §7-1): 배포 · 롤링 재시작 때 처리 중인 요청 · 메시지를 끝내고 내려간다.
 * 단계당 20초 — Compose stop_grace_period(30초)보다 짧아야 바깥이 먼저 SIGKILL 하지 않는다.
 * graceful 은 Boot 4 의 기본값이기도 하다 — application.yml 에 명시해 기본이 바뀌어도 그대로인지 함께 고정한다.
 */
class GracefulShutdownConfigTest : ApiIntegrationTest() {

    @Autowired private lateinit var server: ServerProperties
    @Autowired private lateinit var lifecycle: LifecycleProperties

    @Test
    fun `종료는 graceful 이고 단계마다 최대 20초`() {
        assertEquals(Shutdown.GRACEFUL, server.shutdown)
        assertEquals(Duration.ofSeconds(20), lifecycle.timeoutPerShutdownPhase)
    }

    @Test
    fun `application yml 이 graceful · 20s · probes 를 명시한다 - Boot 기본값에 기대지 않게`() {
        val yml = Binder(ConfigurationPropertySources.from(YamlPropertySourceLoader().load("application", ClassPathResource("application.yml"))))
        assertEquals("graceful", yml.bind("server.shutdown", String::class.java).get())
        assertEquals("20s", yml.bind("spring.lifecycle.timeout-per-shutdown-phase", String::class.java).get())
        assertEquals("true", yml.bind("management.endpoint.health.probes.enabled", String::class.java).get())
    }

    @Test
    fun `readiness 프로브가 UP - Compose healthcheck 가 이 경로를 본다`() {
        val body = client.get().uri("/actuator/health/readiness").retrieve().body(String::class.java)!!
        assertTrue(body.contains("\"status\":\"UP\""), body)
    }
}
