package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.io.ClassPathResource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 인스턴스 구분 (P8-2, 스펙 §5-1 · §7-4): Compose 가 INFO_INSTANCE=api-1 | api-2 를 넣으면 /actuator/info 의 instance 로 보인다.
 * nginx 뒤에서 어느 api 가 답했는지(분배 확인) · ShedLock 의 locked_by 가 이 값이다. 환경변수가 없으면 local.
 */
class InstanceInfoTest : ApiIntegrationTest() {

    @Test
    fun `actuator info 에 instance 가 보인다 - 환경변수가 없으면 local`() {
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue(info.contains("\"instance\":\"local\""), info)
    }

    @Test
    fun `INFO_INSTANCE 환경변수가 info instance 가 된다`() {
        val env = StandardEnvironment()
        YamlPropertySourceLoader().load("application", ClassPathResource("application.yml")).forEach { env.propertySources.addLast(it) }
        env.propertySources.addFirst(MapPropertySource("container-env", mapOf<String, Any>("INFO_INSTANCE" to "api-2")))

        assertEquals("api-2", env.getProperty("info.instance"))
    }
}
