package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.tomcat.autoconfigure.TomcatVirtualThreadsWebServerFactoryCustomizer
import org.springframework.context.ApplicationContext
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * P7 (설계 §5 · §7): 가상 스레드는 기본으로 꺼져 있다 — 켤지는 ADR-0014. 측정 사전 확인이 읽는 `/actuator/info` 의 `threads.virtual` 도 고정한다.
 * 공유 컨텍스트(ApiIntegrationTest 기본 설정)를 그대로 쓴다.
 */
class VirtualThreadDefaultTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext
    @Autowired @Qualifier("applicationTaskExecutor") private lateinit var executor: Executor

    @Test
    fun `기본은 가상 스레드를 쓰지 않는다 - 앱 실행기는 플랫폼 스레드, Tomcat 가상 스레드 설정 없음`() {
        assertFalse(CompletableFuture.supplyAsync({ Thread.currentThread().isVirtual }, executor).get(5, TimeUnit.SECONDS))
        assertTrue(context.getBeansOfType(TomcatVirtualThreadsWebServerFactoryCustomizer::class.java).isEmpty())
    }

    @Test
    fun `actuator info 에 threads virtual false 가 보인다 - 측정 사전 확인이 읽는다`() {
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue(Regex("\"threads\":\\{\"virtual\":\"?false\"?[,}]").containsMatchIn(info), info)
    }
}
