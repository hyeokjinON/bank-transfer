package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 기본 설정의 1원인증 요청 판 (P6-2). 측정 스크립트(load/verification/run.sh)는 /actuator/info 로 띄운 판을 확인한 뒤에만 잰다. */
class VerificationWebModeWiringTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext

    @Test
    fun `기본 설정이면 비동기 컨트롤러 하나만 뜨고 actuator info 에 판 · Tomcat 최대 스레드가 보인다`() {
        assertEquals(1, context.getBeanNamesForType(AsyncVerificationRequestController::class.java).size)
        assertEquals(0, context.getBeanNamesForType(BlockingVerificationRequestController::class.java).size)
        assertEquals(1, context.getBeanNamesForType(AccountVerificationController::class.java).size)   // 확인은 판과 상관없이 늘 뜬다

        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!

        assertTrue("\"web-mode\":\"async\"" in info, info)
        assertTrue(Regex("\"max\":\"?200\"?").containsMatchIn(info), info)
    }
}
