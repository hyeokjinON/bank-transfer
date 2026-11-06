package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 실제 application.yml 로 뜬 앱의 컨슈머 모드 (P6-1). 측정 스크립트(load/consumer/run.sh)는 /actuator/info 로 띄운 조건을 확인한 뒤에만 잰다. */
class ConsumerModeWiringTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext

    @Test
    fun `기본 설정이면 블로킹 리스너 하나만 뜨고 actuator info 에 모드 · 상한 · 커넥션 풀 크기가 보인다`() {
        assertEquals(1, context.getBeanNamesForType(TransferRequestedListener::class.java).size)
        assertEquals(0, context.getBeanNamesForType(SuspendingTransferRequestedListener::class.java).size)

        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!

        assertTrue("\"mode\":\"blocking\"" in info, info)
        assertTrue(Regex("\"max-in-flight-bank-calls\":\"?64\"?[,}]").containsMatchIn(info), info)
        assertTrue(Regex("\"maximum-pool-size\":\"?20\"?[,}]").containsMatchIn(info), info)
    }
}
