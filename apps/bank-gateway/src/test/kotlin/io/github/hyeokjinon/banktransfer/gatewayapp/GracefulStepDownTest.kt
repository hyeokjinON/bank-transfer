package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LEADERSHIP_PHASE
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 종료 훅 (P8-3b 결정 11 · 12): SIGTERM(컨텍스트 종료) → 새 요청 거절 → 처리 중 전문 비우기 → 끊기. 웹 서버보다 먼저. */
class GracefulStepDownTest {

    @Test
    fun `종료 때 처리 중 전문은 응답을 받은 뒤 끊고, 새 요청은 안 보냄`() {
        MockBankServer(responseDelay = Duration.ofMillis(800)).start().use { bank ->
            val ctx = GatewayApps.start(WebApplicationType.REACTIVE, bank.boundPort, "--bank.gateway.response-timeout=3s")
            await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(ctx.bankClient().isConnected) }
            val owner = ctx.getBean(BankConnectionOwner::class.java)
            val inflight = owner.transfer("090", "1002123456789", 1_000, "m", "T261116GRACEFUL0001")

            val closing = Thread { ctx.close() }.apply { start() }
            await.atMost(Duration.ofSeconds(2)) untilAsserted {
                val error = runCatching { owner.inquireName("090", "1002123456789").get(1, TimeUnit.SECONDS) }.exceptionOrNull()
                assertIs<BankNotSentException>((error as? ExecutionException)?.cause)
            }
            closing.join(10_000)

            assertEquals(ResponseCodes.OK, inflight.get(1, TimeUnit.SECONDS).header.responseCode)
            assertEquals(1, bank.depositsOf("1002123456789").size)
        }
    }

    @Test
    fun `종료 훅은 웹 서버 graceful 종료보다 먼저 멈춘다 (phase 가 더 높다)`() {
        // Boot 4.1.1 spring-boot-web-server.jar: WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE = DEFAULT_PHASE - 1024 (javap 로 확인)
        assertTrue(LEADERSHIP_PHASE > org.springframework.boot.web.server.context.WebServerGracefulShutdownLifecycle.SMART_LIFECYCLE_PHASE)
    }
}
