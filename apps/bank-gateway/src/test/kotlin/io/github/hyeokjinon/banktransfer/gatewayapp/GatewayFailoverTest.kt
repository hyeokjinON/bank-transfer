package io.github.hyeokjinon.banktransfer.gatewayapp

import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LeaderLabel
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.WebApplicationType
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 게이트웨이 앱 두 대 + 가짜 API 서버 + 세션 하나 · 번호 유일 모의 은행 (P8-3b 성공 기준 1 · 2 · 3 의 앱 단위 확인).
 * 리더만 은행에 붙고, 둘 다 readiness 200, 리더가 정상 종료하면 대기자가 이어받아 번호가 겹치지 않게 보낸다.
 */
class GatewayFailoverTest {
    private val ns = "test"

    private fun start(k8s: MockK8s, bankPort: Int, id: String): ConfigurableApplicationContext {
        k8s.createPod(ns, id)
        return GatewayApps.start(
            WebApplicationType.REACTIVE, bankPort,
            "--bank.gateway.response-timeout=3s",
            "--bank.gateway.leader-election.enabled=true",
            "--bank.gateway.leader-election.api-server-url=${k8s.url}",
            "--bank.gateway.leader-election.namespace=$ns",
            "--bank.gateway.leader-election.identity=$id",
            "--bank.gateway.leader-election.lease-duration=3s",
            "--bank.gateway.leader-election.renew-deadline=2s",
            "--bank.gateway.leader-election.retry-period=500ms",
            "--bank.gateway.leader-election.drain-timeout=1s",
            "--bank.gateway.leader-election.tran-no-block-size=10",
        )
    }

    private fun ConfigurableApplicationContext.owner() = getBean(BankConnectionOwner::class.java)
    private fun ConfigurableApplicationContext.readiness(): Int =
        RestClient.create("http://127.0.0.1:${port()}").get().uri("/actuator/health/readiness").exchange { _, res -> res.statusCode.value() }

    @Test
    fun `리더만 은행에 붙고 둘 다 Ready, 리더가 내려가면 대기자가 이어받아 번호가 겹치지 않는다`() {
        MockK8s().use { k8s ->
            MockBankServer(singleSession = true, uniqueTranNo = true).start().use { bank ->
                val a = start(k8s, bank.boundPort, "gw-a")
                val b = start(k8s, bank.boundPort, "gw-b")
                try {
                    await.atMost(Duration.ofSeconds(15)) untilAsserted {
                        assertEquals(1, listOf(a, b).count { it.owner().isLeader })
                        assertTrue(listOf(a, b).first { it.owner().isLeader }.bankClient().isConnected)
                        assertEquals(200, a.readiness())
                        assertEquals(200, b.readiness())
                    }
                    val (leader, standby) = if (a.owner().isLeader) a to b else b to a
                    repeat(12) { i ->   // 구간(10개)을 넘겨 미리 예약까지 거친다
                        assertEquals(ResponseCodes.OK, leader.owner().transfer("090", "1002123456789", 1_000, "m", "T261116FAILOVER%04d".format(i)).get(3, TimeUnit.SECONDS).header.responseCode)
                    }

                    leader.close()   // 정상 교대

                    await.atMost(Duration.ofSeconds(5)) untilAsserted {
                        assertTrue(standby.owner().isLeader)
                        assertTrue(standby.bankClient().isConnected)
                    }
                    repeat(12) { i ->
                        assertEquals(ResponseCodes.OK, standby.owner().transfer("090", "1002123456789", 1_000, "m", "T261116TAKEOVER%04d".format(i)).get(3, TimeUnit.SECONDS).header.responseCode)
                    }
                    assertEquals(24, bank.depositsOf("1002123456789").size)   // 3001(번호 중복) 없이 전부 처리
                    assertEquals(1, bank.peakSessions())
                    val standbyPod = if (standby === a) "gw-a" else "gw-b"
                    assertEquals(LeaderLabel.VALUE, k8s.client.pods().inNamespace(ns).withName(standbyPod).get().metadata.labels[LeaderLabel.KEY])
                } finally {
                    listOf(a, b).forEach { runCatching { it.close() } }
                }
            }
        }
    }
}
