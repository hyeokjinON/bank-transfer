package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.config.LeaderElectionProperties
import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.BlackholeProxy
import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * Lease 리더 선출 (P8-3b 결정 3 · 6 · 11): 둘 중 하나만 은행에 붙는다, 정상 교대는 반납으로 바로 넘어간다,
 * API 서버와 끊기면 갱신 기한 안에 스스로 끊는다 (실험 H5 의 단위 대체 — kind CNI 가 egress 를 집행하지 않을 때의 근거).
 * 시간은 줄여서: Lease 3s · 갱신 기한 2s · 재시도 500ms.
 */
class GatewayLeaderElectionTest {
    private val k8s = MockK8s()
    private val bank = MockBankServer(singleSession = true).start()
    private val ns = "test"
    private val started = mutableListOf<GatewayLeaderElection>()

    @AfterTest
    fun tearDown() {
        started.forEach { runCatching { it.stop() } }
        bank.close()
        k8s.close()
    }

    private fun props(id: String) = LeaderElectionProperties(
        enabled = true, identity = id, namespace = ns,
        leaseDuration = Duration.ofSeconds(3), renewDeadline = Duration.ofSeconds(2), retryPeriod = Duration.ofMillis(500),
        drainTimeout = Duration.ofSeconds(1),
    )

    private fun gateway(id: String): Pair<GatewayLeaderElection, BankConnectionOwner> {
        k8s.createPod(ns, id)
        val tranNo = InMemoryTranNoSource()
        val owner = BankConnectionOwner(tranNo) {
            BankGatewayClient(
                BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
                tranNoSource = tranNo,
            )
        }
        val election = GatewayLeaderElection(k8s.client, props(id), owner, LeaderLabel(k8s.client, ns, id))
        election.start()
        started += election
        return election to owner
    }

    private fun holder() = k8s.client.leases().inNamespace(ns).withName("bank-gateway-leader").get()?.spec?.holderIdentity
    private fun role(pod: String) = k8s.client.pods().inNamespace(ns).withName(pod).get().metadata.labels[LeaderLabel.KEY]

    @Test
    fun `둘 중 하나만 리더 - 그 Pod 에만 라벨, 은행 세션은 하나`() {
        val (_, a) = gateway("gw-a")
        val (_, b) = gateway("gw-b")

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            assertEquals(1, listOf(a, b).count { it.isLeader })
            val leader = if (a.isLeader) "gw-a" else "gw-b"
            assertEquals(leader, holder())
            assertEquals(LeaderLabel.VALUE, role(leader))
            assertEquals(null, role(if (leader == "gw-a") "gw-b" else "gw-a"))
            assertTrue((if (a.isLeader) a else b).current()!!.isConnected)
        }
        assertEquals(1, bank.peakSessions())
    }

    @Test
    fun `리더가 정상 종료하면 반납 - 대기자가 만료를 기다리지 않고 이어받는다`() {
        val (electionA, a) = gateway("gw-a")
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(a.isLeader) }
        val (_, b) = gateway("gw-b")

        electionA.stop()   // 거절 → 비우기 → 끊기 → 라벨 떼기 → 반납
        started -= electionA

        assertFalse(a.isLeader)
        assertEquals(null, role("gw-a"))
        assertNotEquals("gw-a", holder())
        // 반납했으니 Lease 만료(3s)가 아니라 재시도 주기(500ms) 수준에 넘어간다
        await.atMost(Duration.ofMillis(2_500)) untilAsserted {
            assertTrue(b.isLeader)
            assertEquals("gw-b", holder())
            assertEquals(LeaderLabel.VALUE, role("gw-b"))
            assertTrue(b.current()!!.isConnected)
        }
        assertEquals(1, bank.peakSessions())
    }

    @Test
    fun `API 서버와 끊기면 리더는 갱신 기한 안에 은행 연결을 끊는다 (자기 차단)`() {
        val (_, a) = gateway("gw-a")
        await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(a.isLeader && a.current()!!.isConnected) }

        val cutAt = System.nanoTime()
        k8s.stopApiServer()

        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertFalse(a.isLeader) }
        val fencedMs = (System.nanoTime() - cutAt) / 1_000_000
        assertTrue(fencedMs <= 2_000 + 1_500, "갱신 기한(2s) + 재시도 여유 안에 끊어야 한다: ${fencedMs}ms")
    }

    @Test
    fun `API 서버가 응답하지 않아도(패킷 드롭) 갱신 기한 안에 은행 연결을 끊는다`() {
        BlackholeProxy(k8s.url).use { proxy ->
            val proxied = k8s.clientFor(proxy.url)
            var election: GatewayLeaderElection? = null
            try {
                k8s.createPod(ns, "gw-a")
                val tranNo = InMemoryTranNoSource()
                val owner = BankConnectionOwner(tranNo) {
                    BankGatewayClient(
                        BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
                        tranNoSource = tranNo,
                    )
                }
                election = GatewayLeaderElection(proxied, props("gw-a"), owner, LeaderLabel(proxied, ns, "gw-a"))
                election.start()
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(owner.isLeader && owner.current()!!.isConnected) }
                val session = owner.current()!!

                val cutAt = System.nanoTime()
                proxy.blackhole()

                await.atMost(Duration.ofSeconds(8)) untilAsserted { assertFalse(owner.isLeader) }
                val fencedMs = (System.nanoTime() - cutAt) / 1_000_000
                assertTrue(fencedMs <= 2_000 + 1_000, "갱신 기한(2s) + 1s 안에 끊어야 한다 (응답 없는 API 서버): ${fencedMs}ms")
                assertFalse(session.isConnected)
            } finally {
                runCatching { election?.stop() }   // 클라이언트를 닫기 전에 선출을 멈춘다
                proxied.close()
            }
        }
    }

    @Test
    fun `자기 차단 뒤 API 서버가 돌아오면 다시 리더가 정해져 은행에 붙는다 - 리더 없는 상태로 굳지 않는다`() {
        BlackholeProxy(k8s.url).use { proxy ->
            val proxied = k8s.clientFor(proxy.url)
            k8s.createPod(ns, "gw-a")
            val tranNo = InMemoryTranNoSource()
            val owner = BankConnectionOwner(tranNo) {
                BankGatewayClient(
                    BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
                    tranNoSource = tranNo,
                )
            }
            val election = GatewayLeaderElection(proxied, props("gw-a"), owner, LeaderLabel(proxied, ns, "gw-a"))
            try {
                election.start()
                await.atMost(Duration.ofSeconds(10)) untilAsserted { assertTrue(owner.isLeader && owner.current()!!.isConnected) }

                proxy.blackhole()
                await.atMost(Duration.ofSeconds(8)) untilAsserted { assertFalse(owner.isLeader) }
                proxy.heal()   // 매달렸던 갱신이 뒤늦게 성공해도, 차단한 선출기는 끝나고 새로 후보가 된다

                await.atMost(Duration.ofSeconds(3 + 3)) untilAsserted { assertTrue(owner.isLeader && owner.current()!!.isConnected) }
                assertEquals(1, bank.peakSessions())
            } finally {
                runCatching { election.stop() }
                proxied.close()
            }
        }
    }
}
