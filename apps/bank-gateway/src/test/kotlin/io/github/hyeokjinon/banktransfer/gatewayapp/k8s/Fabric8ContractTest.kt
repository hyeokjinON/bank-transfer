package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * P8-3b 가 기대는 fabric8 동작을 고정한다 (버전을 올리면 여기서 먼저 깨진다).
 * ConfigMap CAS(구간 예약) · create 충돌 · Lease 선출 · Pod 라벨 merge patch.
 */
class Fabric8ContractTest {
    private val k8s = MockK8s()
    private val ns = "test"

    @AfterTest
    fun tearDown() = k8s.close()

    private fun configMap(reservedTo: String) =
        ConfigMapBuilder().withNewMetadata().withName("bank-gateway-tran-no").endMetadata()
            .addToData("date", "20261116").addToData("reservedTo", reservedTo).build()

    @Test
    fun `ConfigMap 을 낡은 resourceVersion 으로 update 하면 409`() {
        val maps = k8s.client.configMaps().inNamespace(ns)
        maps.resource(configMap("1000")).create()
        val first = maps.withName("bank-gateway-tran-no").get()
        val second = maps.withName("bank-gateway-tran-no").get()

        first.data = mapOf("date" to "20261116", "reservedTo" to "2000")
        maps.resource(first).update()

        second.data = mapOf("date" to "20261116", "reservedTo" to "2000")
        val error = assertFailsWith<KubernetesClientException> { maps.resource(second).update() }
        assertEquals(409, error.code)
        assertEquals("2000", maps.withName("bank-gateway-tran-no").get().data["reservedTo"])
    }

    @Test
    fun `이미 있는 ConfigMap 을 create 하면 409`() {
        val maps = k8s.client.configMaps().inNamespace(ns)
        maps.resource(configMap("1000")).create()
        val error = assertFailsWith<KubernetesClientException> { maps.resource(configMap("1")).create() }
        assertEquals(409, error.code)
    }

    @Test
    fun `Lease 선출 - 둘 중 하나가 리더, holder = 그 identity`() {
        val leader = AtomicReference<String>()
        val futures = listOf("gw-a", "gw-b").map { id ->
            val config = LeaderElectionConfigBuilder()
                .withName("contract")
                .withLock(LeaseLock(ns, "bank-gateway-leader", id))
                .withLeaseDuration(Duration.ofSeconds(3))
                .withRenewDeadline(Duration.ofSeconds(2))
                .withRetryPeriod(Duration.ofMillis(500))
                .withReleaseOnCancel(true)
                .withLeaderCallbacks(LeaderCallbacks({ leader.compareAndSet(null, id) }, {}, {}))
                .build()
            k8s.client.leaderElector().withConfig(config).build().start()
        }
        try {
            await.atMost(Duration.ofSeconds(10)) untilAsserted {
                assertNotNull(leader.get())   // 둘 다 null 이면 통과해 버리는 것을 막는다
                assertEquals(leader.get(), k8s.client.leases().inNamespace(ns).withName("bank-gateway-leader").get()?.spec?.holderIdentity)
            }
        } finally {
            futures.forEach { it.cancel(true) }
        }
    }

    @Test
    fun `Pod 라벨 JSON merge patch - 붙이고 null 로 뗀다`() {
        k8s.createPod(ns, "gw-a")
        val pod = k8s.client.pods().inNamespace(ns).withName("gw-a")
        pod.patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"bank-transfer/gateway-role":"leader"}}}""")
        assertEquals("leader", pod.get().metadata.labels["bank-transfer/gateway-role"])
        pod.patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"bank-transfer/gateway-role":null}}}""")
        assertNull(pod.get().metadata.labels["bank-transfer/gateway-role"])
        assertEquals("bank-gateway", pod.get().metadata.labels["app"])
    }
}
