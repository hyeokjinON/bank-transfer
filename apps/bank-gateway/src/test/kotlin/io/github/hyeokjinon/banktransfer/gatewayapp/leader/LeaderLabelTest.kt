package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gatewayapp.k8s.MockK8s
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** 리더 라벨 (P8-3b 결정 5): Service selector 가 이 라벨로 리더 Pod 만 고른다. */
class LeaderLabelTest {
    private val k8s = MockK8s()

    @AfterTest
    fun tearDown() = k8s.close()

    private fun role() = k8s.client.pods().inNamespace("test").withName("gw-a").get().metadata.labels[LeaderLabel.KEY]

    @Test
    fun `리더면 붙이고 아니면 뗀다 - 다른 라벨은 그대로`() {
        k8s.createPod("test", "gw-a")
        val label = LeaderLabel(k8s.client, "test", "gw-a")

        label.ensure(true)
        assertEquals(LeaderLabel.VALUE, role())
        label.ensure(false)
        assertNull(role())
        assertEquals("bank-gateway", k8s.client.pods().inNamespace("test").withName("gw-a").get().metadata.labels["app"])
    }

    @Test
    fun `처음엔 상태를 모르니 대기자도 한 번 뗀다 - 컨테이너 재시작으로 남은 라벨 정리`() {
        k8s.createPod("test", "gw-a")
        LeaderLabel(k8s.client, "test", "gw-a").ensure(true)   // 앞 프로세스가 붙이고 죽었다

        LeaderLabel(k8s.client, "test", "gw-a").ensure(false)  // 같은 Pod 에서 다시 뜬 프로세스

        assertNull(role())
    }

    @Test
    fun `API 서버가 없으면 던지지 않는다 - 다음 주기에 다시`() {
        k8s.createPod("test", "gw-a")
        val label = LeaderLabel(k8s.client, "test", "gw-a")
        k8s.stopApiServer()

        label.ensure(true)   // 예외 없이 끝나야 한다
    }
}
