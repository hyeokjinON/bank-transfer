package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType
import org.slf4j.LoggerFactory

/**
 * 자기 Pod 의 리더 라벨 (P8-3b 결정 5 · 20). Service `bank-gateway` 의 selector 에 이 라벨이 있어 트래픽은 리더에게만 간다.
 * [ensure] 는 "원하는 상태" 로 맞춘다 — 실패하면 다음 주기(GatewayLeaderElection, retry 2s)에 다시. 처음엔 상태를 모르므로 무조건 한 번 맞춘다
 * (컨테이너만 재시작되면 Pod 라벨은 남는다).
 * API 서버와 끊긴 옛 리더는 라벨을 못 뗀다 → 잠깐 라벨 Pod 가 둘. 그쪽으로 간 요청은 연결이 없어 503 BANK_NOT_SENT 로 재시도된다 (스펙 §6-2).
 */
class LeaderLabel(private val k8s: KubernetesClient, private val namespace: String, private val podName: String) {
    private val log = LoggerFactory.getLogger(javaClass)
    private var labeled: Boolean? = null   // null = 모름

    @Synchronized
    fun ensure(leader: Boolean) {
        if (labeled == leader) return
        val value = if (leader) "\"$VALUE\"" else "null"   // JSON merge patch: null 은 키 삭제
        try {
            k8s.pods().inNamespace(namespace).withName(podName)
                .patch(PatchContext.of(PatchType.JSON_MERGE), """{"metadata":{"labels":{"$KEY":$value}}}""")
            labeled = leader
            log.info("leader label {} on {}", if (leader) "set" else "removed", podName)
        } catch (e: Exception) {
            log.warn("leader label update failed (retry next period): {}", e.message)
        }
    }

    companion object {
        const val KEY = "bank-transfer/gateway-role"
        const val VALUE = "leader"
    }
}
