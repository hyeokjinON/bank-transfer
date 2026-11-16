package io.github.hyeokjinon.banktransfer.gatewayapp.k8s

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import io.fabric8.mockwebserver.Context
import io.fabric8.mockwebserver.MockWebServer

/**
 * fabric8 가짜 API 서버 (CRUD 모드, http). 테스트가 직접 켜고 끈다 — [stopApiServer] 가 "API 서버 단절" 이다 (실험 H5 의 단위 대체).
 * CRUD 모드는 PUT 의 resourceVersion 을 검사해 낡았으면 409 를 준다 (진짜 API 서버와 같다 — Fabric8ContractTest 가 고정).
 * 클라이언트는 앱과 같은 "빨리 실패" 설정 (재시도 없음 · 연결 1초) — fabric8 기본 재시도(지수 백오프)면 단절 테스트가 수십 초 걸린다.
 */
class MockK8s : AutoCloseable {
    private val server = KubernetesMockServer(Context(), MockWebServer(), HashMap(), KubernetesCrudDispatcher(), false)

    init {
        server.init()
    }

    private val serverClient: KubernetesClient = server.createClient()
    val url: String get() = serverClient.configuration.masterUrl
    val client: KubernetesClient = KubernetesClientBuilder().withConfig(
        ConfigBuilder(Config.empty()).withMasterUrl(url).withRequestRetryBackoffLimit(0).withConnectionTimeout(1_000).withRequestTimeout(3_000).build(),
    ).build()

    fun createPod(namespace: String, name: String) {
        client.pods().inNamespace(namespace).resource(
            PodBuilder().withNewMetadata().withName(name).addToLabels("app", "bank-gateway").endMetadata().build(),
        ).create()
    }

    fun stopApiServer() = server.destroy()

    override fun close() {
        client.close()
        serverClient.close()
        runCatching { server.destroy() }   // stopApiServer 로 이미 내렸을 수 있다
    }
}
