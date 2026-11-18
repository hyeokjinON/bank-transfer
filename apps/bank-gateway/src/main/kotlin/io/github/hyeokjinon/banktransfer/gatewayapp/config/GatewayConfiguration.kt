package io.github.hyeokjinon.banktransfer.gatewayapp.config

import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.AlwaysLeader
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.BankConnectionOwner
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.ConfigMapTranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.GatewayLeaderElection
import io.github.hyeokjinon.banktransfer.gatewayapp.leader.LeaderLabel
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayGauges
import io.github.hyeokjinon.banktransfer.gatewayapp.metrics.GatewayMetrics
import io.github.hyeokjinon.banktransfer.gatewayapp.tracing.BankRequestTracing
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.Tracer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock
import java.time.Duration
import java.time.LocalTime
import java.time.ZoneId

@ConfigurationProperties("bank.gateway")
data class GatewayBankProperties(
    val host: String = "localhost",
    val port: Int = 9090,
    val responseTimeout: Duration = Duration.ofSeconds(3),
    val pollInterval: Duration = Duration.ofSeconds(30),
)

/** api 와 같은 규칙 (KST 벽시계 "HH:mm", 시작 포함 · 끝 제외, 따옴표 필수). */
@ConfigurationProperties("bank.maintenance")
data class GatewayMaintenanceProperties(
    val enabled: Boolean = true,
    val start: String = "23:50",
    val end: String = "00:10",
) {
    fun toWindow() = BankMaintenanceWindow(LocalTime.parse(start), LocalTime.parse(end), ZoneId.of("Asia/Seoul"), enabled)
}

/**
 * 게이트웨이 이중화 (P8-3b). 꺼져 있으면(기본) 기동 즉시 리더 — 지금까지와 같다.
 * 켜면 K8s Lease 로 리더 1대만 은행에 붙고, 전문번호는 ConfigMap 구간 예약 (스펙 §3 결정 3 · 6 · 8).
 */
@ConfigurationProperties("bank.gateway.leader-election")
data class LeaderElectionProperties(
    val enabled: Boolean = false,
    /** Pod 이름 (Lease holder · 라벨 대상). application.yml 이 POD_NAME 환경변수에서 읽는다. */
    val identity: String = "",
    /** POD_NAMESPACE. */
    val namespace: String = "",
    /** 비우면 Pod 안 자동 설정(ServiceAccount 토큰). 테스트가 가짜 API 서버 주소를 넣는다. */
    val apiServerUrl: String = "",
    val leaseName: String = "bank-gateway-leader",
    val tranNoConfigMap: String = "bank-gateway-tran-no",
    val leaseDuration: Duration = Duration.ofSeconds(15),
    val renewDeadline: Duration = Duration.ofSeconds(10),
    val retryPeriod: Duration = Duration.ofSeconds(2),
    /** 정상 교대 때 처리 중 전문을 기다리는 최대 시간 (응답 시간 초과 3초 + 여유). */
    val drainTimeout: Duration = Duration.ofSeconds(5),
    val tranNoBlockSize: Long = 1000,
)

/** 조립은 여기서만. 은행 연결은 이 앱에 하나뿐이고(스펙 P8-1 §2 결정 7), 그 주인은 [BankConnectionOwner] 다 (P8-3b). */
@Configuration(proxyBeanMethods = false)
class GatewayConfiguration {

    @Bean
    fun clock(): Clock = Clock.system(ZoneId.of("Asia/Seoul"))   // 저장소 시간대 정책은 KST (docs/database.md)

    /**
     * K8s API 클라이언트 — 선출이 켜졌을 때만 (Pod 안 ServiceAccount 토큰으로 자동 설정, 테스트는 가짜 API 서버 주소).
     * 빨리 실패: 재시도 없음 · 연결 2초 · 요청 5초. fabric8 기본 재시도(지수 백오프)는 API 서버 단절 때 갱신 실패를 늦게 알려
     * "갱신 기한(10s) 안에 자기 차단" 을 흐린다 — 재시도는 선출기의 재시도 주기(2s)가 맡는다.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "bank.gateway.leader-election", name = ["enabled"], havingValue = "true")
    fun kubernetesClient(election: LeaderElectionProperties): KubernetesClient {
        val base = if (election.apiServerUrl.isBlank()) {
            Config.autoConfigure(null)
        } else {
            ConfigBuilder(Config.empty()).withMasterUrl(election.apiServerUrl).withNamespace(election.namespace).build()
        }
        val config = ConfigBuilder(base).withRequestRetryBackoffLimit(0).withConnectionTimeout(2_000).withRequestTimeout(5_000).build()
        return KubernetesClientBuilder().withConfig(config).build()
    }

    /** 점검 창 하나 (송신 보류 · 점검 Gauge 가 같이 본다). */
    @Bean
    fun gatewayMaintenanceWindow(maintenance: GatewayMaintenanceProperties): BankMaintenanceWindow = maintenance.toWindow()

    /** P9a 사건 지표. MeterRegistry 는 actuator 자동 구성 (Prometheus 레지스트리는 Task 6). */
    @Bean
    fun gatewayMetrics(registry: MeterRegistry) = GatewayMetrics(registry)

    /** 전문번호 발급: 선출 켜짐 = ConfigMap 구간 예약(게이트웨이 여러 대 사이 유일), 꺼짐 = 메모리 카운터 (지금과 같다). */
    @Bean
    fun tranNoSource(election: LeaderElectionProperties, k8s: ObjectProvider<KubernetesClient>, clock: Clock, metrics: GatewayMetrics): TranNoSource =
        if (election.enabled) {
            ConfigMapTranNoSource(k8s.getObject(), election.namespace, election.tranNoConfigMap, clock, election.tranNoBlockSize, metrics = metrics)
        } else {
            InMemoryTranNoSource()
        }

    /**
     * 은행 연결의 주인. 컨트롤러는 이 빈을 BankGateway 로 받는다 (리더가 아니면 503 BANK_NOT_SENT).
     * 점검 중에는 전문을 보내지 않는다 (P5-1) — 503 BANK_NOT_SENT. 선출이 꺼져 있으면 여기서 바로 리더가 된다 (기동 중 readiness 가 standby 로 잠깐 UP 이 되지 않게).
     */
    @Bean(destroyMethod = "close")
    fun bankConnectionOwner(
        props: GatewayBankProperties,
        window: BankMaintenanceWindow,
        election: LeaderElectionProperties,
        clock: Clock,
        tranNoSource: TranNoSource,
        metrics: GatewayMetrics,
        tracer: ObjectProvider<Tracer>,
    ): BankConnectionOwner {
        val config = BankGatewayConfig(host = props.host, port = props.port, responseTimeout = props.responseTimeout, pollInterval = props.pollInterval)
        return BankConnectionOwner(tranNoSource, metrics, BankRequestTracing(tracer.getIfAvailable { Tracer.NOOP })) {
            BankGatewayClient(
                config,
                clock,
                sendGate = { if (window.isActive(clock.instant())) "bank maintenance window" else null },
                tranNoSource = tranNoSource,
            )
        }.apply { if (!election.enabled) startLeading() }
    }

    /** P9a 상태 Gauge 4개 (리더 · 연결 · 남은 번호 · 점검). */
    @Bean
    fun gatewayGauges(owner: BankConnectionOwner, tranNoSource: TranNoSource, window: BankMaintenanceWindow, clock: Clock) =
        GatewayGauges(owner, tranNoSource, window, clock)

    /** 종료 훅 + (켜짐이면) 선출. 둘 다 웹 서버 graceful 종료보다 먼저 멈춘다. */
    @Bean
    fun gatewayLeadership(owner: BankConnectionOwner, election: LeaderElectionProperties, k8s: ObjectProvider<KubernetesClient>): SmartLifecycle =
        if (election.enabled) {
            val client = k8s.getObject()
            GatewayLeaderElection(client, election, owner, LeaderLabel(client, election.namespace, election.identity))
        } else {
            AlwaysLeader(owner, election.drainTimeout)
        }

    @Bean
    fun bankConnectionHealthIndicator(owner: BankConnectionOwner) = BankConnectionHealthIndicator(owner)
}
