package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock
import io.github.hyeokjinon.banktransfer.gatewayapp.config.LeaderElectionProperties
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * K8s Lease 리더 선출 (P8-3b 결정 3 · 6 · 11 · 12). 리더만 은행에 붙는다.
 * - 콜백 · 재후보 · 라벨 맞추기는 한 스레드(gateway-leader)에서 차례로 — 시작 · 끝이 뒤섞이지 않는다.
 * - 갱신 기한(10s) 안에 갱신하지 못하면 fabric8 이 임기를 끝낸다 → 즉시 연결을 끊는다 (자기 차단). 새 리더는 만료(15s) 뒤 — 겹침 없음 (시계 차 < 5s 전제).
 * - 선출기는 한 번 쓰고 버린다: 임기를 잃으면 새 선출기로 다시 후보가 된다.
 * - 종료(SIGTERM) = 정상 교대: 거절 → 비우기 → 끊기 → 라벨 떼기 → Lease 반납. 대기자는 만료를 기다리지 않고 재시도 주기(2s) 안에 이어받는다.
 */
class GatewayLeaderElection(
    private val k8s: KubernetesClient,
    private val props: LeaderElectionProperties,
    private val owner: BankConnectionOwner,
    private val label: LeaderLabel,
) : SmartLifecycle {

    private val log = LoggerFactory.getLogger(javaClass)
    private val worker = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "gateway-leader").apply { isDaemon = true } }

    @Volatile private var running = false
    @Volatile private var elector: CompletableFuture<*>? = null

    override fun start() {
        require(props.identity.isNotBlank()) { "bank.gateway.leader-election.identity (POD_NAME) is empty" }
        require(props.namespace.isNotBlank()) { "bank.gateway.leader-election.namespace (POD_NAMESPACE) is empty" }
        running = true
        worker.execute { runElector() }
        // 라벨을 "지금 역할" 에 맞춘다 — 붙이기 · 떼기 실패, API 단절 동안 못 뗀 라벨, 컨테이너 재시작으로 남은 라벨을 주기마다 고친다
        worker.scheduleWithFixedDelay({ if (running) label.ensure(owner.isLeader) }, 0, props.retryPeriod.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun runElector() {
        if (!running) return
        val config = LeaderElectionConfigBuilder()
            .withName("bank-gateway")
            .withLock(LeaseLock(props.namespace, props.leaseName, props.identity))
            .withLeaseDuration(props.leaseDuration)
            .withRenewDeadline(props.renewDeadline)
            .withRetryPeriod(props.retryPeriod)
            .withReleaseOnCancel(true)
            .withLeaderCallbacks(
                LeaderCallbacks(
                    { submit { onStartLeading() } },
                    { submit { onStopLeading() } },
                    { holder -> log.info("leader election: current leader is {}", holder) },
                ),
            )
            .build()
        val future = k8s.leaderElector().withConfig(config).build().start()
        elector = future
        future.whenComplete { _, error ->
            if (error != null && error !is CancellationException) log.warn("leader election: elector ended: {}", error.message)
            if (running) runCatching { worker.schedule({ runElector() }, props.retryPeriod.toMillis(), TimeUnit.MILLISECONDS) }
        }
    }

    private fun submit(task: () -> Unit) {
        runCatching { worker.execute(task) }   // 종료 뒤 늦게 온 콜백은 버린다 (owner 는 이미 물러났다)
    }

    private fun onStartLeading() {
        if (!running) return
        log.info("leader election: became leader ({})", props.identity)
        owner.startLeading()   // 구간 예약 → 접속. 접속 전에는 readiness DOWN 이라 라벨이 붙어도 트래픽이 오지 않는다
        label.ensure(true)
    }

    private fun onStopLeading() {
        owner.stopLeading()
        log.warn("leader election: lost leadership ({}) — bank connection closed", props.identity)
        if (running) label.ensure(false)
    }

    override fun stop() {
        log.info("leader election: stepping down (drain {}ms)", props.drainTimeout.toMillis())
        running = false
        owner.stepDown(props.drainTimeout)   // 1 · 2 · 3: 거절 → 비우기 → 끊기
        label.ensure(false)                  // 4: Service 대상에서 빠진다
        elector?.cancel(true)                // 5: releaseOnCancel — holder 비움
        awaitReleased()
        worker.shutdownNow()
    }

    /** 반납이 API 서버에 닿을 때까지 최대 2초 — 프로세스가 먼저 끝나면 대기자가 만료(15s)를 기다린다. */
    private fun awaitReleased() {
        val deadline = System.nanoTime() + 2_000_000_000L
        while (System.nanoTime() < deadline) {
            val holder = runCatching { k8s.leases().inNamespace(props.namespace).withName(props.leaseName).get()?.spec?.holderIdentity }
                .getOrElse { return }   // API 서버가 없으면 기다려도 소용없다
            if (holder != props.identity) return
            Thread.sleep(100)
        }
        log.warn("leader election: lease release not observed within 2s — standby will wait for expiry")
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = LEADERSHIP_PHASE
}
