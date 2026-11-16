package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaderElectionRecord
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.Lock
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
 * - 자기 차단: 마지막으로 성공한 갱신 "시도의 시작"(lastRenewOk — fabric8 이 renewTime 을 찍는 시점이고 대기자는 이것으로 만료를 센다) 뒤
 *   갱신 기한(10s)이 지나면 워치독 스레드(gateway-leader-watchdog)가 직접 연결을 끊는다 — fabric8 콜백 · 작업 큐 · API 서버 응답 여부와 무관하다.
 *   상한: 마지막 성공 갱신 시도 시작 + 갱신 기한 + 250ms (API 서버가 거절하든, 패킷이 버려져 응답이 없든 같다).
 *   끊은 뒤에는 그 선출기도 cancel 한다 (임기당 한 번) — 늦게 성공한 갱신 하나로 fabric8 이 "아직 리더" 라고 믿고 Lease 를 계속 갱신하면
 *   대기자도 못 얻고 우리는 연결이 없는 "리더 없는 상태" 로 굳는다. cancel 은 반납 후 선출기 future 를 끝내 runElector 가 새로 후보가 되게 한다.
 *   fabric8 자체 경로는 요청 타임아웃 + 반납용 조회 + 큐 대기가 겹쳐 기한을 넘길 수 있어 믿지 않는다 (릴리스 시 release 의 Lock.get 이 먼저 돈다).
 *   새 리더는 만료(15s) 뒤 — 겹침 없음 (시계 차 < 5s 전제).
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

    private val watchdog = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "gateway-leader-watchdog").apply { isDaemon = true } }

    @Volatile private var running = false
    @Volatile private var lastRenewOk = System.nanoTime()
    @Volatile private var elector: CompletableFuture<*>? = null
    @Volatile private var fencedElector: CompletableFuture<*>? = null   // 차단하며 이미 cancel 한 선출기 (임기당 한 번)

    override fun start() {
        require(props.identity.isNotBlank()) { "bank.gateway.leader-election.identity (POD_NAME) is empty" }
        require(props.namespace.isNotBlank()) { "bank.gateway.leader-election.namespace (POD_NAMESPACE) is empty" }
        running = true
        worker.execute { runElector() }
        watchdog.scheduleWithFixedDelay({ runCatching { fenceIfRenewOverdue() } }, WATCHDOG_TICK_MS, WATCHDOG_TICK_MS, TimeUnit.MILLISECONDS)
        // 라벨을 "지금 역할" 에 맞춘다 — 붙이기 · 떼기 실패, API 단절 동안 못 뗀 라벨, 컨테이너 재시작으로 남은 라벨을 주기마다 고친다
        worker.scheduleWithFixedDelay({ if (running) label.ensure(owner.isLeader) }, 0, props.retryPeriod.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun fenceIfRenewOverdue() {
        if (!running || !owner.isLeader) return
        if (System.nanoTime() - lastRenewOk <= props.renewDeadline.toNanos()) return
        owner.stopLeading()   // 멱등 — 뒤늦게 오는 fabric8 onStopLeading 은 무해하다
        log.warn("leader election: lost leadership ({}) — renew deadline exceeded, bank connection closed (self-fence)", props.identity)
        val current = elector
        if (current != null && current !== fencedElector) {
            fencedElector = current
            current.cancel(true)   // 반납(동기, 최대 요청 타임아웃)하고 future 를 끝낸다 → whenComplete 가 재후보. 워치독이 한 번 막혀도 연결은 이미 끊겼다
        }
    }

    /** 갱신(create/update)이 API 서버에서 성공했고 holder 가 나일 때만 시각을 갱신한다. 반납(holder 비움)은 해당 없음. */
    private inner class TrackingLock(private val delegate: Lock) : Lock by delegate {
        private val attemptStart = ThreadLocal<Long>()   // fabric8 은 한 시도(get → create/update)를 한 스레드에서 한다

        override fun get(client: KubernetesClient): LeaderElectionRecord? {
            attemptStart.set(System.nanoTime())
            return delegate.get(client)
        }

        override fun create(client: KubernetesClient, record: LeaderElectionRecord) {
            delegate.create(client, record)
            markRenewed(record)
        }

        override fun update(client: KubernetesClient, record: LeaderElectionRecord) {
            delegate.update(client, record)
            markRenewed(record)
        }

        private fun markRenewed(record: LeaderElectionRecord) {
            if (record.holderIdentity == props.identity) lastRenewOk = attemptStart.get() ?: System.nanoTime()
        }
    }

    private fun runElector() {
        if (!running) return
        val config = LeaderElectionConfigBuilder()
            .withName("bank-gateway")
            .withLock(TrackingLock(LeaseLock(props.namespace, props.leaseName, props.identity)))
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
        if (!running) future.cancel(true)   // stop() 과 동시에 만들어진 선출기도 반납한다
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
        label.ensure(owner.isLeader)   // 종료 중이라 startLeading 이 무시됐으면 붙이지 않는다
    }

    private fun onStopLeading() {
        val wasLeader = owner.isLeader
        owner.stopLeading()
        if (!running) return   // 정상 종료의 "released" 로그는 stop() 이 반납을 확인한 뒤 찍는다 (이 콜백은 worker 종료 뒤라 보통 버려진다)
        if (wasLeader) log.warn("leader election: lost leadership ({}) — bank connection closed", props.identity)
        label.ensure(false)
    }

    override fun stop() {
        log.info("leader election: stepping down (drain {}ms)", props.drainTimeout.toMillis())
        running = false
        owner.stepDown(props.drainTimeout)   // 1 · 2 · 3: 거절 → 비우기 → 끊기
        watchdog.shutdownNow()
        worker.shutdownNow()
        runCatching { worker.awaitTermination(2, TimeUnit.SECONDS) }   // 진행 중이던 라벨 맞추기가 끝난 뒤에 떼어야 마지막 말이 된다
        label.ensure(false)                  // 4: Service 대상에서 빠진다
        elector?.cancel(true)                // 5: releaseOnCancel — holder 비움
        if (awaitReleased()) log.info("leader election: released leadership ({})", props.identity)   // 정상 종료 — 비정상 상실(lost leadership)과 구분
    }

    /** 반납이 API 서버에 닿을 때까지 최대 2초 — 프로세스가 먼저 끝나면 대기자가 만료(15s)를 기다린다. */
    private fun awaitReleased(): Boolean {
        val deadline = System.nanoTime() + 2_000_000_000L
        while (System.nanoTime() < deadline) {
            val holder = runCatching { k8s.leases().inNamespace(props.namespace).withName(props.leaseName).get()?.spec?.holderIdentity }
                .getOrElse { return false }   // API 서버가 없으면 기다려도 소용없다
            if (holder != props.identity) return true
            Thread.sleep(100)
        }
        log.warn("leader election: lease release not observed within 2s — standby will wait for expiry")
        return false
    }

    private companion object {
        const val WATCHDOG_TICK_MS = 250L
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = LEADERSHIP_PHASE
}
