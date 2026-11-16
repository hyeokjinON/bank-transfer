package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.fabric8.kubernetes.api.model.ConfigMapBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.protocol.Header
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 전문번호 구간 예약 (P8-3b 스펙 §5, 결정 8 · 9). ConfigMap `date` · `reservedTo` 를 resourceVersion 조건부 update 로 늘려
 * [blockSize] 개씩 가져온다 (hi/lo). 번호 유일성이 선출과 독립 — 게이트웨이 두 대가 잠깐 둘 다 리더라고 믿어도 구간은 겹치지 않는다.
 *
 * - [next] 는 막히지 않는다 (Netty 이벤트 루프 · WebFlux 스레드에서 부른다). 구간이 없으면 백그라운드 예약을 걸고 BankNotSentException.
 * - 구간에 [prefetchRemaining] 개 미만이 남으면 다음 구간을 미리 예약한다. 쓰지 못한 번호는 건너뛴다 (유일성만 요구 — 추정).
 * - 날짜(KST)가 바뀌면 새 날짜 구간을 1부터 (점검 시간 23:50 ~ 00:10 안이라 송신은 원래 막혀 있다 — 결정 10).
 * - 저장된 날짜가 더 새로우면 예약하지 않는다 (시계가 늦은 예약자가 날짜를 되돌려 번호를 다시 내는 것을 막는다).
 * - 저장된 기록이 깨졌으면 덮지 않고 BankNotSentException (운영자가 고친다).
 */
class ConfigMapTranNoSource(
    private val k8s: KubernetesClient,
    private val namespace: String,
    private val name: String,
    private val clock: Clock,
    private val blockSize: Long = 1000,
    private val prefetchRemaining: Long = 100,
    private val maxAttempts: Int = 5,
) : TranNoSource, AutoCloseable {

    private class Block(val date: String, var next: Long, val last: Long)

    private val log = LoggerFactory.getLogger(javaClass)
    private val reserver = Executors.newSingleThreadExecutor { r -> Thread(r, "tran-no-reserve").apply { isDaemon = true } }
    private val reserving = AtomicBoolean(false)
    private var current: Block? = null    // this 로 보호
    private var upcoming: Block? = null   // this 로 보호

    override fun prepare() {
        val today = today()
        try {
            val range = reserve(today)
            synchronized(this) {
                current = Block(today, range.first, range.last)
                upcoming = null
            }
            log.info("tran no block reserved {} {}..{} (term start)", today, range.first, range.last)
        } catch (e: Exception) {
            log.warn("tran no block reservation failed at term start — sending stays blocked until it succeeds: {}", e.message)
        }
    }

    @Synchronized
    override fun next(): Long {
        val today = today()
        var block = current?.takeIf { it.date == today && it.next <= it.last }
        if (block == null) {
            block = upcoming?.takeIf { it.date == today }
            current = block
            upcoming = null
            if (block == null) {
                reserveAsync(today)
                throw BankNotSentException("no tran no block reserved for $today yet")
            }
        }
        val number = block.next++
        if (block.last - number < prefetchRemaining && upcoming == null) reserveAsync(today)
        return number
    }

    private fun reserveAsync(date: String) {
        if (!reserving.compareAndSet(false, true)) return
        try {
            reserver.execute {
                try {
                    val range = reserve(date)
                    synchronized(this) { upcoming = Block(date, range.first, range.last) }
                    log.info("tran no block reserved {} {}..{}", date, range.first, range.last)
                } catch (e: Exception) {
                    log.warn("tran no block reservation failed: {}", e.message)
                } finally {
                    reserving.set(false)
                }
            }
        } catch (e: RejectedExecutionException) {
            reserving.set(false)   // 닫는 중
        }
    }

    /** 구간 하나를 예약한다 (막힘 — API 서버 왕복). 다른 예약자와 겹치면 409 → 다시 읽고 반복. */
    internal fun reserve(date: String): LongRange {
        val configMaps = k8s.configMaps().inNamespace(namespace)
        repeat(maxAttempts) {
            val existing = configMaps.withName(name).get()
            if (existing == null) {
                val created = ConfigMapBuilder().withNewMetadata().withName(name).endMetadata()
                    .addToData(DATE, date).addToData(RESERVED_TO, "$blockSize").build()
                try {
                    configMaps.resource(created).create()
                    return 1L..blockSize
                } catch (e: KubernetesClientException) {
                    if (e.code != CONFLICT) throw e
                    return@repeat   // 다른 예약자가 먼저 만들었다 — 다시 읽는다
                }
            }
            val storedDate = existing.data?.get(DATE)
            val storedTo = existing.data?.get(RESERVED_TO)?.toLongOrNull()
            // 깨진 기록은 덮지 않는다 — 덮으면 번호를 다시 낼 수 있다. 운영자가 고친다.
            if (storedDate == null || !DATE_FORMAT.matches(storedDate) || storedTo == null || storedTo < 0) {
                throw BankNotSentException("tran no record is corrupted (date=$storedDate, reservedTo=${existing.data?.get(RESERVED_TO)}) — operator must fix $name")
            }
            // yyyyMMdd 는 문자열 순서 = 날짜 순서. 저장된 날짜가 더 새로우면 시계가 늦은 예약자 — 되돌리지 않는다.
            if (storedDate > date) throw BankNotSentException("stored date $storedDate is newer than requested $date — not reserving")
            val reservedTo = if (storedDate == date) storedTo else 0L
            val start = reservedTo + 1
            val last = start + blockSize - 1
            if (last > Header.MAX_TRAN_NO) throw BankNotSentException("tran no exhausted for $date (reservedTo=$reservedTo)")
            existing.data = mapOf(DATE to date, RESERVED_TO to "$last")   // resourceVersion 은 읽은 값 그대로 — 그 사이 바뀌었으면 409
            try {
                configMaps.resource(existing).update()
                return start..last
            } catch (e: KubernetesClientException) {
                if (e.code != CONFLICT) throw e
            }
        }
        throw BankNotSentException("tran no reservation conflicted $maxAttempts times")
    }

    private fun today(): String = LocalDate.ofInstant(clock.instant(), KST).format(DateTimeFormatter.BASIC_ISO_DATE)

    override fun close() {
        reserver.shutdownNow()
    }

    companion object {
        private val KST: ZoneId = ZoneId.of("Asia/Seoul")
        private const val DATE = "date"
        private val DATE_FORMAT = Regex("[0-9]{8}")
        private const val RESERVED_TO = "reservedTo"
        private const val CONFLICT = 409
    }
}
