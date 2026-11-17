package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.scheduling.annotation.SchedulingConfigurer
import org.springframework.scheduling.config.ScheduledTaskRegistrar
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

@ConfigurationProperties("banktransfer.metrics")
data class MetricsProperties(
    /** 상태 Gauge 의 DB 조회를 켤지. 테스트는 끄고 refresh() 를 직접 부른다 (스펙 §4-2). */
    val dbPollEnabled: Boolean = true,
    /** 조회 주기 (TransferStatePoller 가 읽는다). */
    val dbPollInterval: Duration = Duration.ofSeconds(10),
)

/** 미완료 출금 · 아웃박스의 지금 모습 (P9a 스펙 §4-2). 나이는 초 — DB 시계(NOW(6))로 잰 값. 행이 없으면 건수 0 · 나이 0. */
data class TransferStateSnapshot(
    val open: Map<TransferStatus, Long>,
    val oldestSeconds: Map<TransferStatus, Double>,
    val unknownExhausted: Long,
    val outboxUnpublished: Long,
    val outboxOldestSeconds: Double,
) {
    companion object {
        /** 지표로 보는 미완료 상태 — 확정(COMPLETED · FAILED)은 쌓이기만 해서 읽지 않는다. */
        val OPEN_STATUSES = listOf(TransferStatus.RECEIVED, TransferStatus.SENDING, TransferStatus.UNKNOWN)
    }
}

/**
 * 상태 조회 SQL (P9a 스펙 §4-2). 나이는 SQL 에서 TIMESTAMPDIFF(MICROSECOND, x, NOW(6)) — 저장은 KST DATETIME, DB 세션 시간대(+09:00) 하나만 믿는다 (위험 A7).
 * SENDING · UNKNOWN 은 idx_transfer_status(status, sending_at) 안에서 끝난다. RECEIVED 의 updated_at 은 인덱스 밖이라 행을 읽는다 (위험 B1 — M4 에서 잰다).
 */
class TransferStateReader(private val jdbc: JdbcTemplate, private val maxInquiries: Int) {

    fun read(): TransferStateSnapshot {
        val open = TransferStateSnapshot.OPEN_STATUSES.associateWith { 0L }.toMutableMap()
        val oldest = TransferStateSnapshot.OPEN_STATUSES.associateWith { 0.0 }.toMutableMap()
        jdbc.query(SENDING_UNKNOWN_SQL, RowCallbackHandler { rs ->
            val status = TransferStatus.valueOf(rs.getString("status"))
            open[status] = rs.getLong("cnt")
            oldest[status] = rs.getLong("oldest_us") / MICROS
        })
        jdbc.query(RECEIVED_SQL, RowCallbackHandler { rs ->
            open[TransferStatus.RECEIVED] = rs.getLong("cnt")
            oldest[TransferStatus.RECEIVED] = rs.getLong("oldest_us") / MICROS   // 행이 없으면 NULL → 0
        })
        val exhausted = jdbc.queryForObject(EXHAUSTED_SQL, Long::class.java, maxInquiries) ?: 0L
        val unpublished = jdbc.queryForObject(OUTBOX_COUNT_SQL, Long::class.java) ?: 0L
        val outboxOldest = jdbc.query(OUTBOX_OLDEST_SQL) { rs, _ -> rs.getLong(1) / MICROS }.firstOrNull() ?: 0.0
        return TransferStateSnapshot(open, oldest, exhausted, unpublished, outboxOldest)
    }

    companion object {
        private const val MICROS = 1_000_000.0

        internal const val SENDING_UNKNOWN_SQL = """
SELECT status, COUNT(*) AS cnt, TIMESTAMPDIFF(MICROSECOND, MIN(sending_at), NOW(6)) AS oldest_us
  FROM transfer
 WHERE status IN ('SENDING', 'UNKNOWN')
 GROUP BY status"""

        internal const val RECEIVED_SQL = """
SELECT COUNT(*) AS cnt, TIMESTAMPDIFF(MICROSECOND, MIN(updated_at), NOW(6)) AS oldest_us
  FROM transfer
 WHERE status = 'RECEIVED'"""

        internal const val EXHAUSTED_SQL = "SELECT COUNT(*) FROM transfer WHERE status = 'UNKNOWN' AND inquiry_attempts >= ?"

        internal const val OUTBOX_COUNT_SQL = "SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL"

        /** 가장 작은 id = 가장 먼저 쌓인 행 (id 는 접수 순서로 는다). idx_outbox_unpublished(published_at, id) 안에서 찾고 그 행 하나만 읽는다. */
        internal const val OUTBOX_OLDEST_SQL = """
SELECT TIMESTAMPDIFF(MICROSECOND, created_at, NOW(6))
  FROM outbox_event
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT 1"""
    }
}

/**
 * 상태 Gauge (P9a 스펙 §4-2). 값은 마지막 조회 결과 하나 — 조회가 실패하면 버리고 NaN 을 낸다 (0 이 아니다 — 위험 A6).
 * api 2대가 같은 값을 낸다 → 대시보드 · 규칙은 max().
 */
class TransferStateGauges(
    private val registry: MeterRegistry,
    /** 조회가 멈추거나 스케줄이 서지 않아 이 시간보다 오래된 값은 NaN (위험 A6). 보통 조회 주기의 3배. */
    private val staleAfter: Duration = Duration.ofSeconds(30),
    /** 값의 신선도만 재는 단조 시계 (나이 계산이 아니다 — 나이는 SQL, 위험 A7). 테스트가 바꿔 끼운다. */
    private val nanoTime: () -> Long = System::nanoTime,
    private val read: () -> TransferStateSnapshot,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private class Reading(val snapshot: TransferStateSnapshot, val atNanos: Long)

    private val latest = AtomicReference<Reading?>(null)
    private val ok: Timer = Timer.builder(DB_QUERY).description("상태 지표 DB 조회 시간").tag("result", "ok").register(registry)
    private val error: Timer = Timer.builder(DB_QUERY).description("상태 지표 DB 조회 시간").tag("result", "error").register(registry)

    init {
        for (status in TransferStateSnapshot.OPEN_STATUSES) {
            Gauge.builder(OPEN, this) { it.value { s -> s.open.getValue(status).toDouble() } }
                .description("미완료 출금 수 (DB)").tag("status", status.name).register(registry)
            Gauge.builder(OPEN_OLDEST, this) { it.value { s -> s.oldestSeconds.getValue(status) } }
                .description("가장 오래된 미완료의 나이 초 — RECEIVED: updated_at, SENDING · UNKNOWN: sending_at").tag("status", status.name).register(registry)
        }
        Gauge.builder(UNKNOWN_EXHAUSTED, this) { it.value { s -> s.unknownExhausted.toDouble() } }
            .description("결과조회를 다 쓴 UNKNOWN (일마감 대사 대상)").register(registry)
        Gauge.builder(OUTBOX_UNPUBLISHED, this) { it.value { s -> s.outboxUnpublished.toDouble() } }
            .description("아웃박스 미발행 행").register(registry)
        Gauge.builder(OUTBOX_OLDEST, this) { it.value { s -> s.outboxOldestSeconds } }
            .description("가장 오래된 미발행 행의 나이 초").register(registry)
    }

    /** 한 번 조회한다. 실패하면 값을 NaN 으로 · error 를 센다 (이전 값을 들고 있지 않는다). 예외는 밖으로 내지 않는다. */
    fun refresh() {
        val sample = Timer.start(registry)
        try {
            val snapshot = read()
            latest.set(Reading(snapshot, nanoTime()))
            sample.stop(ok)
        } catch (e: Exception) {
            latest.set(null)
            sample.stop(error)
            log.warn("state metrics query failed — gauges are NaN until the next success: {}", e.toString())
        }
    }

    private fun value(pick: (TransferStateSnapshot) -> Double): Double {
        val reading = latest.get() ?: return Double.NaN
        return if (nanoTime() - reading.atNanos > staleAfter.toNanos()) Double.NaN else pick(reading.snapshot)
    }

    companion object {
        const val OPEN = "banktransfer.transfer.open"
        const val OPEN_OLDEST = "banktransfer.transfer.open.oldest.seconds"
        const val UNKNOWN_EXHAUSTED = "banktransfer.transfer.unknown.exhausted"
        const val OUTBOX_UNPUBLISHED = "banktransfer.outbox.unpublished"
        const val OUTBOX_OLDEST = "banktransfer.outbox.unpublished.oldest.seconds"
        const val DB_QUERY = "banktransfer.metrics.db.query"
    }
}

/** 상태 조회 주기 실행 (기본 10초). 끄면(banktransfer.metrics.db-poll-enabled=false) 스케줄을 등록하지 않아 Gauge 는 NaN 그대로. */
class TransferStatePoller(private val gauges: TransferStateGauges, private val properties: MetricsProperties) : SchedulingConfigurer {
    override fun configureTasks(registrar: ScheduledTaskRegistrar) {
        if (properties.dbPollEnabled) registrar.addFixedDelayTask(gauges::refresh, properties.dbPollInterval)
    }
}
