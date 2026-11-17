package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.micrometer.core.instrument.MeterRegistry
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * P9a: 앱 전체에서 세는 지점이 이어져 있는지 — 접수(created · replayed) → 발행 → 컨슈머 → 은행 거절 → FAILED 확정.
 * 공유 컨텍스트라 다른 테스트가 남긴 메시지가 같이 처리될 수 있다: 접수는 정확히(HTTP 요청만 늘린다), 처리 쪽은 "최소 +1" 로 본다.
 */
class TransferMetricsFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var registry: MeterRegistry
    @Autowired private lateinit var relay: OutboxRelay

    private fun counter(name: String, key: String, value: String) = registry.find(name).tag(key, value).counter()?.count() ?: 0.0
    private fun timerCount(status: String) = registry.find(TransferMetrics.TRANSFER_COMPLETED).tag("status", status).timer()?.count() ?: 0L

    private fun post(userId: String, key: String, accountNo: String): Int =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .header("Idempotency-Key", key)
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 1_000, "memo" to "metrics"))
            .exchange { _, r -> r.statusCode.value() }

    @Test
    fun `접수 · 같은 키 재요청 · 은행 거절 확정을 센다 - 잔고 부족 422 는 세지 않는다`() {
        val created0 = counter(TransferMetrics.ACCEPTED, "result", "created")
        val replayed0 = counter(TransferMetrics.ACCEPTED, "result", "replayed")
        val rejected0 = counter(TransferMetrics.BANK_RESULT, "outcome", "rejected")
        val failed0 = timerCount("FAILED")

        val user = fundedUser()
        val account = verifiedBy(user, unregisteredAccount())   // 모의 은행에 없는 계좌 → 1001 거절 → FAILED + 환불
        val key = "metrics-${UUID.randomUUID()}"
        assertEquals(202, post(user, key, account))
        assertEquals(200, post(user, key, account))
        val poor = newUserId()
        assertEquals(422, post(poor, "metrics-${UUID.randomUUID()}", verifiedBy(poor, unregisteredAccount())))

        assertEquals(created0 + 1, counter(TransferMetrics.ACCEPTED, "result", "created"))
        assertEquals(replayed0 + 1, counter(TransferMetrics.ACCEPTED, "result", "replayed"))

        relay.publishPending()
        await.atMost(Duration.ofSeconds(15)) untilAsserted {
            assertTrue(counter(TransferMetrics.BANK_RESULT, "outcome", "rejected") >= rejected0 + 1)
            assertTrue(timerCount("FAILED") >= failed0 + 1)
        }
    }
}
