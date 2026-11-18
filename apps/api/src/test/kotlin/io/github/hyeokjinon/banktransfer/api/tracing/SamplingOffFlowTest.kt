package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 추적 끔 = 샘플링 0 (P9b 결정 E10, 스펙 §8 통합 6 · 실험 T1 의 "끔"): 돈 흐름은 그대로, 내보낸 구간 0, 전파는 계속(표본 비트 꺼짐).
 * 대표 흐름 둘 — 출금 · 1원인증. 설정이 달라 컨텍스트 · 컨테이너가 한 벌 더 뜬다 — 클래스 끝에 닫는다.
 */
@TestPropertySource(properties = ["management.tracing.sampling.probability=0.0"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SamplingOffFlowTest : ApiIntegrationTest() {
    @Autowired private lateinit var spans: TestSpans
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    @Test
    fun `샘플링 0 - 출금은 그대로 COMPLETED · 은행 1건, 1원인증은 그대로 VERIFIED, 내보낸 구간 0, 아웃박스에는 표본 꺼진 traceparent`() {
        spans.clear()
        val user = fundedUser(100_000)
        val account = verifiedBy(user, "1002123456789")
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "OFF"))
            .retrieve().body(mapType)!!
        val id = UUID.fromString(body["id"] as String)

        val stored = jdbc.queryForObject("SELECT trace_parent FROM outbox_event WHERE aggregate_id = ?", String::class.java, id.toString())
        // 끝 깃발은 리터럴로 비교하지 않는다 — W3C 레벨 2 의 random-trace-id 비트(02)가 붙을 수 있다. 표본 비트(01)만 꺼져 있으면 된다
        assertTrue(OutboxTracing.isValid(stored), "trace_parent = $stored")
        assertEquals(0, stored!!.takeLast(2).toInt(16) and 1, "표본 비트가 켜져 있다: $stored")

        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        assertEquals(1, bank.depositsOf(account).count { it.reference == store.findById(id)!!.reference })

        // 1원인증: 요청 → 은행 입금 적요의 코드 → 확인 → VERIFIED
        val verifier = newUserId()
        val created = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, verifier)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .retrieve().body(mapType)!!
        assertEquals("SENT", created["status"])
        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        val confirmed = client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, verifier)
            .body(mapOf("code" to code)).retrieve().body(mapType)!!
        assertEquals("VERIFIED", confirmed["status"])

        assertTrue(spans.all().isEmpty(), "샘플링 0 인데 내보낸 구간: ${spans.all().map { it.name }}")
    }
}
