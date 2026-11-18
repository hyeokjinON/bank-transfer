package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.support.TestSpans
import io.github.hyeokjinon.banktransfer.api.support.attr
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.trace.data.SpanData
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpStatusCode
import org.springframework.http.MediaType
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 금지 속성 (P9b 결정 E9, 스펙 §8 통합 7): 정상 출금 · 잔고 부족(도메인 예외 메시지에 사용자 id · 금액) · 입력 검증 실패(거절된 값) ·
 * 1원인증(예금주 · 코드)을 지난 뒤, 내보낸 어떤 구간의 이름 · 속성 · 사건 속성 · 상태 설명에도 민감값이 없다 (지우개 포함 — 운영과 같은 경로).
 */
class SensitiveAttributesTest : ApiIntegrationTest() {
    @Autowired private lateinit var spans: TestSpans
    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var tracer: io.micrometer.tracing.Tracer

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun postTransfer(userId: String, key: String, accountNo: String, amount: Long): Pair<HttpStatusCode, Map<String, Any?>?> =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, userId).header("Idempotency-Key", key)
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "SA"))
            .exchange { _, res -> res.statusCode to runCatching { res.bodyTo(mapType) }.getOrNull() }

    private fun strings(span: SpanData): List<String> =
        listOf(span.name, span.status.description) +
            span.attributes.asMap().values.map { it.toString() } +
            span.events.flatMap { e -> listOf(e.name) + e.attributes.asMap().values.map { it.toString() } }

    @Test
    fun `어떤 구간에도 금액 · 계좌번호 · 예금주 · 1원인증 코드 · Idempotency-Key · 사용자 id 가 없다`() {
        spans.clear()
        val payer = fundedUser(8_000_000)
        val account = verifiedBy(payer, "1002123456789")
        val okKey = "idem-ok-" + UUID.randomUUID()
        val poorKey = "idem-poor-" + UUID.randomUUID()

        // 1) 정상 출금 7,777,771원
        val (okStatus, okBody) = postTransfer(payer, okKey, account, 7_777_771)
        assertEquals(202, okStatus.value())
        val id = UUID.fromString(okBody!!["id"] as String)
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        // 2) 잔고 부족 — StoreConflict.InsufficientBalance 메시지 = "balance of '<사용자 id>' is less than 7777771"
        assertEquals(422, postTransfer(payer, poorKey, account, 7_777_771).first.value())
        // 3) 입력 검증 실패 — 거절된 값이 예외 메시지에 들어간다
        assertEquals(400, postTransfer(payer, "idem-bad-" + UUID.randomUUID(), "SECRET778899", 1_000).first.value())
        // 4) 1원인증 (직결) — 요청 · 코드 확인, 그리고 예금주가 다른 요청
        val verifier = newUserId()
        val created = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, verifier)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .retrieve().body(mapType)!!
        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, verifier)
            .body(mapOf("code" to code)).retrieve().toBodilessEntity()
        client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, verifier)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길순비밀"))
            .exchange { _, res -> res.statusCode }

        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            assertTrue(spans.all().count { it.kind == SpanKind.SERVER } >= 6, "서버 구간 6개(출금 3 · 1원인증 3)를 기다린다")
            assertTrue(spans.all().any { it.kind == SpanKind.CONSUMER && it.attr(TraceKeys.TRANSFER_ID) == id.toString() })
        }

        // 전제 (E9 가 헛돌지 않게): 지울 것이 실제로 있었다. MVC 예외 처리기(ApiExceptionHandler)가 받은 422 · 400 은 구간에 예외로 남지 않는다
        // (구간은 정상 종료 + 상태 코드만) — 그래서 지우개가 일할 길을 직접 연다: 잔고 부족 · 검증 실패 메시지와 같은 모양의 예외를 구간에 남긴다.
        val probe = tracer.nextSpan().name("e9.probe").start()
        probe.error(IllegalStateException("balance of '$payer' is less than 7777771 / account SECRET778899"))
        probe.end()
        val rawProbe = spans.raw().filter { it.name == "e9.probe" }.flatMap(::strings)
        assertTrue(rawProbe.any { payer in it && "7777771" in it && "SECRET778899" in it }, "지우기 전 구간에 민감값이 없다 — 지울 것이 없었다: $rawProbe")

        val all = spans.all().flatMap(::strings)
        listOf("7777771", "1002123456789", "SECRET778899", "홍길동", "홍길순비밀", okKey, poorKey, payer, verifier).forEach { secret ->
            assertTrue(all.none { secret in it }, "구간에 민감값 '$secret' 이 있다")
        }
        // 코드는 짧은 숫자라 다른 숫자 속성값과 우연히 같을 수 있다 — 코드가 실린 적요 모양("BT" + 코드)과, 코드가 든 지우기 전 구간이 있다면 그 문자열만 본다
        assertTrue(all.none { "BT$code" in it }, "구간에 1원인증 코드(적요)가 있다")
    }
}
