package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class TransferApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>, val location: String?)

    private fun post(key: String?, body: Map<String, Any>): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .apply { if (key != null) header("Idempotency-Key", key) }
            .body(body)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    private fun get(id: Any?): Response =
        client.get().uri("/api/v1/transfers/$id")
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), null) }

    /** 1원인증을 마친 계좌를 DB 에 만든다. 테스트끼리 DB 를 공유하므로 계좌번호는 매번 새로. */
    private fun verifiedAccount(): String {
        val accountNo = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)
        verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
        return accountNo
    }

    private fun body(accountNo: String, amount: Long = 50_000) =
        mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "출금")

    @Test
    fun `인증된 계좌로 요청하면 202 RECEIVED, 조회하면 같은 건`() {
        val created = post(UUID.randomUUID().toString(), body(verifiedAccount()))

        assertEquals(202, created.status)
        assertEquals("RECEIVED", created.body["status"])
        assertEquals("/api/v1/transfers/${created.body["id"]}", created.location)

        val found = get(created.body["id"])
        assertEquals(200, found.status)
        assertEquals(created.body["id"], found.body["id"])
        assertEquals(50_000, (found.body["amount"] as Number).toInt())
    }

    @Test
    fun `같은 키 같은 내용으로 다시 보내면 200 과 같은 id`() {
        val key = UUID.randomUUID().toString()
        val account = verifiedAccount()
        val first = post(key, body(account))
        val again = post(key, body(account))

        assertEquals(200, again.status)
        assertEquals(first.body["id"], again.body["id"])
    }

    @Test
    fun `대소문자만 다른 키는 서로 다른 키다 (바이트 비교)`() {
        val key = "case-" + UUID.randomUUID()
        val account = verifiedAccount()
        val lower = post(key, body(account))
        val upper = post(key.uppercase(), body(account))

        assertEquals(202, lower.status)
        assertEquals(202, upper.status)
        assertNotEquals(lower.body["id"], upper.body["id"])
    }

    @Test
    fun `같은 키에 금액을 바꾸면 422 IDEMPOTENCY_KEY_REUSED`() {
        val key = UUID.randomUUID().toString()
        val account = verifiedAccount()
        post(key, body(account))

        val reused = post(key, body(account, amount = 70_000))

        assertEquals(422, reused.status)
        assertEquals("IDEMPOTENCY_KEY_REUSED", reused.body["code"])
    }

    @Test
    fun `인증되지 않은 계좌는 422 ACCOUNT_NOT_VERIFIED`() {
        val response = post(UUID.randomUUID().toString(), body("1101234567890"))

        assertEquals(422, response.status)
        assertEquals("ACCOUNT_NOT_VERIFIED", response.body["code"])
    }

    @Test
    fun `키 헤더가 없으면 400 IDEMPOTENCY_KEY_REQUIRED`() {
        val response = post(null, body(verifiedAccount()))

        assertEquals(400, response.status)
        assertEquals("IDEMPOTENCY_KEY_REQUIRED", response.body["code"])
    }

    @Test
    fun `금액이 0 이면 400 INVALID_AMOUNT`() {
        val response = post(UUID.randomUUID().toString(), body(verifiedAccount(), amount = 0))

        assertEquals(400, response.status)
        assertEquals("INVALID_AMOUNT", response.body["code"])
    }

    @Test
    fun `없는 id 는 404 TRANSFER_NOT_FOUND`() {
        val response = get(UUID.randomUUID())

        assertEquals(404, response.status)
        assertEquals("TRANSFER_NOT_FOUND", response.body["code"])
    }

    @Test
    fun `같은 키로 동시에 10건 보내도 이체 1건 · 이벤트 1건`() {
        val key = UUID.randomUUID().toString()
        val request = body(verifiedAccount())
        val pool = Executors.newFixedThreadPool(10)
        val start = CountDownLatch(1)

        val responses = (1..10).map { pool.submit(Callable { start.await(); post(key, request) }) }
        start.countDown()
        val results = responses.map { it.get() }
        pool.shutdown()

        assertEquals(1, results.count { it.status == 202 })
        assertEquals(9, results.count { it.status == 200 })
        assertEquals(1, results.map { it.body["id"] }.toSet().size)
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE idempotency_key = ?", Int::class.java, key))
        assertEquals(
            1,
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event o JOIN transfer t ON o.aggregate_id = t.id WHERE t.idempotency_key = ?",
                Int::class.java, key,
            ),
        )
    }
}
