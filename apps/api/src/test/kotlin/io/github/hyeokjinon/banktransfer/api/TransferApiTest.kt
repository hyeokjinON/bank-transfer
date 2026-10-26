package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class TransferApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>, val location: String?)

    /** 이 테스트의 사용자 (테스트마다 새로, 1천만 원 충전 — 출금은 이 지갑에서 빠진다, P4-1). */
    private val user by lazy { fundedUser() }

    private fun post(key: String?, body: Map<String, Any>, userId: String = user): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .apply { if (key != null) header("Idempotency-Key", key) }
            .body(body)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    /** 직렬화를 거치지 않고 JSON 문자열을 그대로 보낸다 (소수 · 문자열 금액 같은 잘못된 형식을 재현). */
    private fun postRaw(key: String, json: String): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", key)
            .body(json)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    private fun get(id: Any?, userId: String = user): Response =
        client.get().uri("/api/v1/transfers/$id")
            .header(USER_ID_HEADER, userId)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), null) }

    /**
     * [userId] 가 1원인증을 마친 계좌. 모의 은행에 없는 번호라, 여기서 남긴 RECEIVED 를 다른 테스트의 발행 · 정리 작업이 처리해도
     * 1001 거절 → FAILED → 환불로 끝난다 (공용 계좌에 입금이 생기지 않는다).
     */
    private fun verifiedAccount(userId: String = user): String = verifiedBy(userId, unregisteredAccount())

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
    fun `금액이 소수이거나 문자열이면 400`() {
        val account = verifiedAccount()
        val fraction = postRaw(UUID.randomUUID().toString(), """{"bankCode":"090","accountNo":"$account","amount":50000.5,"memo":"출금"}""")
        val text = postRaw(UUID.randomUUID().toString(), """{"bankCode":"090","accountNo":"$account","amount":"70000","memo":"출금"}""")

        assertEquals(400, fraction.status)
        assertEquals(400, text.status)
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
    fun `JSON 이 깨지면 400 INVALID_REQUEST`() {
        val response = postRaw(UUID.randomUUID().toString(), """{"bankCode": "090", """)

        assertEquals(400, response.status)
        assertEquals("INVALID_REQUEST", response.body["code"])
    }

    @Test
    fun `조회 id 가 UUID 가 아니면 400 INVALID_REQUEST`() {
        val response = get("not-a-uuid")

        assertEquals(400, response.status)
        assertEquals("INVALID_REQUEST", response.body["code"])
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

    @Test
    fun `잔고보다 큰 출금은 422 INSUFFICIENT_BALANCE 이고 출금도 차감도 남지 않는다`() {
        val poor = fundedUser(49_999)
        val key = UUID.randomUUID().toString()

        val response = post(key, body(verifiedAccount(poor)), userId = poor)

        assertEquals(422, response.status)
        assertEquals("INSUFFICIENT_BALANCE", response.body["code"])
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE user_id = ? AND idempotency_key = ?", Int::class.java, poor, key))
        assertEquals(49_999L, wallets.balanceOf(poor))
    }

    @Test
    fun `남이 인증한 계좌로는 출금할 수 없다 - 422 ACCOUNT_NOT_VERIFIED`() {
        val othersAccount = verifiedAccount(newUserId())

        val response = post(UUID.randomUUID().toString(), body(othersAccount))

        assertEquals(422, response.status)
        assertEquals("ACCOUNT_NOT_VERIFIED", response.body["code"])
    }

    @Test
    fun `남의 출금을 조회하면 404 TRANSFER_NOT_FOUND - 존재 여부도 숨긴다`() {
        val created = post(UUID.randomUUID().toString(), body(verifiedAccount()))

        val response = get(created.body["id"], userId = newUserId())

        assertEquals(404, response.status)
        assertEquals("TRANSFER_NOT_FOUND", response.body["code"])
    }

    @Test
    fun `같은 키 · 같은 내용이라도 다른 사용자면 각자 새 출금이다 (202 두 번)`() {
        val key = UUID.randomUUID().toString()
        val other = fundedUser()
        val account = unregisteredAccount()
        verifiedBy(user, account)
        verifiedBy(other, account)

        val mine = post(key, body(account))
        val theirs = post(key, body(account), userId = other)

        assertEquals(202, mine.status)
        assertEquals(202, theirs.status)          // 전역 UNIQUE 였다면 남의 출금을 200 으로 돌려받았다
        assertNotEquals(mine.body["id"], theirs.body["id"])
    }
}
