package io.github.hyeokjinon.banktransfer.reactive.web

import io.github.hyeokjinon.banktransfer.reactive.support.VerificationSuite
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 기본 판(코루틴)의 조립 · 요청 · 점검 + 공통(VerificationSuite). */
class CoroutineModeTest : VerificationSuite() {

    @Autowired private lateinit var context: ApplicationContext

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun post(userId: String) = client.post().uri("/api/v1/account-verifications")
        .contentType(MediaType.APPLICATION_JSON)
        .header(USER_ID_HEADER, userId)
        .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
        .exchange { _, res -> Triple(res.statusCode.value(), res.headers, res.bodyTo(mapType) ?: emptyMap()) }

    @Test
    fun `기본 설정이면 코루틴 컨트롤러 하나만 뜨고 info 에 판 · 커넥션 풀이 보인다`() {
        assertEquals(1, context.getBeanNamesForType(CoroutineVerificationController::class.java).size)
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"web-mode\":\"coroutine\"" in info, info)
        assertTrue(Regex("\"maximum-pool-size\":\"?20\"?").containsMatchIn(info), info)
    }

    @Test
    fun `요청하면 201 SENT, DB 에 요청자의 행, 은행에 코드를 실은 1원`() {
        val user = newUserId()
        val before = bank.depositsOf("1002123456789").size

        val (status, _, body) = post(user)

        assertEquals(201, status)
        assertEquals("SENT", body["status"])
        assertEquals(1, rowsOf(user))
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertTrue(bank.depositsOf("1002123456789").last().memo.startsWith("BT"))
    }

    @Test
    fun `점검 시간이면 503 BANK_MAINTENANCE + Retry-After, 은행에 보내지 않는다`() {
        val seoul = ZoneId.of("Asia/Seoul")
        testClock.fix(LocalDate.now(seoul).atTime(23, 55).atZone(seoul).toInstant())
        val before = bank.depositsOf("1002123456789").size

        val (status, headers, body) = post(newUserId())

        assertEquals(503, status)
        assertEquals("BANK_MAINTENANCE", body["code"])
        assertEquals((15 * 60).toString(), headers.getFirst(HttpHeaders.RETRY_AFTER))
        assertEquals(before, bank.depositsOf("1002123456789").size)
    }
}
