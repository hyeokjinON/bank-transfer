package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 1원인증 요청 블로킹 판 (P6-2 대조군). 판이 설정으로 갈려 이 클래스만 별도 컨텍스트다 — 끝나면 닫는다.
 * Tomcat 최대 스레드를 2로 줄여 "요청 스레드가 은행을 끝까지 기다린다" 를 시간으로 본다: 은행이 한 번에 250ms 걸리면 요청 하나 ≈ 500ms,
 * 동시 4건은 스레드 2개에 두 번 나뉘어 ≈ 1초. 비동기 판이면 스레드를 놓아 ≈ 0.5초에 다 끝난다 (변이 검사: join → future 반환이면 실패해야 한다).
 * 공유 모의 은행의 지연은 게이트웨이 응답 시간 초과(테스트 500ms)보다 짧게 두고 끝나면 되돌린다.
 */
@TestPropertySource(properties = ["account.verification.web-mode=blocking", "server.tomcat.threads.max=2"])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BlockingVerificationRequestTest : ApiIntegrationTest() {

    @Autowired private lateinit var context: ApplicationContext

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun request(userId: String): Int =
        client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() }

    @Test
    fun `blocking 이면 블로킹 컨트롤러 하나만 뜨고 info 에 보인다`() {
        assertEquals(1, context.getBeanNamesForType(BlockingVerificationRequestController::class.java).size)
        assertEquals(0, context.getBeanNamesForType(AsyncVerificationRequestController::class.java).size)
        val info = client.get().uri("/actuator/info").retrieve().body(String::class.java)!!
        assertTrue("\"web-mode\":\"blocking\"" in info, info)
        assertTrue(Regex("\"max\":\"?2\"?[,}]").containsMatchIn(info), info)
    }

    @Test
    fun `응답 · 저장 · 확인은 비동기 판과 같다`() {
        val user = newUserId()
        val (status, created) = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .exchange { _, res -> res.statusCode.value() to (res.bodyTo(mapType) ?: emptyMap()) }
        assertEquals(201, status)
        assertEquals("SENT", created["status"])

        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        val confirmed = client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .body(mapOf("code" to code))
            .exchange { _, res -> res.bodyTo(mapType) ?: emptyMap() }
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `Tomcat 스레드 2개면 은행을 기다리는 4건이 두 번에 나뉘어 끝난다 - 요청 스레드를 붙잡는다`() {
        request(newUserId())   // 데우기 (첫 요청의 클래스 로딩 · 커넥션이 재는 시간에 섞이지 않게)
        bank.responseDelay = Duration.ofMillis(250)
        val pool = Executors.newFixedThreadPool(4)
        try {
            val started = System.nanoTime()
            val results = (1..4).map { pool.submit<Int> { request(newUserId()) } }.map { it.get(10, TimeUnit.SECONDS) }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000

            assertEquals(listOf(201, 201, 201, 201), results)
            println("[P6-2 블로킹 판] Tomcat 스레드 2 · 은행 250ms × 2 · 동시 4건 → ${elapsedMs}ms")
            assertTrue(elapsedMs >= 900, "스레드 2개가 두 번에 나눠 처리해야 한다 (≈ 1000ms): ${elapsedMs}ms")
        } finally {
            bank.responseDelay = Duration.ZERO
            pool.shutdownNow()
        }
    }
}
