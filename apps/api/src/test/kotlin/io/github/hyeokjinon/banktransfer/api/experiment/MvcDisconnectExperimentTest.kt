package io.github.hyeokjinon.banktransfer.api.experiment

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.net.Socket
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * P6-2 진행 조건 실험 E2 의 MVC 쪽 (설계 §4-1 · §4-3). 서블릿 비동기(CompletableFuture 반환)는 클라이언트가 끊어도 future 를 취소하지 않아
 * 1원 이체 · 저장까지 끝난다 — WebFlux 판(끊기면 취소)과 다른 점이다. 결과는 ADR-0013 에 옮긴다.
 * 공유 모의 은행의 지연을 잠깐 바꾼다: 게이트웨이 응답 시간 초과(테스트 500ms)보다 짧게, 끝나면 되돌린다.
 */
class MvcDisconnectExperimentTest : ApiIntegrationTest() {

    @Autowired private lateinit var jdbc: JdbcTemplate

    @Test
    fun `E2 MVC 비동기 - 응답 전에 끊어도 1원 이체와 저장은 끝까지 간다`() {
        val user = newUserId()
        val bytes = """{"bankCode":"090","accountNo":"1002123456789","holderName":"홍길동"}""".toByteArray()
        bank.responseDelay = Duration.ofMillis(300)
        try {
            Socket("localhost", port).use { s ->
                s.getOutputStream().write(
                    ("POST /api/v1/account-verifications HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n" +
                        "$USER_ID_HEADER: $user\r\nContent-Length: ${bytes.size}\r\n\r\n").toByteArray() + bytes,
                )
                s.getOutputStream().flush()
                Thread.sleep(100)   // 성명조회 응답(300ms)을 기다리는 중에 끊는다
            }
            await.atMost(Duration.ofSeconds(5)).ignoreExceptions() untilAsserted {
                assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM account_verification WHERE user_id = ?", Int::class.java, user))
            }
            println("[P6-2 실험 E2 MVC] 끊긴 요청도 저장됨 (취소되지 않음)")
        } finally {
            bank.responseDelay = Duration.ZERO
        }
    }
}
