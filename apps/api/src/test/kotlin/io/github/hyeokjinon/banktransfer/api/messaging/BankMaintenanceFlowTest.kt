package io.github.hyeokjinon.banktransfer.api.messaging

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.BankMaintenanceWindow
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 은행 점검 시간 흐름 (P5-1). 점검 중에는 아무것도 은행에 가지 않는다 — 점검 전에 접수된 출금은 게이트웨이의 보내기 문에 막혀
 * "연결 없음" 과 같은 길(RECEIVED → 재시도 토픽 → DLT → 재발행)을 타고, 점검이 끝나면 처리된다. 시계를 옮겨 만든다.
 */
class BankMaintenanceFlowTest : ApiIntegrationTest() {

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var reconciler: TransferReconciler
    @Autowired private lateinit var window: BankMaintenanceWindow

    private val seoul = ZoneId.of("Asia/Seoul")
    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private fun kst(day: LocalDate, h: Int, m: Int): Instant = day.atTime(h, m).atZone(seoul).toInstant()

    @Test
    fun `기본 설정은 23시 50분 ~ 00시 10분 (KST), 켜짐 - yml 의 시각은 따옴표로 문자열`() {
        assertTrue(window.enabled)
        assertEquals(LocalTime.of(23, 50), window.start)
        assertEquals(LocalTime.of(0, 10), window.end)
        assertEquals(seoul, window.zone)
    }

    @Test
    fun `점검 전에 접수된 출금은 점검 중 은행에 가지 않고 RECEIVED - 점검이 끝나면 COMPLETED`() {
        val user = fundedUser()
        val account = verifiedBy(user, "1002123456789")
        val day = LocalDate.now(seoul)

        testClock.fix(kst(day, 23, 49))   // 점검 직전 접수 (발행기는 테스트에서 꺼져 있어 아직 은행에 가지 않는다)
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to account, "amount" to 10_000, "memo" to "점검"))
            .retrieve().body(mapType)!!
        val id = UUID.fromString(body["id"] as String)
        val reference = store.findById(id)!!.reference

        testClock.fix(kst(day, 23, 55))   // 점검 중 발행 → 컨슈머가 보내려 했지만 보내기 문이 닫혀 있다
        await.atMost(Duration.ofSeconds(10)) untilAsserted {
            relay.publishPending()
            val transfer = store.findById(id)!!
            assertEquals(TransferStatus.RECEIVED, transfer.status)
            assertNotNull(transfer.sendingAt)   // 보내려 한 흔적 — 재발행 대상이 된다
        }
        assertFalse(bank.hasProcessed(reference))

        testClock.fix(kst(day.plusDays(1), 0, 11))   // 점검이 끝났다: 재시도 토픽이 남아 있으면 그쪽이, DLT 로 갔으면 재발행이 처리한다
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            reconciler.reconcile()
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        assertEquals(1, bank.depositsOf(account).count { it.reference == reference })   // 한 번만 갔다
    }
}
