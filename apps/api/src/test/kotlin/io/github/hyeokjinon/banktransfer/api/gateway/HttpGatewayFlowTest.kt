package io.github.hyeokjinon.banktransfer.api.gateway

import io.github.hyeokjinon.banktransfer.api.config.BankInquiryAdapter
import io.github.hyeokjinon.banktransfer.api.messaging.OutboxRelay
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.api.web.USER_ID_HEADER
import io.github.hyeokjinon.banktransfer.gateway.http.HttpBankGateway
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.GatewayProcess
import io.github.hyeokjinon.banktransfer.gateway.http.fixture.TcpCutProxy
import io.github.hyeokjinon.banktransfer.transfer.TransferReconciler
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

/**
 * mode=http (P8-1, 스펙 §7): api → (끊는 프록시) → 게이트웨이 앱(별도 JVM) → 모의 은행.
 * 돈이 걸린 두 경로가 분리 뒤에도 그대로인지: 게이트웨이가 꺼짐 → 안 보냄 → 재시도 토픽, 받고 끊음 → 모름 → 결과조회.
 * 컨텍스트가 기본 테스트와 달라(새 MySQL · Kafka 빈) 클래스 끝에 닫는다 (P7: 닫히지 않은 컨텍스트의 리스너가 CI 4코어를 묶은 일).
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HttpGatewayFlowTest : ApiIntegrationTest() {

    companion object {
        private val gatewayApp: GatewayProcess by lazy { GatewayProcess.start(bank.boundPort, "--bank.gateway.response-timeout=500ms") }
        val proxy: TcpCutProxy by lazy { TcpCutProxy(gatewayApp.port).start() }

        @JvmStatic
        @DynamicPropertySource
        fun httpGateway(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.mode") { "http" }
            registry.add("bank.gateway.http.base-url") { proxy.baseUrl }
            registry.add("bank.gateway.http.response-timeout") { "2s" }
        }

        @JvmStatic
        @AfterAll
        fun stopGateway() { proxy.close(); gatewayApp.close() }
    }

    @Autowired private lateinit var relay: OutboxRelay
    @Autowired private lateinit var store: TransferStore

    override fun bankConnected(): Boolean = gatewayApp.bankConnected()

    @AfterEach
    fun resetProxy() { proxy.mode = TcpCutProxy.Mode.PASS; proxy.resume() }

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}
    private val user by lazy { fundedUser(1_000_000) }

    private fun request(accountNo: String, memo: String): UUID {
        val body = client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .header(USER_ID_HEADER, user)
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .body(mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to 10_000, "memo" to memo))
            .retrieve().body(mapType)!!
        return UUID.fromString(body["id"] as String)
    }

    private fun memo() = "H" + Random.nextInt(100_000, 999_999)

    @Test
    fun `http 모드에서는 Netty 직결 빈 대신 HTTP 클라이언트가 은행 게이트웨이다`() {
        assertIs<HttpBankGateway>(gateway)
    }

    @Test
    fun `출금이 게이트웨이 앱을 거쳐 COMPLETED, 은행 입금 1건`() {
        val account = verifiedBy(user, "1002123456789")
        val id = request(account, memo())
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status)
        }
        val ref = store.findById(id)!!.reference
        assertEquals(1, bank.depositsOf(account).count { it.reference == ref })
    }

    @Test
    fun `1원인증 SENT 뒤 적요 코드로 VERIFIED`() {
        val userId = newUserId()
        val created = client.post().uri("/api/v1/account-verifications")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, userId)
            .body(mapOf("bankCode" to "090", "accountNo" to "1002123456789", "holderName" to "홍길동"))
            .retrieve().body(mapType)!!
        assertEquals("SENT", created["status"])
        val code = bank.depositsOf("1002123456789").last { it.memo.startsWith("BT") }.memo.removePrefix("BT")
        val confirmed = client.post().uri("/api/v1/account-verifications/${created["id"]}/confirm")
            .contentType(MediaType.APPLICATION_JSON).header(USER_ID_HEADER, userId)
            .body(mapOf("code" to code))
            .retrieve().body(mapType)!!
        assertEquals("VERIFIED", confirmed["status"])
    }

    @Test
    fun `게이트웨이가 꺼져 있으면 안 보냄 - UNKNOWN 이 되지 않고, 다시 켜지면 재시도로 COMPLETED`() {
        val account = verifiedBy(user, "1002123456789")
        proxy.pause()                                   // 연결 거부 = 게이트웨이가 요청을 받지 못했다
        Thread.sleep(100)
        val id = request(account, memo())
        relay.publishPending()
        val deadline = System.currentTimeMillis() + 1_500
        while (System.currentTimeMillis() < deadline) {  // 재시도 토픽 0.5s · 1s 가 도는 동안
            assertNotEquals(TransferStatus.UNKNOWN, store.findById(id)!!.status)
            Thread.sleep(100)
        }
        proxy.resume()
        await.atMost(Duration.ofSeconds(20)) untilAsserted { assertEquals(TransferStatus.COMPLETED, store.findById(id)!!.status) }
        val ref = store.findById(id)!!.reference
        assertEquals(1, bank.depositsOf(account).count { it.reference == ref })
    }

    @Test
    fun `게이트웨이가 받고 응답 전에 끊기면 UNKNOWN, 결과조회(HTTP)로 COMPLETED - 이체 전문은 한 번`() {
        val account = verifiedBy(user, "1002123456789")
        proxy.mode = TcpCutProxy.Mode.DROP_RESPONSE
        val id = request(account, memo())
        await.atMost(Duration.ofSeconds(20)) untilAsserted {
            relay.publishPending()
            assertEquals(TransferStatus.UNKNOWN, store.findById(id)!!.status)
        }
        proxy.mode = TcpCutProxy.Mode.PASS
        TransferReconciler(store, BankInquiryAdapter(gateway), Clock.offset(Clock.systemUTC(), Duration.ofSeconds(31))).reconcile()
        val after = store.findById(id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals(1, bank.depositsOf(account).count { it.reference == after.reference })
    }
}
