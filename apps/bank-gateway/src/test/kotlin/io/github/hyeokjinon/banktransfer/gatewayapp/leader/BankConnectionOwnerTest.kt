package io.github.hyeokjinon.banktransfer.gatewayapp.leader

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankNotSentException
import io.github.hyeokjinon.banktransfer.gateway.InMemoryTranNoSource
import io.github.hyeokjinon.banktransfer.gateway.NotSentReason
import io.github.hyeokjinon.banktransfer.gateway.TranNoSource
import io.github.hyeokjinon.banktransfer.gatewayapp.health.BankConnectionHealthIndicator
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.springframework.boot.health.contributor.Status
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** 지금 은행 연결의 단일 진실 (P8-3b 결정 4 · 18): 임기마다 새 클라이언트, 물러나면 즉시 close, 리더가 아니면 "안 보냄". */
class BankConnectionOwnerTest {
    private val bank = MockBankServer(singleSession = true).start()
    private val prepared = AtomicInteger()
    private val tranNo = object : TranNoSource {
        val delegate = InMemoryTranNoSource()
        override fun next() = delegate.next()
        override fun prepare() { prepared.incrementAndGet() }
    }
    private val owner = BankConnectionOwner(tranNo) {
        BankGatewayClient(
            BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
            tranNoSource = tranNo,
        )
    }

    @AfterTest
    fun tearDown() {
        owner.close()
        bank.close()
    }

    private fun failureOf(block: () -> java.util.concurrent.CompletableFuture<*>): Throwable =
        runCatching { block().get(3, TimeUnit.SECONDS) }.exceptionOrNull().let { (it as? ExecutionException)?.cause ?: it!! }

    @Test
    fun `리더가 아니면 연결 없음 - 요청은 BankNotSentException, 헬스는 UP(standby)`() {
        assertFalse(owner.isLeader)
        assertNull(owner.current())
        assertEquals(NotSentReason.NOT_LEADER, assertIs<BankNotSentException>(failureOf { owner.inquireName("090", "1002123456789") }).reason)

        val health = BankConnectionHealthIndicator(owner).health()
        assertEquals(Status.UP, health.status)
        assertEquals("standby", health.details["role"])
        assertEquals(0, bank.sessionsOpened)
    }

    @Test
    fun `임기를 시작하면 번호 구간을 준비하고 접속, 헬스는 연결을 따라간다`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }

        assertEquals(1, prepared.get())
        assertEquals("홍길동", owner.inquireName("090", "1002123456789").get(3, TimeUnit.SECONDS).holderName)
        val up = BankConnectionHealthIndicator(owner).health()
        assertEquals(Status.UP, up.status)
        assertEquals("leader", up.details["role"])

        bank.pause()
        await.atMost(Duration.ofSeconds(5)) untilAsserted {
            val down = BankConnectionHealthIndicator(owner).health()
            assertEquals(Status.DOWN, down.status)
            assertEquals("leader", down.details["role"])
        }
    }

    @Test
    fun `물러나면 즉시 연결을 닫고, 다음 임기는 새 클라이언트`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        val first = owner.current()

        owner.stopLeading()
        assertNull(owner.current())
        assertFalse(first!!.isConnected)

        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        assertTrue(owner.current() !== first)
        assertEquals(2, prepared.get())
        assertEquals(1, bank.peakSessions())   // 은행 쪽에서도 동시에 둘이 아니었다
    }

    @Test
    fun `정상 교대에 들어가면 새 요청은 STEPPING_DOWN 사유로 안 보냄`() {
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }

        owner.stepDown(Duration.ofSeconds(1))

        assertEquals(NotSentReason.STEPPING_DOWN, assertIs<BankNotSentException>(failureOf { owner.inquire("T261117STEPDOWN0001") }).reason)
    }

    @Test
    fun `stepDown - 새 요청 거절, 처리 중 전문은 비운 뒤 끊고, 다시 리더가 되지 않는다`() {
        bank.responseDelay = Duration.ofMillis(800)
        owner.startLeading()
        await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(owner.current()!!.isConnected) }
        val inflight = owner.transfer("090", "1002123456789", 1_000, "m", "T261116STEPDOWN0001")

        val stepping = Thread { owner.stepDown(Duration.ofSeconds(5)) }.apply { start() }
        await.atMost(Duration.ofSeconds(2)) untilAsserted {
            assertIs<BankNotSentException>(failureOf { owner.inquireName("090", "1002123456789") })
        }
        stepping.join(5_000)

        assertEquals(ResponseCodes.OK, inflight.get(1, TimeUnit.SECONDS).header.responseCode)
        assertNull(owner.current())
        owner.startLeading()
        assertNull(owner.current())   // 종료 중에 늦게 온 "리더 됨" 콜백은 무시
    }

    @Test
    fun `stepDown 은 거절 확인을 통과한 요청이 대기 표에 오를 때까지 기다린다 - 끊어서 결과 모름을 만들지 않는다`() {
        val armed = java.util.concurrent.atomic.AtomicBoolean(false)
        val entered = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val gated = object : TranNoSource {
            override fun next(): Long {
                if (armed.get() && Thread.currentThread().name == "gated-transfer") {
                    entered.countDown()
                    release.await(5, TimeUnit.SECONDS)   // 거절 확인은 지났고 아직 대기 표에는 오르기 전
                }
                return tranNo.next()
            }
            override fun prepare() = tranNo.prepare()
        }
        val gatedOwner = BankConnectionOwner(gated) {
            BankGatewayClient(
                BankGatewayConfig("127.0.0.1", bank.boundPort, reconnectMinDelay = Duration.ofMillis(100), reconnectMaxDelay = Duration.ofMillis(300)),
                tranNoSource = gated,
            )
        }
        try {
            gatedOwner.startLeading()
            await.atMost(Duration.ofSeconds(5)) untilAsserted { assertTrue(gatedOwner.current()!!.isConnected) }
            armed.set(true)

            var transfer: java.util.concurrent.CompletableFuture<*>? = null
            val sender = Thread({ transfer = gatedOwner.transfer("090", "1002123456789", 1_000, "m", "T261116INFLIGHT00001") }, "gated-transfer").apply { start() }
            assertTrue(entered.await(3, TimeUnit.SECONDS))

            val stepping = Thread { gatedOwner.stepDown(Duration.ofSeconds(2)) }.apply { start() }
            Thread.sleep(300)
            assertTrue(stepping.isAlive)               // 처리 중인 요청이 있으니 아직 끊지 않았다
            assertTrue(gatedOwner.current() != null)

            release.countDown()
            sender.join(3_000)
            val response = transfer!!.get(3, TimeUnit.SECONDS) as io.github.hyeokjinon.banktransfer.protocol.TransferResponse
            assertEquals(ResponseCodes.OK, response.header.responseCode)
            stepping.join(3_000)
            assertFalse(stepping.isAlive)
            assertNull(gatedOwner.current())
        } finally {
            release.countDown()
            gatedOwner.close()
        }
    }
}
