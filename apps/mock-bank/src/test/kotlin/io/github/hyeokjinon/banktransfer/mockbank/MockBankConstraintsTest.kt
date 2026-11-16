package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.BankMessageSerializer
import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.PollRequest
import io.github.hyeokjinon.banktransfer.protocol.PollResponse
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import io.github.hyeokjinon.banktransfer.protocol.TransferResponse
import java.io.DataInputStream
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** 실무 제약 재현 옵션 (P8-3b 스펙 §7): 세션 하나 · 유휴 세션 정리 · 전문번호 일자별 유일. 기본은 모두 꺼짐. */
class MockBankConstraintsTest {

    /** 소켓으로 직접 붙는 최소 클라이언트 (모의 은행 모듈은 게이트웨이 모듈을 모른다 — 의존 방향). */
    private class RawClient(port: Int) : AutoCloseable {
        private val socket = Socket("127.0.0.1", port).apply { soTimeout = 3_000 }
        private val input = DataInputStream(socket.getInputStream())

        fun send(message: BankMessage) {
            val body = BankMessageSerializer.serialize(message)
            socket.getOutputStream().apply {
                write("%04d".format(body.size).toByteArray(Charsets.US_ASCII))
                write(body)
                flush()
            }
        }

        fun receive(): BankMessage {
            val length = String(ByteArray(4).also { input.readFully(it) }, Charsets.US_ASCII).toInt()
            return BankMessageSerializer.deserialize(ByteArray(length).also { input.readFully(it) })
        }

        fun poll(tranNo: Long): BankMessage {
            send(PollRequest(Header(MessageClass.ADMIN_REQUEST, BizType.POLL, tranNo, LocalDateTime.now())))
            return receive()
        }

        /** 서버가 연결을 끊었는지 (soTimeout 안에 EOF · RST 면 true, 시간이 지나도 열려 있으면 false). */
        fun closedByServer(): Boolean =
            try {
                input.read() == -1
            } catch (e: SocketTimeoutException) {
                false
            } catch (e: SocketException) {
                true
            }

        override fun close() = socket.close()
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
    }

    private fun header(tranNo: Long, bizType: BizType = BizType.TRANSFER) =
        Header(MessageClass.REQUEST, bizType, tranNo, LocalDateTime.of(2026, 11, 16, 10, 0))

    private fun transfer(tranNo: Long, reference: String) =
        TransferRequest(header(tranNo), "090", "1002123456789", 1_000, "m", reference)

    @Test
    fun `--single-session - 세션이 있으면 새 연결을 받자마자 끊고, 원래 세션은 그대로`() {
        MockBankServer(singleSession = true).start().use { bank ->
            RawClient(bank.boundPort).use { first ->
                assertIs<PollResponse>(first.poll(1))   // 첫 세션이 확실히 받아들여진 뒤
                RawClient(bank.boundPort).use { second -> assertTrue(second.closedByServer()) }
                assertIs<PollResponse>(first.poll(2))
            }
            assertEquals(1, bank.peakSessions())
            assertEquals(1, bank.sessionsOpened)
            // 첫 세션이 닫히면 다시 받는다 (서버가 닫힘을 알아채는 데 잠깐 걸린다)
            val deadline = System.currentTimeMillis() + 3_000
            while (true) {
                val ok = RawClient(bank.boundPort).use { runCatching { it.poll(3) }.isSuccess }
                if (ok) break
                check(System.currentTimeMillis() < deadline) { "세션이 닫힌 뒤에도 새 연결을 받지 않는다" }
                Thread.sleep(50)
            }
        }
    }

    @Test
    fun `--session-idle-timeout - 받은 전문이 없으면 끊고, 폴링이 오면 유지`() {
        MockBankServer(sessionIdleTimeout = Duration.ofMillis(400)).start().use { bank ->
            RawClient(bank.boundPort).use { busy ->
                repeat(6) { i ->
                    assertIs<PollResponse>(busy.poll(i + 1L))
                    Thread.sleep(150)
                }
            }
            RawClient(bank.boundPort).use { idle -> assertTrue(idle.closedByServer()) }
        }
    }

    @Test
    fun `--unique-tran-no - 같은 KST 날짜에 같은 전문번호는 처리하지 않고 3001, 날짜가 바뀌면 다시 받는다`() {
        val clock = MutableClock(Instant.parse("2026-11-16T01:00:00Z"))   // KST 10:00
        MockBankServer(uniqueTranNo = true, clock = clock).use { bank ->
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116UNIQUE000001")) as TransferResponse).header.responseCode)

            val dup = bank.respond(transfer(7, "T261116UNIQUE000002")) as TransferResponse
            assertEquals(ResponseCodes.DUPLICATE_TRAN_NO, dup.header.responseCode)
            assertEquals(1, bank.depositsOf("1002123456789").size)   // 두 번째는 입금하지 않았다
            assertEquals(
                ResponseCodes.DUPLICATE_TRAN_NO,
                bank.respond(NameInquiryRequest(header(7, BizType.NAME_INQUIRY), "090", "1002123456789"))!!.header.responseCode,
            )

            clock.now = Instant.parse("2026-11-16T15:00:00Z")   // KST 다음 날 00:00
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261117UNIQUE000003")) as TransferResponse).header.responseCode)
        }
    }

    @Test
    fun `옵션을 끄면 지금과 같다 - 같은 번호도 처리, 세션 여러 개`() {
        MockBankServer().start().use { bank ->
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116DEFAULT00001")) as TransferResponse).header.responseCode)
            assertEquals(ResponseCodes.OK, (bank.respond(transfer(7, "T261116DEFAULT00002")) as TransferResponse).header.responseCode)
            RawClient(bank.boundPort).use { a ->
                RawClient(bank.boundPort).use { b ->
                    assertIs<PollResponse>(a.poll(1))
                    assertIs<PollResponse>(b.poll(1))
                }
            }
            assertEquals(2, bank.peakSessions())
        }
    }
}
