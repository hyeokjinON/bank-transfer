package io.github.hyeokjinon.banktransfer.protocol.netty

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.NameInquiryRequest
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.DecoderException
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** EmbeddedChannel: 소켓 없이 파이프라인만 돌려 보는 Netty 테스트 도구. */
class BankCodecTest {
    private val message = NameInquiryRequest(
        Header(MessageClass.REQUEST, BizType.NAME_INQUIRY, 1, LocalDateTime.of(2026, 10, 1, 9, 0, 0)),
        "090",
        "1002123456789",
    )

    private fun newChannel() = EmbeddedChannel().also { it.pipeline().addBankCodec() }

    private fun encode(msg: BankMessage): ByteArray {
        val ch = newChannel()
        ch.writeOutbound(msg)
        val out = Unpooled.buffer()
        while (true) {
            val part = ch.readOutbound<ByteBuf>() ?: break
            out.writeBytes(part)
            part.release()
        }
        return ByteArray(out.readableBytes()).also { out.readBytes(it) }
    }

    @Test
    fun `보낼 때 앞에 4자리 ASCII 길이를 붙인다`() {
        val bytes = encode(message)

        val length = String(bytes, 0, 4, Charsets.US_ASCII)
        assertEquals("%04d".format(bytes.size - 4), length)
    }

    @Test
    fun `전문이 쪼개져 도착해도 다 모일 때까지 기다렸다가 하나로 읽는다`() {
        val bytes = encode(message)
        val ch = newChannel()

        ch.writeInbound(Unpooled.wrappedBuffer(bytes, 0, 10))
        assertNull(ch.readInbound<BankMessage>(), "아직 덜 왔으므로 아무것도 나오면 안 된다")

        ch.writeInbound(Unpooled.wrappedBuffer(bytes, 10, bytes.size - 10))
        assertEquals(message, ch.readInbound())
    }

    @Test
    fun `두 전문이 붙어서 와도 각각 읽는다`() {
        val bytes = encode(message)
        val second = encode(message.copy(header = message.header.copy(tranNo = 2)))
        val ch = newChannel()

        ch.writeInbound(Unpooled.wrappedBuffer(bytes + second))

        assertEquals(1L, ch.readInbound<BankMessage>().header.tranNo)
        assertEquals(2L, ch.readInbound<BankMessage>().header.tranNo)
    }

    @Test
    fun `길이 필드가 숫자가 아니면 예외로 연결을 끊게 한다`() {
        val ch = newChannel()
        assertFailsWith<DecoderException> {
            ch.writeInbound(Unpooled.wrappedBuffer("AB12xxxxxxxxxxxx".toByteArray()))
        }
    }
}
