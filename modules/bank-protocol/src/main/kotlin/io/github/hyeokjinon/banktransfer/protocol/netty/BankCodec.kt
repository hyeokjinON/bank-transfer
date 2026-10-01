package io.github.hyeokjinon.banktransfer.protocol.netty

import io.github.hyeokjinon.banktransfer.protocol.BankMessage
import io.github.hyeokjinon.banktransfer.protocol.BankMessageSerializer
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.EncoderException
import io.netty.handler.codec.MessageToMessageCodec
import io.netty.handler.codec.MessageToMessageEncoder

/** 보낼 본문 앞에 4자리 ASCII 길이("0052")를 붙인다. 상태가 없으므로 여러 채널이 공유해도 된다. */
@Sharable
class AsciiLengthFieldPrepender : MessageToMessageEncoder<ByteBuf>() {
    override fun encode(ctx: ChannelHandlerContext, msg: ByteBuf, out: MutableList<Any>) {
        val length = msg.readableBytes()
        if (length > AsciiLengthFieldFrameDecoder.MAX_BODY_LENGTH) throw EncoderException("frame too long: $length")
        val lengthField = "%04d".format(length).toByteArray(Charsets.US_ASCII)
        out.add(ctx.alloc().buffer(lengthField.size).writeBytes(lengthField))
        out.add(msg.retain()) // 부모가 msg 를 release 하므로, 다음 핸들러로 넘길 참조를 하나 늘린다
    }
}

/** 프레임(ByteBuf) <-> BankMessage. 비즈니스 핸들러는 바이트를 몰라도 된다. */
@Sharable
class BankMessageCodec : MessageToMessageCodec<ByteBuf, BankMessage>() {
    override fun encode(ctx: ChannelHandlerContext, msg: BankMessage, out: MutableList<Any>) {
        out.add(Unpooled.wrappedBuffer(BankMessageSerializer.serialize(msg)))
    }

    override fun decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: MutableList<Any>) {
        out.add(BankMessageSerializer.deserialize(ByteBufUtil.getBytes(msg)))
    }
}

private val SHARED_PREPENDER = AsciiLengthFieldPrepender()
private val SHARED_CODEC = BankMessageCodec()

/**
 * 은행 전문 처리용 핸들러를 순서대로 붙인다.
 * - 인바운드: 바이트 -> 프레임 디코더 -> 전문 디코드 -> 비즈니스 핸들러
 * - 아웃바운드: 비즈니스 핸들러 -> 전문 인코드 -> 길이 붙이기 -> 바이트
 *
 * 프레임 디코더는 채널마다 누적 버퍼를 가지므로 매번 새로 만들고, 나머지는 상태가 없어 공유한다.
 */
fun ChannelPipeline.addBankCodec(): ChannelPipeline =
    addLast("frameDecoder", AsciiLengthFieldFrameDecoder())
        .addLast("lengthPrepender", SHARED_PREPENDER)
        .addLast("messageCodec", SHARED_CODEC)
