package io.github.hyeokjinon.banktransfer.protocol.netty

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.CorruptedFrameException
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import java.nio.ByteOrder

/**
 * TCP 는 메시지 경계가 없는 바이트 스트림이다. 한 번의 read 에 전문이 반쯤 오거나 두 개가 붙어 올 수 있다.
 * 앞 4바이트 길이 필드로 프레임을 자르는데, 은행 전문은 길이를 바이너리 int 가 아니라
 * ASCII 숫자("0052")로 보낸다. 그래서 Netty 의 LengthFieldBasedFrameDecoder 를 상속해
 * 길이 해석 부분만 바꾼다. 부분 수신, 누적, 최대 길이 검사는 부모 클래스가 처리한다.
 *
 * 결과 프레임은 길이 필드를 뺀 본문(initialBytesToStrip = 4)이다.
 */
class AsciiLengthFieldFrameDecoder(maxBodyLength: Int = MAX_BODY_LENGTH) :
    LengthFieldBasedFrameDecoder(maxBodyLength + LENGTH_FIELD_SIZE, 0, LENGTH_FIELD_SIZE, 0, LENGTH_FIELD_SIZE) {

    override fun getUnadjustedFrameLength(buf: ByteBuf, offset: Int, length: Int, order: ByteOrder): Long {
        val text = buf.toString(offset, length, Charsets.US_ASCII)
        if (text.isEmpty() || !text.all { it in '0'..'9' }) {
            // 길이를 못 읽으면 이후 스트림 전체가 어긋난다. 복구하지 말고 연결을 끊어야 한다.
            throw CorruptedFrameException("length field is not numeric: $text")
        }
        return text.toLong()
    }

    companion object {
        const val LENGTH_FIELD_SIZE = 4
        const val MAX_BODY_LENGTH = 9999
    }
}
