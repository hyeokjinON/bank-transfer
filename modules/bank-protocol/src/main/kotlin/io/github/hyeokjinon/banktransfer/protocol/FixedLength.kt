package io.github.hyeokjinon.banktransfer.protocol

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

object BankCharsets {
    /** 국내 은행 전문은 대부분 EUC-KR 이다. 한글 1자 = 2바이트. */
    val EUC_KR: Charset = Charset.forName("EUC-KR")
}

private const val SPACE = 0x20

/**
 * 고정길이 전문 쓰기.
 * - 문자: 왼쪽 정렬, 남는 자리는 공백(0x20). 길이는 "글자 수"가 아니라 "바이트 수"다.
 * - 숫자: 오른쪽 정렬, 남는 자리는 0.
 */
class FixedLengthWriter(private val charset: Charset = BankCharsets.EUC_KR) {
    private val out = ByteArrayOutputStream()

    fun text(value: String, length: Int): FixedLengthWriter {
        val bytes = fitToBytes(value, length)
        out.write(bytes)
        repeat(length - bytes.size) { out.write(SPACE) }
        return this
    }

    fun number(value: Long, length: Int): FixedLengthWriter {
        require(value >= 0) { "negative number is not allowed: $value" }
        val digits = value.toString()
        if (digits.length > length) throw ProtocolException("number $value does not fit in $length digits")
        out.write(digits.padStart(length, '0').toByteArray(Charsets.US_ASCII))
        return this
    }

    fun toByteArray(): ByteArray = out.toByteArray()

    /** 바이트 길이를 넘으면 글자 단위로 자른다. 한글 2바이트의 절반만 남는 깨진 글자를 만들지 않기 위해서다. */
    private fun fitToBytes(value: String, length: Int): ByteArray {
        val encoded = value.toByteArray(charset)
        if (encoded.size <= length) return encoded
        var end = value.length
        while (end > 0) {
            val candidate = value.substring(0, end).toByteArray(charset)
            if (candidate.size <= length) return candidate
            end--
        }
        return ByteArray(0)
    }
}

/** 고정길이 전문 읽기. 앞에서부터 정해진 바이트 수만큼 잘라 읽는다. */
class FixedLengthReader(private val bytes: ByteArray, private val charset: Charset = BankCharsets.EUC_KR) {
    private var position = 0

    val remaining: Int get() = bytes.size - position

    fun text(length: Int): String = String(take(length), charset).trimEnd(' ')

    fun number(length: Int): Long {
        val raw = String(take(length), Charsets.US_ASCII)
        return raw.trim().toLongOrNull()
            ?: throw ProtocolException("not a number: '$raw' at offset ${position - length}")
    }

    private fun take(length: Int): ByteArray {
        if (position + length > bytes.size) {
            throw ProtocolException("message too short: need ${position + length} bytes, got ${bytes.size}")
        }
        return bytes.copyOfRange(position, position + length).also { position += length }
    }
}
