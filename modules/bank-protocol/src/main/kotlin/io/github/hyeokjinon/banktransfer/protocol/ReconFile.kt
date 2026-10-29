package io.github.hyeokjinon.banktransfer.protocol

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.TemporalAccessor

/**
 * 대사 파일 상세 한 건 = 은행이 실제로 처리(입금)한 이체 하나 (규격 v0.3 "대사 파일", P5-1).
 * 계좌가 없어 거절한 건은 돈이 움직이지 않았으므로 상세가 없다. 1원인증 송금(V…)도 처리했으면 들어간다.
 */
data class ReconDetail(
    val reference: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    /** 은행 처리 시각 (KST). 처리일은 파일 헤더 · 거래 기록 파일 이름의 날짜다. */
    val processedAt: LocalTime,
) {
    /** 로그에 계좌번호가 그대로 남지 않게 한다 (data class 기본 toString 은 모든 필드를 찍는다). */
    override fun toString(): String =
        "ReconDetail(reference=$reference, bankCode=$bankCode, accountNo=***${accountNo.takeLast(4)}, amount=$amount, processedAt=$processedAt)"
}

/** 읽은 상세와 파일 안의 물리 줄 번호 (헤더 = 1, 첫 상세 = 2). 운영자가 파일에서 바로 찾을 수 있게 그대로 쓴다. */
data class ReconLine(val lineNo: Int, val detail: ReconDetail)

/** 검증을 통과한 대사 파일 하나. */
data class ReconFile(val date: LocalDate, val bankCode: String, val lines: List<ReconLine>)

/**
 * 대사 파일 형식 (규격 v0.3). 필드 규칙은 전문과 같다 — AN 왼쪽 정렬 · 공백, N 오른쪽 정렬 · 0, EUC-KR, 길이는 바이트.
 * 한 줄 = 한 레코드 + `\n`. 레코드 길이는 구분자 1자리를 포함해 모두 [RECORD_LENGTH] 바이트로 공백을 채운다.
 *
 * - 헤더 `H`: 구분(1) · 처리일 yyyyMMdd(8) · 은행코드(3)
 * - 상세 `D`: 구분(1) · 이용기관 거래번호 AN(20) · 은행코드 AN(3) · 계좌번호 AN(16) · 금액 N(13) · 처리시각 HHmmss(6)
 * - 트레일러 `T`: 구분(1) · 상세 건수 N(9) · 금액 합계 N(18)
 *
 * 모의 은행(일자별 거래 기록 · 일마감 쓰기)과 API 의 일마감 대사(읽기 · 검증)가 이 코드 하나를 쓴다.
 * 마지막 레코드 뒤 줄바꿈이 없으면 거부한다 — 쓰다 만 파일을 받아들이지 않게 (모의 은행은 임시 파일 + 이름 바꾸기로 쓴다).
 */
object ReconFileFormat {
    /** 가장 긴 상세 레코드 = 1 + 20 + 3 + 16 + 13 + 6. 헤더 · 트레일러도 이 길이로 채운다. */
    const val RECORD_LENGTH = 59

    private const val NEWLINE: Byte = 0x0A
    private const val SPACE: Byte = 0x20
    private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    private val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

    /** 은행이 날짜마다 만드는 대사 파일 이름. */
    fun fileName(date: LocalDate): String = "RECON_${DATE.format(date)}.dat"

    /** 모의 은행의 일자별 거래 기록 파일 이름 (처리할 때마다 상세 레코드 한 줄을 덧붙인다). */
    fun journalFileName(date: LocalDate): String = "${DATE.format(date)}.log"

    /** 상세 레코드 한 줄 (줄바꿈 제외, 정확히 [RECORD_LENGTH] 바이트). */
    fun detailRecord(detail: ReconDetail): ByteArray =
        FixedLengthWriter()
            .text("D", 1)
            .text(detail.reference, 20)
            .text(detail.bankCode, 3)
            .text(detail.accountNo, 16)
            .number(detail.amount, 13)
            .text(TIME.format(detail.processedAt), 6)
            .toByteArray()

    /** 거래 기록 파일에 덧붙일 한 줄 (상세 레코드 + 줄바꿈). */
    fun journalLine(detail: ReconDetail): ByteArray = detailRecord(detail) + NEWLINE

    /** 거래 기록 파일 전체를 읽는다. 빈 파일은 0건. 줄이 깨졌으면 [ProtocolException] (줄 번호 포함). */
    fun parseJournal(bytes: ByteArray): List<ReconDetail> =
        if (bytes.isEmpty()) emptyList() else splitRecords(bytes).mapIndexed { i, record -> withLine(i + 1) { parseDetail(record) } }

    /** 상세 레코드 한 줄(줄바꿈 제외)을 읽는다. 길이 · 구분 · 숫자 · 시각이 틀리면 [ProtocolException]. */
    fun parseDetail(record: ByteArray): ReconDetail {
        requireLength(record)
        val reader = FixedLengthReader(record)
        val type = reader.text(1)
        if (type != "D") throw ProtocolException("not a detail record: '$type'")
        return ReconDetail(
            reference = reader.text(20),
            bankCode = reader.text(3),
            accountNo = reader.text(16),
            amount = reader.number(13),
            processedAt = parse(reader.text(6), TIME, LocalTime::from),
        )
    }

    /** 헤더 + 상세 + 트레일러. 처리한 이체가 없는 날도 헤더 + 0건 트레일러인 파일이다. */
    fun write(date: LocalDate, bankCode: String, details: List<ReconDetail>): ByteArray {
        val out = ByteArrayOutputStream()
        fun line(record: ByteArray) {
            out.write(record)
            out.write(NEWLINE.toInt())
        }
        line(padded(FixedLengthWriter().text("H", 1).text(DATE.format(date), 8).text(bankCode, 3)))
        details.forEach { line(detailRecord(it)) }
        line(padded(FixedLengthWriter().text("T", 1).number(details.size.toLong(), 9).number(details.sumOf { it.amount }, 18)))
        return out.toByteArray()
    }

    /**
     * 파일을 읽고 검증한다: 모든 줄 [RECORD_LENGTH] 바이트, 첫 줄 헤더 · 마지막 줄 트레일러, 헤더 날짜 = [expectedDate],
     * 트레일러 건수 · 합계 = 상세. 하나라도 틀리면 [ProtocolException] — 대사는 이 파일을 믿지 않는다.
     */
    fun read(bytes: ByteArray, expectedDate: LocalDate): ReconFile {
        val records = splitRecords(bytes)
        if (records.size < 2) throw ProtocolException("recon file needs a header and a trailer, got ${records.size} record(s)")
        val (date, bankCode) = withLine(1) { header(records.first()) }
        if (date != expectedDate) throw ProtocolException("header date $date is not the expected $expectedDate")
        val lines = records.subList(1, records.size - 1).mapIndexed { i, record ->
            ReconLine(i + 2, withLine(i + 2) { parseDetail(record) })
        }
        val (count, sum) = withLine(records.size) { trailer(records.last()) }
        if (count != lines.size.toLong()) throw ProtocolException("trailer count $count does not match ${lines.size} detail record(s)")
        val actual = lines.sumOf { it.detail.amount }
        if (sum != actual) throw ProtocolException("trailer sum $sum does not match the details sum $actual")
        return ReconFile(date, bankCode, lines)
    }

    private fun header(record: ByteArray): Pair<LocalDate, String> {
        requireLength(record)
        val reader = FixedLengthReader(record)
        if (reader.text(1) != "H") throw ProtocolException("first record must be the header (H)")
        return parse(reader.text(8), DATE, LocalDate::from) to reader.text(3)
    }

    private fun trailer(record: ByteArray): Pair<Long, Long> {
        requireLength(record)
        val reader = FixedLengthReader(record)
        if (reader.text(1) != "T") throw ProtocolException("last record must be the trailer (T)")
        return reader.number(9) to reader.number(18)
    }

    /** `\n` 으로 끝나는 레코드들로 나눈다. EUC-KR 의 2바이트 글자는 0x81 이상이라 0x0A 와 겹치지 않는다. */
    private fun splitRecords(bytes: ByteArray): List<ByteArray> {
        if (bytes.isEmpty()) throw ProtocolException("recon file is empty")
        if (bytes.last() != NEWLINE) throw ProtocolException("file must end with a newline (partially written?)")
        val records = mutableListOf<ByteArray>()
        var start = 0
        for (i in bytes.indices) {
            if (bytes[i] == NEWLINE) {
                records += bytes.copyOfRange(start, i)
                start = i + 1
            }
        }
        return records
    }

    private fun requireLength(record: ByteArray) {
        if (record.size != RECORD_LENGTH) throw ProtocolException("record length ${record.size}, expected $RECORD_LENGTH")
    }

    private fun padded(writer: FixedLengthWriter): ByteArray {
        val bytes = writer.toByteArray()
        return bytes + ByteArray(RECORD_LENGTH - bytes.size) { SPACE }
    }

    private fun <T> parse(text: String, format: DateTimeFormatter, query: (TemporalAccessor) -> T): T =
        try {
            query(format.parse(text))
        } catch (e: DateTimeParseException) {
            throw ProtocolException("not a valid date/time: '$text'", e)
        }

    private inline fun <T> withLine(lineNo: Int, block: () -> T): T =
        try {
            block()
        } catch (e: ProtocolException) {
            throw ProtocolException("line $lineNo: ${e.message}", e)
        }
}
