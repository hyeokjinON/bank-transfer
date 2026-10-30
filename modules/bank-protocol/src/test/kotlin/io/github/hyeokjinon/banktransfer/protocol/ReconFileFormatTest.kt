package io.github.hyeokjinon.banktransfer.protocol

import java.time.LocalDate
import java.time.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 대사 파일 형식 v0.3 (P5-1). 모의 은행이 쓰고 API 의 일마감 대사가 읽는다 — 같은 코드라 왕복이 맞아야 한다. */
class ReconFileFormatTest {

    private val day = LocalDate.of(2026, 10, 28)
    private val paid = ReconDetail("T261028ABCDEFGHIJKLM", "090", "1002123456789", 30_000, LocalTime.of(10, 15, 30))
    private val verification = ReconDetail("V261028NOPQRSTUVWXYZ", "090", "3333012345678", 1, LocalTime.of(23, 49, 59))

    private fun text(bytes: ByteArray) = String(bytes, BankCharsets.EUC_KR)

    /** ASCII 레코드들을 59자로 채우고 줄바꿈을 붙여 파일 바이트로 (잘못된 파일을 손으로 만들 때). */
    private fun fileOf(vararg records: String): ByteArray =
        records.joinToString("") { it.padEnd(ReconFileFormat.RECORD_LENGTH) + "\n" }.toByteArray(BankCharsets.EUC_KR)

    private fun detailText(detail: ReconDetail) = text(ReconFileFormat.detailRecord(detail))

    private fun readFails(bytes: ByteArray): String =
        assertFailsWith<ProtocolException> { ReconFileFormat.read(bytes, day) }.message!!

    @Test
    fun `쓰기 → 읽기 왕복 - 레코드는 모두 59바이트 + 줄바꿈, 줄 번호는 헤더가 1`() {
        val bytes = ReconFileFormat.write(day, "090", listOf(paid, verification))

        val lines = text(bytes).split("\n")
        assertEquals(listOf(59, 59, 59, 59, 0), lines.map { it.toByteArray(BankCharsets.EUC_KR).size })   // 마지막 레코드 뒤에도 줄바꿈
        assertEquals("H20261028090", lines[0].trimEnd())
        assertEquals("DT261028ABCDEFGHIJKLM0901002123456789   0000000030000101530", lines[1])
        assertEquals("T000000002000000000000030001", lines[3].trimEnd())

        val file = ReconFileFormat.read(bytes, day)
        assertEquals(day, file.date)
        assertEquals("090", file.bankCode)
        assertEquals(listOf(ReconLine(2, paid), ReconLine(3, verification)), file.lines)
    }

    @Test
    fun `처리한 이체가 없는 날도 헤더 + 0건 트레일러`() {
        val bytes = ReconFileFormat.write(day, "090", emptyList())

        assertEquals(2, text(bytes).count { it == '\n' })
        assertEquals("T000000000000000000000000000", text(bytes).split("\n")[1].trimEnd())
        assertTrue(ReconFileFormat.read(bytes, day).lines.isEmpty())
    }

    @Test
    fun `트레일러 건수가 상세와 다르면 거부`() {
        val message = readFails(fileOf("H20261028090", detailText(paid), "T000000002000000000000030000"))

        assertTrue("trailer count 2" in message, message)
    }

    @Test
    fun `트레일러 합계가 상세와 다르면 거부`() {
        val message = readFails(fileOf("H20261028090", detailText(paid), "T000000001000000000000029999"))

        assertTrue("trailer sum 29999" in message, message)
    }

    @Test
    fun `헤더 날짜가 대사일과 다르면 거부`() {
        val message = readFails(ReconFileFormat.write(day.minusDays(1), "090", listOf(paid)))

        assertTrue("header date 2026-10-27" in message, message)
    }

    @Test
    fun `길이가 틀린 줄은 줄 번호와 함께 거부 - CRLF 도 길이가 60 이라 거부`() {
        val short = text(ReconFileFormat.write(day, "090", listOf(paid))).replaceFirst("101530\n", "10153\n")
        assertTrue("line 2: record length 58" in readFails(short.toByteArray(BankCharsets.EUC_KR)))

        val crlf = text(ReconFileFormat.write(day, "090", listOf(paid))).replace("\n", "\r\n")
        assertTrue("line 1: record length 60" in readFails(crlf.toByteArray(BankCharsets.EUC_KR)))
    }

    @Test
    fun `마지막 줄바꿈이 없거나(반쯤 쓴 파일) 비었거나 트레일러가 없으면 거부`() {
        val full = ReconFileFormat.write(day, "090", listOf(paid))
        assertTrue("must end with a newline" in readFails(full.copyOf(full.size - 1)))
        assertTrue("empty" in readFails(ByteArray(0)))
        assertTrue("line 2: last record must be the trailer" in readFails(fileOf("H20261028090", detailText(paid))))
        assertTrue("line 2: not a detail record" in readFails(fileOf("H20261028090", "T000000000000000000000000000", "T000000000000000000000000000")))
    }

    @Test
    fun `없는 날짜 · 시각은 거부 (엄격 해석) - 20260230 을 2월 28일로, 240000 을 자정으로 고쳐 읽지 않는다`() {
        val feb28 = LocalDate.of(2026, 2, 28)
        val badDate = assertFailsWith<ProtocolException> { ReconFileFormat.read(fileOf("H20260230090", "T000000000000000000000000000"), feb28) }.message!!
        assertTrue("line 1: not a valid date/time: '20260230'" in badDate, badDate)

        val badTime = detailText(paid).replace("101530", "240000")
        assertTrue("line 2: not a valid date/time: '240000'" in readFails(fileOf("H20261028090", badTime, "T000000001000000000000030000")))
        assertFailsWith<ProtocolException> { ReconFileFormat.parseJournal((detailText(paid).replace("101530", "126000") + "\n").toByteArray(BankCharsets.EUC_KR)) }
    }

    @Test
    fun `숫자 칸에 숫자가 아니면 거부`() {
        val broken = detailText(paid).replace("0000000030000", "00000000300X0")

        assertTrue("line 2: not a number" in readFails(fileOf("H20261028090", broken, "T000000001000000000000030000")))
    }

    @Test
    fun `거래 기록 줄 왕복 - 상세 레코드 + 줄바꿈`() {
        val journal = ReconFileFormat.journalLine(paid) + ReconFileFormat.journalLine(verification)

        assertEquals(listOf(paid, verification), ReconFileFormat.parseJournal(journal))
        assertEquals(emptyList(), ReconFileFormat.parseJournal(ByteArray(0)))
    }

    @Test
    fun `파일 이름 - 대사 파일 RECON_yyyyMMdd_dat, 거래 기록 yyyyMMdd_log`() {
        assertEquals("RECON_20261028.dat", ReconFileFormat.fileName(day))
        assertEquals("20261028.log", ReconFileFormat.journalFileName(day))
    }
}
