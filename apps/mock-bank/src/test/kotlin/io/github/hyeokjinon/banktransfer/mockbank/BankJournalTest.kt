package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.ReconFileFormat
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** 모의 은행의 일자별 거래 기록 · 일마감 (P5-1). 시계는 UTC 로 주고 날짜는 KST 로 정해지는지 본다. */
class BankJournalTest {

    @TempDir
    lateinit var dir: Path

    private val day = LocalDate.of(2026, 10, 28)

    private fun transfer(reference: String, amount: Long = 30_000) =
        TransferRequest(Header(MessageClass.REQUEST, BizType.TRANSFER, 1, LocalDateTime.of(2026, 10, 28, 10, 0)), "090", "1002123456789", amount, "memo", reference)

    private fun journalAt(utc: String) = BankJournal(dir, Clock.fixed(Instant.parse(utc), ZoneOffset.UTC))

    @Test
    fun `처리할 때마다 그날(KST) 기록에 한 줄, 마감하면 헤더 + 상세 + 트레일러`() {
        val journal = journalAt("2026-10-28T14:59:59Z")   // KST 23:59:59
        journal.record(transfer("T261028AAAAAAAAAAAAA"))
        journal.record(transfer("V261028BBBBBBBBBBBBB", amount = 1))

        val file = journal.closeDay(day)

        assertEquals(dir.resolve("recon").resolve("RECON_20261028.dat"), file)
        val read = ReconFileFormat.read(Files.readAllBytes(file), day)
        assertEquals("090", read.bankCode)
        assertEquals(listOf("T261028AAAAAAAAAAAAA", "V261028BBBBBBBBBBBBB"), read.lines.map { it.detail.reference })
        assertEquals(LocalTime.of(23, 59, 59), read.lines.first().detail.processedAt)
    }

    @Test
    fun `날짜는 처리 시각의 KST 날짜 - UTC 15시는 다음 날 기록`() {
        journalAt("2026-10-28T15:00:00Z").record(transfer("T261029AAAAAAAAAAAAA"))

        assertTrue(Files.exists(dir.resolve("journal").resolve("20261029.log")))
    }

    @Test
    fun `이미 만든 대사 파일은 덮어쓰지 않는다 - 그 뒤 기록이 늘어도`() {
        val journal = journalAt("2026-10-28T01:00:00Z")
        journal.record(transfer("T261028AAAAAAAAAAAAA"))
        val first = Files.readAllBytes(journal.closeDay(day))

        journal.record(transfer("T261028CCCCCCCCCCCCC"))
        val again = Files.readAllBytes(journal.closeDay(day))

        assertTrue(first.contentEquals(again))
        assertEquals(1, ReconFileFormat.read(again, day).lines.size)
    }

    @Test
    fun `기록이 없는 날도 헤더 + 0건 트레일러 파일을 만들고 임시 파일은 남지 않는다`() {
        val file = journalAt("2026-10-28T01:00:00Z").closeDay(day)

        assertTrue(ReconFileFormat.read(Files.readAllBytes(file), day).lines.isEmpty())
        assertEquals(listOf("RECON_20261028.dat"), dir.resolve("recon").listDirectoryEntries().map { it.name })
    }

    @Test
    fun `처리한 거래번호는 모든 날짜의 기록에서 되살린다`() {
        journalAt("2026-10-27T01:00:00Z").record(transfer("T261027AAAAAAAAAAAAA"))
        journalAt("2026-10-28T01:00:00Z").record(transfer("T261028AAAAAAAAAAAAA"))

        assertEquals(setOf("T261027AAAAAAAAAAAAA", "T261028AAAAAAAAAAAAA"), journalAt("2026-10-29T01:00:00Z").processedReferences())
        assertEquals(emptySet(), BankJournal(dir.resolve("empty")).processedReferences())
    }

    @Test
    fun `일마감은 매일 00시 05분 (KST) - 정각이면 다음 날`() {
        assertEquals(Duration.ofSeconds(1), delayUntilNextClose(Instant.parse("2026-10-28T15:04:59Z")))   // KST 00:04:59
        assertEquals(Duration.ofDays(1), delayUntilNextClose(Instant.parse("2026-10-28T15:05:00Z")))       // KST 00:05:00
        assertEquals(Duration.ofMinutes(65), delayUntilNextClose(Instant.parse("2026-10-28T14:00:00Z")))  // KST 23:00
    }
}
