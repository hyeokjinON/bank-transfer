package io.github.hyeokjinon.banktransfer.mockbank

import io.github.hyeokjinon.banktransfer.protocol.BizType
import io.github.hyeokjinon.banktransfer.protocol.Header
import io.github.hyeokjinon.banktransfer.protocol.InquiryRequest
import io.github.hyeokjinon.banktransfer.protocol.InquiryResponse
import io.github.hyeokjinon.banktransfer.protocol.MessageClass
import io.github.hyeokjinon.banktransfer.protocol.ResponseCodes
import io.github.hyeokjinon.banktransfer.protocol.TransferRequest
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** 재기동해도 결과조회가 처리 기록대로 답한다 (P5-1). 기록 디렉터리를 주지 않으면(기존 테스트의 기본값) 아무것도 쓰지 않는다. */
class MockBankRestartTest {

    @TempDir
    lateinit var dir: Path

    private val header = Header(MessageClass.REQUEST, BizType.TRANSFER, 1, LocalDateTime.of(2026, 10, 28, 10, 0))

    private fun inquire(bank: MockBankServer, reference: String) =
        bank.respond(InquiryRequest(header.copy(bizType = BizType.INQUIRY), reference)) as InquiryResponse

    @Test
    fun `재기동해도 결과조회가 처리 기록대로 답한다`() {
        val reference = "T261028RESTART000001"
        MockBankServer(bankFiles = dir).start().use { it.respond(TransferRequest(header, "090", "1002123456789", 30_000, "m", reference)) }

        MockBankServer(bankFiles = dir).start().use { restarted ->
            assertTrue(restarted.hasProcessed(reference))
            assertEquals(ResponseCodes.OK, inquire(restarted, reference).result)
            assertEquals(ResponseCodes.ORIGINAL_NOT_FOUND, inquire(restarted, "T261028NEVERSENT0001").result)
        }
    }

    @Test
    fun `없는 계좌로 거절한 이체는 기록하지 않는다 - 돈이 움직이지 않았다`() {
        MockBankServer(bankFiles = dir).start().use { it.respond(TransferRequest(header, "090", "5500000000001", 1_000, "m", "T261028REJECTED00001")) }

        assertFalse(Files.exists(dir.resolve("journal")))
    }

    @Test
    fun `기록 디렉터리를 주지 않으면 작업 디렉터리에 아무것도 만들지 않는다 (기존 테스트의 기본값)`() {
        MockBankServer().start().use { it.respond(TransferRequest(header, "090", "1002123456789", 1_000, "m", "T261028MEMORYONLY001")) }

        assertFalse(Files.exists(Path.of("bank-files")))   // Gradle 테스트의 작업 디렉터리 = apps/mock-bank
    }
}
