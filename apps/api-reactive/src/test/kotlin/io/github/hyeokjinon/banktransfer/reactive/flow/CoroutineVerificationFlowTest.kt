package io.github.hyeokjinon.banktransfer.reactive.flow

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.AccountVerificationException
import io.github.hyeokjinon.banktransfer.account.InMemoryVerificationRepository
import io.github.hyeokjinon.banktransfer.account.RequestVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationRequestRules
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.gateway.BankTimeoutException
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** 코루틴 판 흐름 (P6-2, 설계 §3-3 · §4-2 · §4-3). 은행은 진짜 모의 은행 · 게이트웨이, 저장소는 저장한 스레드를 기록하는 가짜. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CoroutineVerificationFlowTest {

    private class RecordingRepository(private val inner: InMemoryVerificationRepository = InMemoryVerificationRepository()) :
        VerificationRepository by inner {
        val saveThreads = CopyOnWriteArrayList<String>()
        override fun save(verification: AccountVerification): AccountVerification =
            inner.save(verification).also { saveThreads += Thread.currentThread().name }
    }

    private val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val ioExecutor = Executors.newSingleThreadExecutor { Thread(it, "test-io") }
    private val io = ioExecutor.asCoroutineDispatcher()
    private lateinit var repository: RecordingRepository
    private lateinit var flow: CoroutineVerificationFlow

    @BeforeEach
    fun setUp() {
        bank.responseDelay = Duration.ZERO
        repository = RecordingRepository()
        flow = CoroutineVerificationFlow(gateway, VerificationRequestRules(codeGenerator = { "1234" }), repository, RandomReferenceGenerator(prefix = 'V'), io)
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { gateway.close(); bank.close(); io.close() }

    private fun command(accountNo: String, name: String) = RequestVerification("user-1", "090", accountNo, name)

    @Test
    fun `예금주가 맞으면 1원을 보내고 IO 디스패처에서 저장한다 - 이벤트 루프에서 JDBC 를 기다리지 않는다`() = runBlocking {
        val before = bank.depositsOf("1002123456789").size

        val v = flow.request(command("1002123456789", "홍 길동"))

        assertEquals("홍길동", v.holderName)
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertEquals("BT1234", bank.depositsOf("1002123456789").last().memo)
        assertTrue(bank.depositsOf("1002123456789").last().reference.startsWith("V"), "1원인증 거래번호는 V 접두")
        assertEquals(listOf("test-io"), repository.saveThreads.map { it.substringBefore(" @coroutine") })
    }

    @Test
    fun `없는 계좌면 AccountNotFound, 1원을 보내지 않는다`() = runBlocking {
        assertFailsWith<AccountVerificationException.AccountNotFound> { flow.request(command("5599000000001", "홍길동")) }
        assertTrue(repository.saveThreads.isEmpty())
    }

    @Test
    fun `예금주가 다르면 HolderNameMismatch, 1원을 보내지 않는다`() = runBlocking {
        val before = bank.depositsOf("3333012345678").size
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { flow.request(command("3333012345678", "홍길동")) }
        assertEquals(before, bank.depositsOf("3333012345678").size)
    }

    @Test
    fun `은행 무응답이면 BankTimeoutException 그대로`() = runBlocking {
        assertFailsWith<BankTimeoutException> { flow.request(command("9990000000001", "무응답")) }
        Unit
    }

    @Test
    fun `1원 이체를 기다리는 중 취소되면 CancellationException 그대로 - 오류로 바꾸지 않고 저장하지 않는다`() = runBlocking {
        bank.responseDelay = Duration.ofMillis(300)
        val before = bank.depositsOf("1002123456789").size
        // 테스트 스레드가 아래에서 busy-wait 하므로 runBlocking 이벤트 루프의 async 는 시작하지 못한다 — Default 에서 돌린다
        val job = async(Dispatchers.Default) { flow.request(command("1002123456789", "홍길동")) }
        val deadline = System.currentTimeMillis() + 5_000
        while (bank.depositsOf("1002123456789").size == before) {
            check(System.currentTimeMillis() < deadline) { "은행이 1원 이체를 받지 못했다" }
            Thread.sleep(5)
        }   // 은행이 1원 이체를 받았다 = 응답 대기 중
        job.cancel()

        assertFailsWith<CancellationException> { job.await() }
        Thread.sleep(500)   // 응답(300ms)이 지나도
        assertTrue(repository.saveThreads.isEmpty(), "취소된 요청은 저장하지 않는다")
    }
}
