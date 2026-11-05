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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Reactor 판 흐름 (P6-2, 설계 §3-3 · §4-2 · §4-3). CoroutineVerificationFlowTest 와 같은 행동을 같은 방법으로 본다. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactorVerificationFlowTest {

    private class RecordingRepository(private val inner: InMemoryVerificationRepository = InMemoryVerificationRepository()) :
        VerificationRepository by inner {
        val saveThreads = CopyOnWriteArrayList<String>()
        override fun save(verification: AccountVerification): AccountVerification =
            inner.save(verification).also { saveThreads += Thread.currentThread().name }
    }

    private val bank = MockBankServer(silentAccounts = setOf("9990000000001")).start()
    private val gateway = BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort, responseTimeout = Duration.ofMillis(500))).start()
    private val elasticExecutor = Executors.newSingleThreadExecutor { Thread(it, "test-elastic") }
    private val scheduler = Schedulers.fromExecutorService(elasticExecutor)
    private lateinit var repository: RecordingRepository
    private lateinit var flow: ReactorVerificationFlow

    @BeforeEach
    fun setUp() {
        bank.responseDelay = Duration.ZERO
        repository = RecordingRepository()
        flow = ReactorVerificationFlow(gateway, VerificationRequestRules(codeGenerator = { "1234" }), repository, RandomReferenceGenerator(prefix = 'V'), scheduler)
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) { check(System.currentTimeMillis() < deadline) { "gateway not connected" }; Thread.sleep(20) }
    }

    @AfterAll
    fun tearDown() { gateway.close(); bank.close(); scheduler.dispose() }

    @Test
    fun `예금주가 맞으면 1원을 보내고 publishOn 스케줄러에서 저장한다 - 이벤트 루프에서 JDBC 를 기다리지 않는다`() {
        val before = bank.depositsOf("1002123456789").size

        val v = flow.request(command("1002123456789", "홍 길동")).block(Duration.ofSeconds(5))!!

        assertEquals("홍길동", v.holderName)
        assertEquals(before + 1, bank.depositsOf("1002123456789").size)
        assertEquals(listOf("test-elastic"), repository.saveThreads)
    }

    @Test
    fun `없는 계좌 · 불일치 · 무응답은 같은 예외로 끝난다`() {
        assertFailsWith<AccountVerificationException.AccountNotFound> { flow.request(command("5599000000001", "홍길동")).block(Duration.ofSeconds(5)) }
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { flow.request(command("3333012345678", "홍길동")).block(Duration.ofSeconds(5)) }
        assertFailsWith<BankTimeoutException> { flow.request(command("9990000000001", "무응답")).block(Duration.ofSeconds(5)) }
        assertTrue(repository.saveThreads.isEmpty())
    }

    @Test
    fun `구독을 1원 이체 대기 중에 끊으면 저장하지 않는다`() {
        bank.responseDelay = Duration.ofMillis(300)
        val before = bank.depositsOf("1002123456789").size
        val subscription = flow.request(command("1002123456789", "홍길동")).subscribe()
        while (bank.depositsOf("1002123456789").size == before) Thread.sleep(5)

        subscription.dispose()

        Thread.sleep(500)
        assertTrue(repository.saveThreads.isEmpty(), "취소된 요청은 저장하지 않는다")
    }

    private fun command(accountNo: String, name: String) = RequestVerification("user-1", "090", accountNo, name)
}
