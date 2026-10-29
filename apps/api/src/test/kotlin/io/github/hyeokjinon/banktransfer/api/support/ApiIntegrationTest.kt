package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.persistence.WalletLedger
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

/**
 * 모든 Spring 통합 테스트의 부모. 설정이 같으므로 스프링 테스트 컨텍스트가 하나로 캐시되어
 * MySQL 컨테이너와 모의 은행도 한 번만 뜬다. 테스트끼리 DB 를 공유하므로 키 · 계좌번호는 테스트마다 새로 만든다.
 * 폴링 퍼블리셔 스케줄은 끈다: 테스트가 OutboxRelay.publishPending() 을 직접 불러 발행 시점을 정한다 (다른 테스트의 이체 상태가 갑자기 바뀌지 않게).
 * 정리 작업도 마찬가지로 끈다 (TransferReconciler.reconcile() 을 직접 부른다).
 * 재시도 토픽 간격은 위 프로퍼티로 줄인다. 은행 장애는 [withBankDown] 으로 만든다.
 * 잔고 단언이 다른 테스트에 흔들리지 않게 테스트마다 새 사용자를 쓴다 (P4-1): 정리 작업(RepublishFlowTest 의 1ms 대기)이 남은 RECEIVED 를 모두 훑어 환불할 수 있다.
 * Redis 컨테이너는 차감 전략 테스트(RedisWalletDebitTest)용이다 (P4-2). 앱은 기본 전략(conditional)이라 Redis 를 쓰지 않는다.
 * 앱 시계는 [testClock] 이다 (P5-1): 점검 시간 · 대사 테스트는 fix 로 시각을 정하고, 테스트가 끝나면 진짜 시각으로 돌아간다.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "transfer.outbox.relay.enabled=false",
        "transfer.reconcile.enabled=false",
        // 재시도 간격 0.5초 → 1초 → 2초 (운영 5 · 30 · 60초). 뒤 메시지가 막히지 않는지 볼 여유(3.5초)는 남기고 테스트는 빨리 끝나게
        "transfer.retry.delay-ms=500",
        "transfer.retry.multiplier=2.0",
        "transfer.retry.max-delay-ms=2000",
    ],
)
@Import(MySqlTestContainer::class, KafkaTestContainer::class, RedisTestContainer::class, TestClockConfiguration::class)
abstract class ApiIntegrationTest {

    companion object {
        /** 9990000000001 = 무응답 · 처리 안 함. 9990000000002 = 처리 후 무응답 (MockBankServer 기본값). 컨텍스트와 수명이 같으므로 닫지 않는다. */
        val bank: MockBankServer = MockBankServer(silentAccounts = setOf("9990000000001")).start()

        @JvmStatic
        @DynamicPropertySource
        fun bankProperties(registry: DynamicPropertyRegistry) {
            registry.add("bank.gateway.port") { bank.boundPort }
            registry.add("bank.gateway.response-timeout") { "500ms" }
        }
    }

    @Value("\${local.server.port}")
    protected var port: Int = 0

    @Autowired
    protected lateinit var gateway: BankGatewayClient

    protected val client: RestClient by lazy { RestClient.create("http://localhost:$port") }

    @Autowired
    protected lateinit var wallets: WalletLedger

    @Autowired
    protected lateinit var testClock: TestClock

    // 이름에 test 를 붙인다: 하위 클래스가 이미 private verifications · jdbc 를 갖고 있어 같은 이름이면 가려진다
    @Autowired
    private lateinit var testVerifications: VerificationRepository

    @Autowired
    private lateinit var testJdbc: JdbcTemplate

    /** 새 사용자 id (UUID 36자 = X-User-Id 상한). 테스트끼리 DB 를 공유하므로 매번 새로 만든다. */
    protected fun newUserId(): String = UUID.randomUUID().toString()

    /** 새 사용자에게 [amount] 원을 충전해 id 를 돌려준다 (충전 API 를 거치지 않고 WalletLedger 로). */
    protected fun fundedUser(amount: Long = 10_000_000): String = newUserId().also { wallets.deposit(it, amount) }

    /** [userId] 가 1원인증을 마친(VERIFIED) 계좌로 DB 에 남긴다. 받는 계좌 확인은 사용자 범위다 (P4-1). */
    protected fun verifiedBy(userId: String, accountNo: String): String {
        testVerifications.save(
            AccountVerification(
                id = UUID.randomUUID(), userId = userId, bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
        return accountNo
    }

    /**
     * 모의 은행에 없는 계좌 (처리되면 1001 거절 → FAILED → 환불). RECEIVED 를 남기는 테스트는 이것을 쓴다:
     * 나중에 다른 테스트의 발행 · 정리 작업이 처리해도 공용 계좌(1002123456789)에 입금이 생기지 않는다.
     */
    protected fun unregisteredAccount(): String = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)

    /** 이 출금의 원장 종류(WITHDRAW, REFUND …)를 기록 순서대로. */
    protected fun ledgerTypes(transferId: UUID): List<String> =
        testJdbc.queryForList("SELECT type FROM wallet_ledger WHERE transfer_id = ? ORDER BY id", String::class.java, transferId.toString()).filterNotNull()

    /** 원장 합 = 충전 - 출금 + 환불. 지갑 잔고와 같아야 한다 (database.md §9 운영 쿼리와 같은 식). */
    protected fun ledgerSum(userId: String): Long =
        testJdbc.queryForObject(
            "SELECT COALESCE(SUM(CASE type WHEN 'WITHDRAW' THEN -amount ELSE amount END), 0) FROM wallet_ledger WHERE user_id = ?",
            Long::class.java, userId,
        )!!

    @BeforeEach
    fun waitForBankConnection() = awaitGateway(connected = true, timeout = Duration.ofSeconds(5))

    /** 시각을 고정한 테스트가 다음 테스트 · 백그라운드 컨슈머에 고정 시각을 남기지 않게. */
    @AfterEach
    fun releaseClock() = testClock.release()

    /**
     * 은행 장애를 만든다: 모의 은행을 내려(붙어 있던 연결도 끊음) [block] 을 돌리고, 끝나면 **반드시** 같은 포트로 다시 올린다.
     * 게이트웨이 재접속은 1초부터 두 배씩(최대 30초) 늘어나므로 다시 붙을 때까지 넉넉히 기다린다 — 다음 테스트가 끊긴 채 시작하지 않게.
     */
    protected fun <T> withBankDown(block: () -> T): T {
        bank.pause()
        var failure: Throwable? = null
        try {
            awaitGateway(connected = false, timeout = Duration.ofSeconds(5))
            return block()
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            // 정리가 실패해도 진짜 실패 원인(block 의 예외)을 가리지 않는다: 원래 예외를 주(主)로 두고 정리 실패는 suppressed 로 붙인다
            try {
                bank.resume()
                awaitGateway(connected = true, timeout = Duration.ofSeconds(40))
            } catch (cleanup: Throwable) {
                if (failure == null) throw cleanup
                failure.addSuppressed(cleanup)
            }
        }
    }

    private fun awaitGateway(connected: Boolean, timeout: Duration) {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        while (gateway.isConnected != connected) {
            check(System.currentTimeMillis() < deadline) { "gateway connected=${gateway.isConnected}, expected $connected within $timeout" }
            Thread.sleep(20)
        }
    }
}
