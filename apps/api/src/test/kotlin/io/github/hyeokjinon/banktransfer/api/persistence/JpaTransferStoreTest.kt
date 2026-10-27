package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.CannotAcquireLockException
import org.springframework.dao.PessimisticLockingFailureException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class JpaTransferStoreTest : ApiIntegrationTest() {

    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var namedJdbc: NamedParameterJdbcTemplate

    /** 기준선 차감 전략 (P4-2). 테스트가 직접 만드는 저장소(아웃박스를 깨뜨린 것 등)에 넣는다. */
    private val conditional by lazy { ConditionalWalletDebit(namedJdbc, wallets) }

    private val references = RandomReferenceGenerator()

    /** 이 테스트의 사용자 (테스트마다 새로, 1천만 원 충전 — 출금마다 5만 원씩 빠진다). */
    private val user by lazy { fundedUser() }

    private fun newTransfer(
        key: String = UUID.randomUUID().toString(),
        reference: String = references.next(),
        userId: String = user,
        // 모의 은행에 없는 계좌: 남는 RECEIVED 를 나중에 다른 테스트가 발행해도 공용 계좌에 입금이 생기지 않는다
        accountNo: String = unregisteredAccount(),
    ): Transfer {
        val now = Instant.parse("2026-10-19T01:00:00.123456Z")
        return Transfer(
            id = UUID.randomUUID(), userId = userId, idempotencyKey = key, reference = reference, requestHash = "h".repeat(64),
            bankCode = "090", accountNo = accountNo, amount = 50_000, memo = "출금",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
    }

    private fun requested(t: Transfer) = TransferEvent.TransferRequested(t.id, t.createdAt)

    @Test
    fun `이체와 아웃박스 이벤트를 함께 저장한다`() {
        val transfer = newTransfer()

        val saved = store.saveWithEvent(transfer, requested(transfer))

        assertEquals(transfer, saved)
        assertEquals(transfer, store.findById(transfer.id))
        assertEquals(transfer, store.findByIdempotencyKey(user, transfer.idempotencyKey))
        assertEquals(1, outbox.countByAggregateId(transfer.id.toString()))
        val payloadTransferId = jdbc.queryForObject(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.transferId')) FROM outbox_event WHERE aggregate_id = ?",
            String::class.java, transfer.id.toString(),
        )
        assertEquals(transfer.id.toString(), payloadTransferId)
    }

    @Test
    fun `같은 키면 DuplicateIdempotencyKey 이고 두 번째 이벤트는 남지 않는다`() {
        val first = newTransfer()
        store.saveWithEvent(first, requested(first))
        val second = newTransfer(key = first.idempotencyKey)

        assertFailsWith<StoreConflict.DuplicateIdempotencyKey> { store.saveWithEvent(second, requested(second)) }
        assertEquals(0, outbox.countByAggregateId(second.id.toString()))
    }

    @Test
    fun `같은 거래번호면 DuplicateReference`() {
        val first = newTransfer()
        store.saveWithEvent(first, requested(first))
        val second = newTransfer(reference = first.reference)

        assertFailsWith<StoreConflict.DuplicateReference> { store.saveWithEvent(second, requested(second)) }
    }

    @Test
    fun `아웃박스 저장이 실패하면 이체도 저장되지 않는다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, wallets, conditional, payloadOf = { "not-json" })
        val transfer = newTransfer()

        assertFails { broken.saveWithEvent(transfer, requested(transfer)) }
        assertNull(store.findById(transfer.id))
    }

    @Test
    fun `조건부 전이 - 현재 상태가 맞을 때만 바뀌고 sending_at 은 KST 로 기록된다`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))
        val at = Instant.parse("2026-10-20T01:00:00Z")

        assertTrue(store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at))
        assertFalse(store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at))   // 두 번째는 실패

        val after = store.findById(transfer.id)!!
        assertEquals(TransferStatus.SENDING, after.status)
        assertEquals(at, after.sendingAt)
        val row = jdbc.queryForMap(
            "SELECT DATE_FORMAT(sending_at, '%Y-%m-%d %H:%i:%s') AS sending_kst, version FROM transfer WHERE id = ?",
            transfer.id.toString(),
        )
        assertEquals("2026-10-20 10:00:00", row["sending_kst"])
        assertEquals(1L, (row["version"] as Number).toLong())
    }

    @Test
    fun `상태 표에 없는 전이는 DB 에 가기 전에 거부한다`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))

        assertFailsWith<IllegalArgumentException> {
            store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.COMPLETED, Instant.now())
        }
        assertEquals(TransferStatus.RECEIVED, store.findById(transfer.id)!!.status)
    }

    @Test
    fun `결과 기록 - SENDING 에서 COMPLETED 와 응답코드`() {
        val transfer = newTransfer()
        store.saveWithEvent(transfer, requested(transfer))
        val at = Instant.parse("2026-10-20T01:00:01Z")
        store.transition(transfer.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)

        assertTrue(store.transition(transfer.id, TransferStatus.SENDING, TransferStatus.COMPLETED, at, "0000"))

        val after = store.findById(transfer.id)!!
        assertEquals(TransferStatus.COMPLETED, after.status)
        assertEquals("0000", after.bankResponseCode)
        assertEquals(at, after.updatedAt)
    }

    @Test
    fun `멈춘 SENDING 조회 - 기준 시각보다 오래된 SENDING 만`() {
        val old = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val recent = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val now = Instant.now()
        store.transition(old.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(60))
        store.transition(recent.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now)

        val found = store.findSendingBefore(now.minusSeconds(30), 1_000).map { it.id }

        assertTrue(old.id in found)
        assertFalse(recent.id in found)
    }

    @Test
    fun `결과조회 대상 - sending_at 이 유예 기준보다 이른 UNKNOWN 만`() {
        val now = Instant.now()
        val old = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val fresh = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        listOf(old to now.minusSeconds(31), fresh to now.minusSeconds(1)).forEach { (t, sendingAt) ->
            store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, sendingAt)
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)   // updated_at 은 now, sending_at 은 그대로
        }

        val found = store.findUnknownForInquiry(10, now.minusSeconds(30), 1_000).map { it.id }

        assertTrue(old.id in found)
        assertFalse(fresh.id in found)   // 방금 UNKNOWN 이 된 건은 유예 안쪽 — 아직 묻지 않는다
    }

    @Test
    fun `결과조회 대상 - 시도 횟수가 한도 미만인 UNKNOWN, 시도 기록은 UNKNOWN 일 때만`() {
        val unknown = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val at = Instant.now()
        store.transition(unknown.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)
        store.transition(unknown.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, at)

        assertTrue(store.recordInquiryAttempt(unknown.id, at))

        assertEquals(1, store.findById(unknown.id)!!.inquiryAttempts)
        val graceOver = at.plusSeconds(1)   // sending_at(at) 은 이 기준보다 이르다
        assertFalse(unknown.id in store.findUnknownForInquiry(1, graceOver, 1_000).map { it.id })
        assertTrue(unknown.id in store.findUnknownForInquiry(2, graceOver, 1_000).map { it.id })

        val received = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        assertFalse(store.recordInquiryAttempt(received.id, at))   // UNKNOWN 이 아니면 늘리지 않는다
    }

    @Test
    fun `재발행 - RECEIVED 이고 기대 횟수가 맞으면 횟수 +1 과 republished_at, 접수와 같은 모양의 이벤트를 아웃박스에 한 줄 더`() {
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val at = Instant.parse("2026-10-22T01:00:00.654321Z")

        assertTrue(store.republish(t.id, 0, at))

        val after = store.findById(t.id)!!
        assertEquals(TransferStatus.RECEIVED, after.status)
        assertEquals(1, after.republishCount)
        assertEquals(at, after.republishedAt)
        assertEquals(at, after.updatedAt)
        val rows = jdbc.queryForList(
            "SELECT event_type, JSON_UNQUOTE(JSON_EXTRACT(payload, '$.transferId')) AS transfer_id, " +
                "JSON_UNQUOTE(JSON_EXTRACT(payload, '$.occurredAt')) AS occurred_at, published_at " +
                "FROM outbox_event WHERE aggregate_id = ? ORDER BY id",
            t.id.toString(),
        )
        assertEquals(2, rows.size)
        assertEquals("TransferRequested", rows[1]["event_type"])
        assertEquals(t.id.toString(), rows[1]["transfer_id"])
        assertEquals(at.toString(), rows[1]["occurred_at"])
        assertNull(rows[1]["published_at"])   // 발행기가 평소처럼 내보낸다
    }

    @Test
    fun `재발행 - 기대 횟수가 다르거나 RECEIVED 가 아니면 0건이고 아웃박스도 그대로`() {
        val raced = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        val sending = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(sending.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())

        assertFalse(store.republish(raced.id, 1, Instant.now()))     // 다른 인스턴스가 먼저 0 → 1 로 바꾼 상황
        assertFalse(store.republish(sending.id, 0, Instant.now()))   // 그 사이 컨슈머가 집어 갔다

        assertEquals(0, store.findById(raced.id)!!.republishCount)
        assertEquals(1, outbox.countByAggregateId(raced.id.toString()))
        assertEquals(1, outbox.countByAggregateId(sending.id.toString()))
    }

    @Test
    fun `재발행 - 아웃박스 INSERT 가 실패하면 횟수 증가도 롤백된다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, wallets, conditional, payloadOf = { "not-json" })
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }

        assertFails { broken.republish(t.id, 0, Instant.now()) }

        val after = store.findById(t.id)!!
        assertEquals(0, after.republishCount)   // 횟수만 늘고 메시지는 없는 상태가 생기지 않는다
        assertNull(after.republishedAt)
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `재발행 대상 - 오래된 RECEIVED 중 마지막 재발행 이후 은행에 보내려 한 흔적이 있는 것만`() {
        val now = Instant.now()
        fun saved() = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        fun triedAndReverted(t: Transfer, at: Instant) {
            store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, at)
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.RECEIVED, at)   // 미전송 → RECEIVED (sending_at 은 남는다)
        }

        val tried = saved().also { triedAndReverted(it, now.minusSeconds(600)) }
        val fresh = saved().also { triedAndReverted(it, now.minusSeconds(60)) }
        val neverTried = saved()                                                        // updated_at 은 2026-10-19 (오래됨), sending_at 없음
        val republishedNotTried = saved().also {
            triedAndReverted(it, now.minusSeconds(1_200))
            store.republish(it.id, 0, now.minusSeconds(600))
        }
        val republishedThenTried = saved().also {
            triedAndReverted(it, now.minusSeconds(1_800))
            store.republish(it.id, 0, now.minusSeconds(1_200))
            triedAndReverted(it, now.minusSeconds(600))
        }
        // 컨트롤러 지시: 재발행과 같은 시각의 시도는 "재발행 이후" 가 아니다 (엄격한 > 경계)
        val sameInstant = saved().also {
            triedAndReverted(it, now.minusSeconds(900))
            store.republish(it.id, 0, now.minusSeconds(900))
        }
        val sending = saved().also { store.transition(it.id, TransferStatus.RECEIVED, TransferStatus.SENDING, now.minusSeconds(600)) }

        val found = store.findRepublishable(now.minus(Duration.ofMinutes(5)), 1_000).map { it.id }

        assertTrue(tried.id in found)
        assertTrue(republishedThenTried.id in found)   // 2회차: republish_count = 1 이고 sending_at 이 republished_at 보다 나중
        assertEquals(1, store.findById(republishedThenTried.id)!!.republishCount)
        assertFalse(fresh.id in found)                 // 5분이 안 됐다 (재시도 토픽 · DLT 진행 중일 수 있다)
        assertFalse(neverTried.id in found)            // 은행에 시도도 못 했다 (Kafka · 컨슈머 장애)
        assertFalse(republishedNotTried.id in found)   // 재발행한 뒤 아직 아무도 안 집어 갔다
        assertFalse(sameInstant.id in found)           // sending_at == republished_at 은 제외 (엄격한 >)
        assertFalse(sending.id in found)
    }

    @Test
    fun `접수 - 출금 INSERT · 잔고 차감 · 원장 WITHDRAW · 아웃박스가 한 트랜잭션에 들어간다`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer()

        store.saveWithEvent(t, requested(t))

        assertEquals(before - 50_000, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW"), ledgerTypes(t.id))
        assertEquals(1, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `접수 - 잔고가 모자라면 InsufficientBalance 이고 출금 · 원장 · 아웃박스 모두 남지 않는다`() {
        val poor = fundedUser(49_999)
        val t = newTransfer(userId = poor)

        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(t, requested(t)) }

        assertNull(store.findById(t.id))
        assertEquals(49_999L, wallets.balanceOf(poor))
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertEquals(0, outbox.countByAggregateId(t.id.toString()))
    }

    @Test
    fun `접수 - 지갑이 없으면 잔고 0 으로 보고 InsufficientBalance`() {
        val t = newTransfer(userId = newUserId())

        assertFailsWith<StoreConflict.InsufficientBalance> { store.saveWithEvent(t, requested(t)) }
        assertNull(store.findById(t.id))
    }

    @Test
    fun `접수 - 사용자가 없는 출금은 저장하지 않는다 (차감 없이 나가는 길을 막는다)`() {
        val t = newTransfer().copy(userId = null)

        assertFailsWith<IllegalArgumentException> { store.saveWithEvent(t, requested(t)) }
        assertNull(store.findById(t.id))
    }

    @Test
    fun `접수 - 아웃박스 INSERT 가 실패하면 차감도 롤백된다`() {
        val broken = JpaTransferStore(transfers, outbox, tx, wallets, conditional, payloadOf = { "not-json" })
        val before = wallets.balanceOf(user)
        val t = newTransfer()

        assertFails { broken.saveWithEvent(t, requested(t)) }

        assertEquals(before, wallets.balanceOf(user))   // 돈만 빠지고 출금은 없는 상태가 생기지 않는다
        assertEquals(emptyList(), ledgerTypes(t.id))
        assertNull(store.findById(t.id))
    }

    @Test
    fun `같은 키 · 다른 사용자는 별개 출금이다 (UNIQUE 는 user_id + idempotency_key)`() {
        val other = fundedUser()
        val mine = newTransfer()
        val theirs = newTransfer(key = mine.idempotencyKey, userId = other)

        store.saveWithEvent(mine, requested(mine))
        store.saveWithEvent(theirs, requested(theirs))

        assertEquals(mine.id, store.findByIdempotencyKey(user, mine.idempotencyKey)!!.id)
        assertEquals(theirs.id, store.findByIdempotencyKey(other, mine.idempotencyKey)!!.id)
    }

    @Test
    fun `transition 은 FAILED 를 거부한다 - 환불을 빠뜨리지 않게 fail() 로만`() {
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())

        assertFailsWith<IllegalArgumentException> {
            store.transition(t.id, TransferStatus.SENDING, TransferStatus.FAILED, Instant.now(), "1001")
        }
        assertEquals(TransferStatus.SENDING, store.findById(t.id)!!.status)
    }

    @Test
    fun `fail - 조건부 전이가 1건이면 FAILED 와 코드, 차감액을 돌려주고 원장 REFUND`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())
        val at = Instant.parse("2026-10-26T01:00:00.123456Z")

        assertTrue(store.fail(t.id, TransferStatus.SENDING, at, "1001"))

        val after = store.findById(t.id)!!
        assertEquals(TransferStatus.FAILED, after.status)
        assertEquals("1001", after.bankResponseCode)
        assertEquals(at, after.updatedAt)
        assertEquals(before, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(t.id))
    }

    @Test
    fun `fail - 두 번 불러도 (이미 FAILED 면 0건) 환불은 한 번`() {
        val before = wallets.balanceOf(user)
        val t = newTransfer().also { store.saveWithEvent(it, requested(it)) }
        store.transition(t.id, TransferStatus.RECEIVED, TransferStatus.SENDING, Instant.now())
        store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, Instant.now())

        assertTrue(store.fail(t.id, TransferStatus.UNKNOWN, Instant.now(), "2001"))
        assertFalse(store.fail(t.id, TransferStatus.UNKNOWN, Instant.now(), "2001"))   // 컨슈머 · 정리 작업이 겹친 상황

        assertEquals(before, wallets.balanceOf(user))
        assertEquals(listOf("WITHDRAW", "REFUND"), ledgerTypes(t.id))
    }

    @Test
    fun `fail - 환불이 실패하면 상태 전이도 롤백된다 (한 트랜잭션)`() {
        // 모의 은행에 없는 계좌: 이 RECEIVED 를 다른 테스트의 발행 · 정리 작업이 집어 가도 공용 계좌에 입금이 생기지 않는다
        val t = newTransfer(accountNo = unregisteredAccount()).also { store.saveWithEvent(it, requested(it)) }
        val balanceBefore = wallets.balanceOf(user)
        // 이미 REFUND 가 있는 것처럼 만든다 → 환불의 원장 INSERT 가 uk_ledger_transfer_type 에 걸린다
        jdbc.update(
            "INSERT INTO wallet_ledger (user_id, type, amount, balance_after, transfer_id, created_at) VALUES (?, 'REFUND', 1, 0, ?, NOW(6))",
            user, t.id.toString(),
        )

        try {
            assertFails { store.fail(t.id, TransferStatus.RECEIVED, Instant.now(), "E001") }

            assertEquals(TransferStatus.RECEIVED, store.findById(t.id)!!.status)   // "FAILED 인데 돈은 안 돌아온" 상태가 생기지 않는다
            assertEquals(balanceBefore, wallets.balanceOf(user))                   // 잔고 += 도 같이 롤백됐다
        } finally {
            // 이 RECEIVED 는 일부러 깨진 상태(가짜 REFUND)라 다시 처리되면 안 된다: 발행된 것으로 표시해 발행기 · 정리 작업이 건드리지 않게
            jdbc.update("UPDATE outbox_event SET published_at = NOW(6) WHERE aggregate_id = ?", t.id.toString())
        }
    }

    @Test
    fun `fail - 차감 기록이 없는 P3 시절 출금(user_id NULL)은 상태만 바꾸고 환불하지 않는다`() {
        val id = UUID.randomUUID()
        jdbc.update(
            "INSERT INTO transfer (id, idempotency_key, reference, request_hash, bank_code, account_no, amount, memo, status, " +
                "created_at, updated_at, version) VALUES (?, ?, ?, ?, '090', '1002123456789', 1000, 'legacy', 'UNKNOWN', NOW(6), NOW(6), 0)",
            id.toString(), UUID.randomUUID().toString(), references.next(), "h".repeat(64),
        )

        assertTrue(store.fail(id, TransferStatus.UNKNOWN, Instant.now(), "2001"))

        val after = store.findById(id)!!
        assertEquals(TransferStatus.FAILED, after.status)
        assertNull(after.userId)
        assertEquals(emptyList(), ledgerTypes(id))
    }

    @Test
    fun `접수 - 바깥 트랜잭션 안에서 부르면 거부한다 (낙관적 재시도 · Redis 락은 트랜잭션 바깥이어야 한다)`() {
        val t = newTransfer()

        assertFailsWith<IllegalStateException> { tx.execute { store.saveWithEvent(t, requested(t)) } }

        assertNull(store.findById(t.id))
    }

    @Test
    fun `접수 - 차감 전략의 바깥(around)이 접수 트랜잭션 전체를 감싼다`() {
        val seen = mutableListOf<String>()
        var activeBefore: Boolean? = null
        var committedAfter: Int? = null
        val t = newTransfer()
        val wrapping = object : WalletDebit by conditional {
            override fun <T> around(userId: String, transaction: () -> T): T {
                seen += userId
                activeBefore = TransactionSynchronizationManager.isActualTransactionActive()
                return transaction().also {
                    // 다른 스레드(다른 커넥션)에서 보인다 = transaction() 이 돌아왔을 때 이미 커밋됐다
                    committedAfter = CompletableFuture.supplyAsync {
                        jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE id = ?", Int::class.java, t.id.toString())
                    }.get(10, TimeUnit.SECONDS)
                }
            }
        }

        JpaTransferStore(transfers, outbox, tx, wallets, wrapping).saveWithEvent(t, requested(t))

        assertEquals(listOf(user), seen)
        assertEquals(false, activeBefore)
        assertEquals(1, committedAfter)
    }

    @Test
    fun `접수 - 락을 못 잡으면(MySQL 1205 · 1213 = PessimisticLockingFailureException 계열) 전략과 무관하게 WalletBusy, 아무것도 남지 않는다`() {
        for (failure in listOf(CannotAcquireLockException("Lock wait timeout exceeded (test)"), PessimisticLockingFailureException("Deadlock found (test)"))) {
            val busy = object : WalletDebit {
                override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean = throw failure
            }
            val before = wallets.balanceOf(user)
            val t = newTransfer()

            assertFailsWith<StoreConflict.WalletBusy> { JpaTransferStore(transfers, outbox, tx, wallets, busy).saveWithEvent(t, requested(t)) }

            assertNull(store.findById(t.id))
            assertEquals(before, wallets.balanceOf(user))
            assertEquals(0, outbox.countByAggregateId(t.id.toString()))
        }
    }
}
