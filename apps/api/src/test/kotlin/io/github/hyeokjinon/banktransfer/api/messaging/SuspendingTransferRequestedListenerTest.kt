package io.github.hyeokjinon.banktransfer.api.messaging

import ch.qos.logback.classic.Level
import io.github.hyeokjinon.banktransfer.api.alert.Alert
import io.github.hyeokjinon.banktransfer.api.alert.AlertLevel
import io.github.hyeokjinon.banktransfer.api.alert.AlertNotifier
import io.github.hyeokjinon.banktransfer.api.support.captureLogs
import io.github.hyeokjinon.banktransfer.transfer.BankTransferOutcome
import io.github.hyeokjinon.banktransfer.transfer.BankTransferPort
import io.github.hyeokjinon.banktransfer.transfer.SuspendBankTransferPort
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferProcessor
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 코루틴 판 리스너 (P6-1). 결과는 블로킹 판과 같아야 하고(같은 판정 · 같은 공용 코드), 다른 것 셋을 본다:
 * DB 호출이 어느 스레드에서 도는가, 은행 동시 호출 상한, 기다리는 중 취소.
 * Spring 7 은 suspend 리스너를 Dispatchers.Unconfined 로 부른다(CoroutinesUtils, Task 1 실험 ③) — 테스트도 같은 조건으로 부른다 ([consume]).
 */
class SuspendingTransferRequestedListenerTest {

    /** 조건부 전이를 흉내 내는 가짜 저장소 + 부른 스레드 기록 (DB 호출이 어디서 돌았나). 여러 코루틴이 함께 부르므로 스레드 안전하게. */
    private class ThreadRecordingStore : TransferStore {
        val transfers = ConcurrentHashMap<UUID, Transfer>()
        val callThreads: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val failCalls = AtomicInteger()

        private fun touch() {
            callThreads += Thread.currentThread().name
        }

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer = transfer.also { transfers[it.id] = it }
        override fun findById(id: UUID): Transfer? {
            touch()
            return transfers[id]
        }

        override fun findByIdempotencyKey(userId: String, key: String): Transfer? = null

        override fun transition(id: UUID, from: TransferStatus, to: TransferStatus, at: Instant, bankResponseCode: String?, sentWithin: OpenEndRange<Instant>?): Boolean {
            touch()
            require(to != TransferStatus.FAILED) { "FAILED 는 fail() 로만 간다" }
            var moved = false
            transfers.computeIfPresent(id) { _, current ->
                if (current.status != from) {
                    current
                } else {
                    moved = true
                    current.copy(status = to, updatedAt = at, bankResponseCode = bankResponseCode, sendingAt = if (to == TransferStatus.SENDING) at else current.sendingAt)
                }
            }
            return moved
        }

        override fun fail(id: UUID, from: TransferStatus, at: Instant, bankResponseCode: String, sentWithin: OpenEndRange<Instant>?): Boolean {
            touch()
            var moved = false
            transfers.computeIfPresent(id) { _, current ->
                if (current.status != from) {
                    current
                } else {
                    moved = true
                    current.copy(status = TransferStatus.FAILED, updatedAt = at, bankResponseCode = bankResponseCode)
                }
            }
            if (moved) failCalls.incrementAndGet()
            return moved
        }

        override fun findSendingBefore(cutoff: Instant, limit: Int): List<Transfer> = emptyList()
        override fun findUnknownForInquiry(maxAttempts: Int, sendingBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun recordInquiryAttempt(id: UUID, at: Instant) = false
        override fun findRepublishable(updatedBefore: Instant, limit: Int): List<Transfer> = emptyList()
        override fun republish(id: UUID, expectedCount: Int, at: Instant) = false
    }

    private class CountingAck : Acknowledgment {
        var count = 0
        override fun acknowledge() {
            count++
        }
    }

    private val now = Instant.parse("2026-11-04T01:00:00Z")
    private val store = ThreadRecordingStore()
    private val processor = TransferProcessor(
        store,
        BankTransferPort { _, _, _, _, _ -> error("코루틴 판은 블로킹 포트를 부르지 않는다") },
        Clock.fixed(now, ZoneOffset.UTC),
    )
    private val dbThreads = AtomicInteger()
    private val dbPool = Executors.newFixedThreadPool(2) { r -> Thread(r, "test-db-${dbThreads.incrementAndGet()}").apply { isDaemon = true } }
    private val db = dbPool.asCoroutineDispatcher()
    private val bankLoop = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, BANK_LOOP).apply { isDaemon = true } }

    @AfterTest
    fun tearDown() {
        dbPool.shutdownNow()
        bankLoop.shutdownNow()
    }

    /** 은행 응답을 다른 스레드(이름 fake-bank-loop)에서 조금 뒤에 완료한다 — 게이트웨이의 Netty 이벤트 루프처럼. */
    private inner class LoopBank(private val outcome: BankTransferOutcome) : SuspendBankTransferPort {
        val calls = AtomicInteger()
        override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
            calls.incrementAndGet()
            return suspendCancellableCoroutine { cont -> bankLoop.schedule(Runnable { cont.resume(outcome) }, 20, TimeUnit.MILLISECONDS) }
        }
    }

    private fun received(): Transfer {
        val t = Transfer(
            id = UUID.randomUUID(), userId = "u-1", idempotencyKey = UUID.randomUUID().toString(), reference = "T2611040000000000001",
            requestHash = "h", bankCode = "090", accountNo = "1002123456789", amount = 1_000, memo = "m",
            status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.transfers[t.id] = t
        return t
    }

    private fun record(id: UUID) = ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, id.toString(), """{"transferId":"$id"}""")

    private fun listener(bank: SuspendBankTransferPort, permits: Int = 64, alerts: AlertNotifier = AlertNotifier { }) =
        SuspendingTransferRequestedListener(processor, bank, Semaphore(permits), db, alerts)

    /** Spring 7 이 suspend 리스너를 부르는 방식과 같게: CoroutinesUtils.invokeSuspendingFunction 은 Dispatchers.Unconfined 다. */
    private fun consume(listener: SuspendingTransferRequestedListener, record: ConsumerRecord<String, String>) =
        runBlocking { withContext(Dispatchers.Unconfined) { listener.on(record) } }

    @Test
    fun `완료 · 거절 · 모름은 블로킹 판과 같은 상태로 기록하고 정상으로 끝난다 (예외 없음 = 프레임워크가 ack)`() {
        val cases = mapOf(
            BankTransferOutcome.Completed to TransferStatus.COMPLETED,
            BankTransferOutcome.Rejected("1001") to TransferStatus.FAILED,
            BankTransferOutcome.ResultUnknown to TransferStatus.UNKNOWN,
        )
        for ((outcome, expected) in cases) {
            val t = received()
            consume(listener(LoopBank(outcome)), record(t.id))
            assertEquals(expected, store.transfers.getValue(t.id).status, "$outcome")
        }
        assertEquals(1, store.failCalls.get())   // 거절만 fail() — 환불 경로
    }

    @Test
    fun `미전송이면 RECEIVED 로 되돌리고 RetryLaterException - 재시도 토픽으로`() {
        val t = received()

        assertFailsWith<RetryLaterException> { consume(listener(LoopBank(BankTransferOutcome.NotSent)), record(t.id)) }
        assertEquals(TransferStatus.RECEIVED, store.transfers.getValue(t.id).status)
    }

    @Test
    fun `은행 포트가 분류하지 못한 예외를 던지면 UNKNOWN 이고 다시 보내지 않는다 (블로킹 process 와 같다)`() {
        val t = received()
        val calls = AtomicInteger()
        val throwing = object : SuspendBankTransferPort {
            override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
                calls.incrementAndGet()
                throw IllegalStateException("boom")
            }
        }

        consume(listener(throwing), record(t.id))
        consume(listener(throwing), record(t.id))   // 같은 메시지가 다시 와도

        assertEquals(TransferStatus.UNKNOWN, store.transfers.getValue(t.id).status)
        assertEquals(1, calls.get())
    }

    @Test
    fun `없는 출금 · 이미 처리된 출금은 은행을 부르지 않는다`() {
        val bank = LoopBank(BankTransferOutcome.Completed)
        val t = received()
        consume(listener(bank), record(t.id))

        consume(listener(bank), record(t.id))               // 이미 COMPLETED
        consume(listener(bank), record(UUID.randomUUID()))  // 없는 출금

        assertEquals(1, bank.calls.get())
    }

    @Test
    fun `key 가 UUID 가 아니면 InvalidTransferKeyException - 재시도 없이 DLT`() {
        val bank = LoopBank(BankTransferOutcome.Completed)

        assertFailsWith<InvalidTransferKeyException> {
            consume(listener(bank), ConsumerRecord(TransferTopics.REQUESTED, 0, 0L, "not-a-uuid", "{}"))
        }
        assertEquals(0, bank.calls.get())
    }

    @Test
    fun `DB 호출(begin · finish)은 은행 응답을 완료한 스레드가 아니라 db 디스패처에서 돈다 - 이벤트 루프를 막지 않는다`() {
        val bank = LoopBank(BankTransferOutcome.Completed)
        // 전제: Unconfined 에서 이 가짜 은행을 기다리면 응답을 완료한 스레드에서 이어진다 (게이트웨이의 Netty 이벤트 루프와 같은 상황)
        val resumedOn = runBlocking {
            withContext(Dispatchers.Unconfined) {
                bank.transfer("090", "1", 1, "m", "r")
                Thread.currentThread().name.substringBefore(" @")   // 코루틴 디버그 모드(-ea)는 스레드 이름에 " @coroutine#n" 을 붙인다
            }
        }
        assertEquals(BANK_LOOP, resumedOn)

        val t = received()
        store.callThreads.clear()
        consume(listener(bank), record(t.id))

        assertEquals(TransferStatus.COMPLETED, store.transfers.getValue(t.id).status)
        assertTrue(store.callThreads.isNotEmpty())
        assertTrue(store.callThreads.all { it.startsWith("test-db-") }, "DB 를 부른 스레드: ${store.callThreads}")
    }

    @Test
    fun `은행 동시 호출은 세마포어 상한을 넘지 않는다 - 나머지는 자리를 기다린다`() {
        val gate = CompletableDeferred<Unit>()
        val inBank = AtomicInteger()
        val maxInBank = AtomicInteger()
        val bank = object : SuspendBankTransferPort {
            override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
                maxInBank.accumulateAndGet(inBank.incrementAndGet()) { a, b -> maxOf(a, b) }
                try {
                    gate.await()
                } finally {
                    inBank.decrementAndGet()
                }
                return BankTransferOutcome.Completed
            }
        }
        val transfers = (1..10).map { received() }
        val listener = listener(bank, permits = 2)

        runBlocking {
            val jobs = transfers.map { t -> launch(Dispatchers.Unconfined) { listener.on(record(t.id)) } }
            withTimeout(5_000) { while (inBank.get() < 2) delay(10) }
            delay(300)   // 상한이 없다면 나머지 8건도 이 사이에 은행에 들어온다
            assertEquals(2, maxInBank.get())
            gate.complete(Unit)
            jobs.joinAll()
        }

        assertEquals(2, maxInBank.get())
        assertTrue(transfers.all { store.transfers.getValue(it.id).status == TransferStatus.COMPLETED })
    }

    @Test
    fun `은행 응답을 기다리는 중에 취소되면 아무것도 기록하지 않고 취소를 다시 던진다 - SENDING 으로 남는다`() {
        val t = received()
        val called = CompletableDeferred<Unit>()
        val never = object : SuspendBankTransferPort {
            override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
                called.complete(Unit)
                awaitCancellation()
            }
        }
        var thrown: Throwable? = null

        runBlocking {
            val job = launch(Dispatchers.Unconfined) {
                try {
                    listener(never).on(record(t.id))
                } catch (e: Throwable) {
                    thrown = e
                    throw e
                }
            }
            withTimeout(5_000) { called.await() }
            job.cancelAndJoin()
        }

        assertIs<CancellationException>(thrown)
        assertEquals(TransferStatus.SENDING, store.transfers.getValue(t.id).status)   // 정리 작업이 30초 뒤 UNKNOWN → 결과조회로 확정한다
        assertNull(store.transfers.getValue(t.id).bankResponseCode)
    }

    @Test
    fun `CONFLICT 면 블로킹 판과 같은 WARN 알림 하나 - 이체 id 만 싣는다`() {
        val alerts = mutableListOf<Alert>()
        val t = received()
        val racing = object : SuspendBankTransferPort {
            override suspend fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String, reference: String): BankTransferOutcome {
                store.transition(t.id, TransferStatus.SENDING, TransferStatus.UNKNOWN, now)
                store.fail(t.id, TransferStatus.UNKNOWN, now, "2001")   // 결과조회가 먼저 FAILED 로 확정
                return BankTransferOutcome.Completed                      // 은행의 늦은 응답은 완료
            }
        }

        consume(listener(racing, alerts = AlertNotifier { alerts += it }), record(t.id))

        val alert = alerts.single()
        assertEquals(AlertLevel.WARN, alert.level)
        assertTrue(alert.lines.any { t.id.toString() in it })
    }

    @Test
    fun `DLT 는 블로킹 판과 같은 공용 코드 - 이 리스너의 로거로 남기고 ack 한다`() {
        val ack = CountingAck()
        val id = UUID.randomUUID().toString()
        val dlt = ConsumerRecord(TransferTopics.DLT, 1, 7L, id, "{}").apply {
            headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN, RetryLaterException::class.java.name.toByteArray())
        }

        val logs = captureLogs(SuspendingTransferRequestedListener::class.java) { listener(LoopBank(BankTransferOutcome.Completed)).onDlt(dlt, ack) }

        assertEquals(1, ack.count)
        assertEquals(Level.WARN, logs.single().level)
        assertTrue(logs.single().formattedMessage.contains("reconcile will republish"))
    }

    @Test
    fun `두 리스너의 재시도 토픽 · 리스너 설정이 같다 - 같은 토픽 · 그룹 · 재시도 단계`() {
        val blocking = TransferRequestedListener::class.java.getMethod("on", ConsumerRecord::class.java, Acknowledgment::class.java)
        val suspending = SuspendingTransferRequestedListener::class.java.getMethod("on", ConsumerRecord::class.java, Continuation::class.java)

        assertEquals(blocking.getAnnotation(RetryableTopic::class.java), suspending.getAnnotation(RetryableTopic::class.java))
        assertEquals(blocking.getAnnotation(KafkaListener::class.java), suspending.getAnnotation(KafkaListener::class.java))
    }

    private companion object {
        const val BANK_LOOP = "fake-bank-loop"
    }
}
