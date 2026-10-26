package io.github.hyeokjinon.banktransfer.account

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class AccountVerificationServiceTest {

    /** 은행 대신 쓰는 가짜. 보낸 이체를 기록해 두고, 사용자가 통장에서 적요를 보는 것처럼 꺼내 본다. */
    private class FakeBank(private val accounts: Map<String, String>) : BankPort {
        val transfers = mutableListOf<Pair<String, String>>()
        override fun inquireHolderName(bankCode: String, accountNo: String) = CompletableFuture.completedFuture(accounts[accountNo])
        override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> {
            transfers += accountNo to memo
            return CompletableFuture.completedFuture(Unit)
        }
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun getZone() = ZoneOffset.UTC
        override fun withZone(zone: java.time.ZoneId?) = this
        override fun instant() = now
    }

    private val bank = FakeBank(mapOf("1002123456789" to "홍길동"))
    private val clock = MutableClock(Instant.parse("2026-10-01T00:00:00Z"))
    private val repository = InMemoryVerificationRepository()
    private val service = AccountVerificationService(
        bank = bank,
        repository = repository,
        codeGenerator = { "1234" },
        clock = clock,
        ttl = Duration.ofMinutes(5),
        maxAttempts = 3,
    )

    private fun request(name: String = "홍길동", accountNo: String = "1002123456789", userId: String = "user-1") =
        service.request(RequestVerification(userId, "090", accountNo, name))

    private fun <T> CompletableFuture<T>.failure(): Throwable =
        assertFailsWith<ExecutionException> { get() }.cause!!

    @Test
    fun `예금주가 맞으면 코드를 적요에 실어 1원을 보낸다`() {
        val v = request().get()

        assertEquals(VerificationStatus.SENT, v.status)
        assertEquals(listOf("1002123456789" to "BT1234"), bank.transfers)
        assertEquals(clock.now.plus(Duration.ofMinutes(5)), v.expiresAt)
    }

    @Test
    fun `예금주명 사이 공백은 무시하고 비교한다`() {
        assertEquals(VerificationStatus.SENT, request(name = "홍 길동").get().status)
    }

    @Test
    fun `예금주명이 다르면 1원을 보내지 않는다`() {
        assertIs<AccountVerificationException.HolderNameMismatch>(request(name = "김철수").failure())
        assertEquals(emptyList(), bank.transfers)
    }

    @Test
    fun `없는 계좌면 AccountNotFound`() {
        assertIs<AccountVerificationException.AccountNotFound>(request(accountNo = "0000").failure())
    }

    @Test
    fun `맞는 코드를 넣으면 VERIFIED`() {
        val v = request().get()

        assertEquals(VerificationStatus.VERIFIED, service.confirm("user-1", v.id, "1234").status)
    }

    @Test
    fun `틀리면 남은 횟수를 알려 주고, 다 쓰면 잠긴다`() {
        val v = request().get()

        assertEquals(2, assertFailsWith<AccountVerificationException.WrongCode> { service.confirm("user-1", v.id, "0000") }.remainingAttempts)
        assertEquals(1, assertFailsWith<AccountVerificationException.WrongCode> { service.confirm("user-1", v.id, "0000") }.remainingAttempts)
        assertFailsWith<AccountVerificationException.Locked> { service.confirm("user-1", v.id, "0000") }
        assertFailsWith<AccountVerificationException.Locked> { service.confirm("user-1", v.id, "1234") }
    }

    @Test
    fun `유효시간이 지나면 맞는 코드여도 만료`() {
        val v = request().get()
        clock.now = clock.now.plus(Duration.ofMinutes(5))

        assertFailsWith<AccountVerificationException.Expired> { service.confirm("user-1", v.id, "1234") }
    }

    @Test
    fun `없는 인증 건은 VerificationNotFound`() {
        assertFailsWith<AccountVerificationException.VerificationNotFound> { service.confirm("user-1", UUID.randomUUID(), "1234") }
    }

    @Test
    fun `인증을 마친 계좌만 existsVerified 가 true`() {
        val v = request().get()
        assertEquals(false, repository.existsVerified("user-1", "090", "1002123456789"))

        service.confirm("user-1", v.id, "1234")

        assertEquals(true, repository.existsVerified("user-1", "090", "1002123456789"))
        assertEquals(false, repository.existsVerified("user-1", "090", "3333012345678"))
        assertEquals(false, repository.existsVerified("user-1", "004", "1002123456789"))
        assertEquals(false, repository.existsVerified("user-2", "090", "1002123456789"))   // 남이 인증한 계좌
    }

    @Test
    fun `인증 요청에 요청한 사용자를 남긴다`() {
        assertEquals("user-1", request().get().userId)
    }

    @Test
    fun `남의 인증 건은 확인할 수 없다 - VerificationNotFound 이고 실패 횟수도 늘지 않는다`() {
        val v = request(userId = "user-1").get()

        assertFailsWith<AccountVerificationException.VerificationNotFound> { service.confirm("user-2", v.id, "1234") }
        assertFailsWith<AccountVerificationException.VerificationNotFound> { service.confirm("user-2", v.id, "0000") }

        val after = repository.findById(v.id)!!
        assertEquals(VerificationStatus.SENT, after.status)
        assertEquals(0, after.failedAttempts)                                     // 남이 시도 횟수를 써 버려 잠기게 할 수 없다
        assertEquals(VerificationStatus.VERIFIED, service.confirm("user-1", v.id, "1234").status)
    }
}
