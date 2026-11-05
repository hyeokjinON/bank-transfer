package io.github.hyeokjinon.banktransfer.account

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class VerificationRequestRulesTest {

    private val now = Instant.parse("2026-11-05T00:00:00Z")
    private val rules = VerificationRequestRules(
        codeGenerator = { "1234" },
        clock = Clock.fixed(now, ZoneOffset.UTC),
        ttl = Duration.ofMinutes(5),
    )
    private val command = RequestVerification("user-1", "090", "1002123456789", "홍 길동")

    @Test
    fun `예금주가 없으면 AccountNotFound`() {
        assertFailsWith<AccountVerificationException.AccountNotFound> { rules.checkHolder(null, "홍길동") }
    }

    @Test
    fun `공백을 빼고 비교한다 - 같으면 은행이 준 예금주명을 돌려준다`() {
        assertEquals("홍길동", rules.checkHolder("홍길동", " 홍 길 동 "))
    }

    @Test
    fun `이름이 다르면 HolderNameMismatch`() {
        assertFailsWith<AccountVerificationException.HolderNameMismatch> { rules.checkHolder("김카뱅", "홍길동") }
    }

    @Test
    fun `초안 - 적요는 접두 + 코드, 해시는 id 와 코드로 만든다`() {
        val draft = rules.newDraft()

        assertEquals("BT1234", draft.memo)
        assertEquals(verificationCodeHash(draft.id, "1234"), draft.codeHash)
        assertEquals(64, draft.codeHash.length)
    }

    @Test
    fun `초안의 문자열 표현에는 적요(인증코드)가 없다 - 로그에 새지 않게`() {
        val draft = rules.newDraft()

        assertFalse("1234" in draft.toString(), draft.toString())
    }

    @Test
    fun `발급 - SENT 이고 만료는 발급 시각 + ttl, 요청자를 남긴다`() {
        val draft = rules.newDraft()

        val v = rules.issue(draft, command, "홍길동")

        assertEquals(draft.id, v.id)
        assertEquals("user-1", v.userId)
        assertEquals("090", v.bankCode)
        assertEquals("1002123456789", v.accountNo)
        assertEquals("홍길동", v.holderName)
        assertEquals(draft.codeHash, v.codeHash)
        assertEquals(VerificationStatus.SENT, v.status)
        assertEquals(0, v.failedAttempts)
        assertEquals(now.plus(Duration.ofMinutes(5)), v.expiresAt)
    }
}
