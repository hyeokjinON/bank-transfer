package io.github.hyeokjinon.banktransfer.account

import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.util.HexFormat
import java.util.UUID

/**
 * 1원인증 요청의 판정 (P6-2). 네 판(MVC 블로킹 · 비동기, WebFlux 코루틴 · Reactor)이 같이 쓴다 — 판마다 다른 것은 은행을 기다리는 방법과
 * 저장을 어느 스레드에서 하는가뿐이다. 그래서 여기는 스레드 · I/O 를 모르는 순수 함수만 둔다 (P6-1 의 begin/finish 와 같은 원칙).
 */
class VerificationRequestRules(
    private val codeGenerator: CodeGenerator = RandomCodeGenerator(),
    private val clock: Clock = Clock.systemUTC(),
    private val ttl: Duration = Duration.ofMinutes(5),
    private val memoPrefix: String = "BT",
) {

    /** 성명조회 결과 [found] 와 입력 [input] 을 비교한다. 공백은 무시한다. 같으면 은행이 준 예금주명 (저장값). */
    fun checkHolder(found: String?, input: String): String = when {
        found == null -> throw AccountVerificationException.AccountNotFound()
        normalize(found) != normalize(input) -> throw AccountVerificationException.HolderNameMismatch()
        else -> found
    }

    /** 1원에 실을 코드와 저장할 해시. 코드 원문은 적요로만 나가고 저장하지 않는다. */
    fun newDraft(): VerificationDraft {
        val id = UUID.randomUUID()
        val code = codeGenerator.generate()
        return VerificationDraft(id, verificationCodeHash(id, code), memoPrefix + code)
    }

    /** 1원을 보낸 뒤 저장할 인증 건. 만료는 지금(발급 시각)부터 ttl. */
    fun issue(draft: VerificationDraft, command: RequestVerification, holderName: String) = AccountVerification(
        id = draft.id,
        userId = command.userId,
        bankCode = command.bankCode,
        accountNo = command.accountNo,
        holderName = holderName,
        codeHash = draft.codeHash,
        status = VerificationStatus.SENT,
        failedAttempts = 0,
        expiresAt = clock.instant().plus(ttl),
    )

    private fun normalize(name: String) = name.filterNot { it.isWhitespace() }
}

/** 1원인증 초안. [memo] 는 인증코드를 담으므로 [toString] 에 넣지 않는다 (로그 · 예외 메시지로 새지 않게). */
class VerificationDraft(val id: UUID, val codeHash: String, val memo: String) {
    override fun toString() = "VerificationDraft(id=$id)"
}

/** 코드 해시. 요청(발급)과 확인이 같은 식을 써야 한다. */
internal fun verificationCodeHash(id: UUID, code: String): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("$id:$code".toByteArray()))
