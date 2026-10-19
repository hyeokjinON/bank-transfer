package io.github.hyeokjinon.banktransfer.transfer

import java.nio.charset.Charset
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.HexFormat
import java.util.UUID

/**
 * 출금이체 접수.
 *
 * 1. 형식 검증 (키 · 금액 · 적요)
 * 2. 같은 키가 이미 있으면 내용이 같을 때만 그 건을 돌려준다 (멱등)
 * 3. 받는 계좌가 1원인증을 마쳤는지 확인
 * 4. 이체(RECEIVED)와 TransferRequested 이벤트를 함께 저장 → 은행 처리는 P3-2 컨슈머가 한다
 *
 * 2번의 "먼저 조회"는 빠른 길일 뿐이다. 동시에 들어온 같은 키는 DB UNIQUE 가 막고,
 * 진 쪽은 [StoreConflict.DuplicateIdempotencyKey] 를 받아 이긴 쪽 건을 돌려준다.
 */
class TransferService(
    private val store: TransferStore,
    private val verifiedAccounts: VerifiedAccounts,
    private val referenceGenerator: ReferenceGenerator,
    private val clock: Clock = Clock.systemUTC(),
    private val maxReferenceAttempts: Int = 3,
) {

    fun request(command: RequestTransfer): TransferResult {
        validate(command)
        val hash = requestHash(command)
        store.findByIdempotencyKey(command.idempotencyKey)?.let { return replay(it, hash) }
        if (!verifiedAccounts.isVerified(command.bankCode, command.accountNo)) throw TransferException.AccountNotVerified()

        repeat(maxReferenceAttempts) {
            val now = clock.instant()
            val transfer = Transfer(
                id = UUID.randomUUID(),
                idempotencyKey = command.idempotencyKey,
                reference = referenceGenerator.next(),
                requestHash = hash,
                bankCode = command.bankCode,
                accountNo = command.accountNo,
                amount = command.amount,
                memo = command.memo,
                status = TransferStatus.RECEIVED,
                createdAt = now,
                updatedAt = now,
            )
            try {
                val saved = store.saveWithEvent(transfer, TransferEvent.TransferRequested(transfer.id, now))
                return TransferResult(saved, created = true)
            } catch (e: StoreConflict.DuplicateIdempotencyKey) {
                val existing = store.findByIdempotencyKey(command.idempotencyKey) ?: throw e
                return replay(existing, hash)
            } catch (conflict: StoreConflict.DuplicateReference) {
                // 무작위 거래번호가 우연히 겹쳤다. 새 번호로 다시 시도한다.
            }
        }
        error("could not issue a unique reference in $maxReferenceAttempts attempts")
    }

    fun get(id: UUID): Transfer = store.findById(id) ?: throw TransferException.TransferNotFound(id)

    private fun replay(existing: Transfer, hash: String): TransferResult =
        if (existing.requestHash == hash) TransferResult(existing, created = false)
        else throw TransferException.IdempotencyKeyReused()

    private fun validate(command: RequestTransfer) {
        if (command.idempotencyKey.isBlank() || command.idempotencyKey.length > MAX_KEY_LENGTH) {
            throw TransferException.IdempotencyKeyRequired()
        }
        if (command.amount !in 1..MAX_TRANSFER_AMOUNT) throw TransferException.InvalidAmount(command.amount)
        if (!fitsMemo(command.memo)) throw TransferException.InvalidMemo()
    }

    companion object {
        private const val MAX_KEY_LENGTH = 64
        private const val MEMO_MAX_BYTES = 20
        private val EUC_KR: Charset = Charset.forName("EUC-KR")

        /** 은행 전문의 적요 필드는 EUC-KR 20바이트다. 인코딩할 수 없는 문자(이모지 등)는 받지 않는다. */
        private fun fitsMemo(memo: String): Boolean =
            EUC_KR.newEncoder().canEncode(memo) && memo.toByteArray(EUC_KR).size <= MEMO_MAX_BYTES

        /** 같은 키에 다른 내용인지 판단하는 기준. 필드 순서와 구분자를 고정한다. */
        internal fun requestHash(command: RequestTransfer): String =
            sha256("${command.bankCode}|${command.accountNo}|${command.amount}|${command.memo}")

        private fun sha256(value: String): String =
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8)))
    }
}

/**
 * 이용기관 거래번호 20자: 접두(1) + KST 날짜 yyMMdd(6) + 대문자·숫자 13자.
 * 날짜를 앞에 두면 은행 · 대사에서 거래일을 바로 읽을 수 있다. 무작위 부분은 36^13 가지라 충돌은 드물고,
 * 겹쳐도 DB UNIQUE 가 잡아 [TransferService] 가 다시 발급한다.
 */
class RandomReferenceGenerator(
    private val prefix: Char = 'T',
    private val clock: Clock = Clock.systemUTC(),
    private val random: SecureRandom = SecureRandom(),
) : ReferenceGenerator {

    override fun next(): String {
        val date = DATE.format(clock.instant().atZone(KST))
        val tail = CharArray(RANDOM_LENGTH) { ALPHABET[random.nextInt(ALPHABET.length)] }
        return "$prefix$date${String(tail)}"
    }

    private companion object {
        val KST: ZoneId = ZoneId.of("Asia/Seoul")
        val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyMMdd")
        const val ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        const val RANDOM_LENGTH = 13
    }
}
