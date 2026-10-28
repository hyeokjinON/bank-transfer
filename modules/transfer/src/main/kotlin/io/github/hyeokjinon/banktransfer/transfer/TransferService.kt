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
 * 2. 이 사용자의 같은 키가 이미 있으면 내용이 같을 때만 그 건을 돌려준다 (멱등, 사용자 범위 — P4-1). 차감은 없다
 * 3. 받는 계좌가 **이 사용자의** 1원인증을 마쳤는지 확인
 * 4. 이체(RECEIVED) · 잔고 차감 · TransferRequested 이벤트를 함께 저장 → 잔고가 모자라면 422 (출금 자체를 받지 않는다).
 *    잔고를 지금 잡지 못하면 WalletBusy, 동시 변경으로 포기하면 ConcurrentUpdate — 둘 다 다시 보내면 될 수 있다 (P4-2). 은행 처리는 컨슈머가 한다
 *
 * 2번의 "먼저 조회"는 빠른 길일 뿐이다. 동시에 들어온 같은 키는 DB UNIQUE 가 막고,
 * 진 쪽은 [StoreConflict.DuplicateIdempotencyKey] 를 받아 이긴 쪽 건을 돌려준다 (진 쪽의 차감도 함께 롤백된다).
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
        store.findByIdempotencyKey(command.userId, command.idempotencyKey)?.let { return replay(it, hash) }
        if (!verifiedAccounts.isVerified(command.userId, command.bankCode, command.accountNo)) {
            throw TransferException.AccountNotVerified()
        }

        repeat(maxReferenceAttempts) {
            val now = clock.instant()
            val transfer = Transfer(
                id = UUID.randomUUID(),
                userId = command.userId,
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
                val existing = store.findByIdempotencyKey(command.userId, command.idempotencyKey) ?: throw e
                return replay(existing, hash)
            } catch (e: StoreConflict.InsufficientBalance) {
                // 잔고가 모자라면 출금 자체를 받지 않는다 (출금 기록 · 이벤트도 함께 롤백됐다). 다시 시도해도 같다
                throw TransferException.InsufficientBalance()
            } catch (e: StoreConflict.WalletBusy) {
                // 락을 못 잡았다 (어떤 락인지는 저장소만 안다). 아무것도 남지 않았고, 같은 요청을 다시 보내면 될 수 있다 → 503 + Retry-After
                throw TransferException.WalletBusy()
            } catch (e: StoreConflict.ConcurrentUpdate) {
                // 낙관적 재시도를 다 썼다. 거래번호 재시도와 달리 여기서 또 돌리지 않는다 — 저장소가 이미 정한 횟수만큼 했다 → 409 + Retry-After
                throw TransferException.ConcurrentUpdate()
            } catch (conflict: StoreConflict.DuplicateReference) {
                // 무작위 거래번호가 우연히 겹쳤다. 새 번호로 다시 시도한다.
            }
        }
        error("could not issue a unique reference in $maxReferenceAttempts attempts")
    }

    /** 요청자의 출금만 보인다 (P4-1). 남의 출금 · 사용자 없는 P3 시절 출금은 없는 것과 같다 — 존재 여부도 숨긴다. */
    fun get(userId: String, id: UUID): Transfer =
        store.findById(id)?.takeIf { it.userId == userId } ?: throw TransferException.TransferNotFound(id)

    private fun replay(existing: Transfer, hash: String): TransferResult =
        if (existing.requestHash == hash) TransferResult(existing, created = false)
        else throw TransferException.IdempotencyKeyReused()

    private fun validate(command: RequestTransfer) {
        // 키는 DB 에서 ascii_bin 으로 바이트 단위 비교한다 (대소문자 · 악센트가 다르면 다른 키).
        // 비 ASCII 는 HTTP 헤더에서도 깨지므로 보이는 ASCII(공백 제외) 1~64자만 받는다.
        if (!IDEMPOTENCY_KEY.matches(command.idempotencyKey)) throw TransferException.IdempotencyKeyRequired()
        // 계좌 형식은 숫자만 허용한다. 해시가 "|" 로 이어 붙이는데 memo 가 마지막 필드라,
        // 계좌번호에 "|" 가 섞이면 서로 다른 요청이 같은 해시가 된다. 웹 계층 검증에만 기대지 않고 도메인에서 막는다.
        if (!BANK_CODE.matches(command.bankCode) || !ACCOUNT_NO.matches(command.accountNo)) {
            throw TransferException.InvalidAccount()
        }
        if (command.amount !in 1..MAX_TRANSFER_AMOUNT) throw TransferException.InvalidAmount(command.amount)
        if (!fitsMemo(command.memo)) throw TransferException.InvalidMemo()
    }

    companion object {
        private const val MEMO_MAX_BYTES = 20
        private val EUC_KR: Charset = Charset.forName("EUC-KR")
        private val IDEMPOTENCY_KEY = Regex("[\\x21-\\x7E]{1,64}")
        private val BANK_CODE = Regex("\\d{3}")
        private val ACCOUNT_NO = Regex("\\d{10,16}")

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
