# P3-1 출금이체 접수 — 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /api/v1/transfers` 로 출금이체를 멱등하게 접수(RECEIVED)하고, 같은 트랜잭션에 아웃박스 이벤트를 남긴다. 1원인증 저장소는 MySQL 로 옮겨 "인증된 계좌만 출금"을 검증한다.

**Architecture:** 순수 Kotlin 도메인 모듈 `modules/transfer` 가 포트(`TransferStore`, `VerifiedAccounts`, `ReferenceGenerator`)만 선언하고, `apps/api` 가 JPA 어댑터 · REST 컨트롤러 · 조립을 맡는다. 스키마는 Flyway, 시각은 KST `DATETIME(6)`. 통합 테스트는 Testcontainers MySQL 을 쓰고, 모든 Spring 통합 테스트가 하나의 컨텍스트(컨테이너 1개)를 공유한다.

**Tech Stack:** Kotlin 2.3.21 · Java 21 · Spring Boot 4.1.1 (WebMVC, Data JPA/Hibernate 7.4, Flyway 12) · MySQL 8.4 · Testcontainers 2.0 · JUnit 5

**Spec:** [docs/superpowers/specs/2026-10-19-p3-transfer-design.md](../specs/2026-10-19-p3-transfer-design.md) (§1 모듈, §2 데이터, §3 상태, §4 접수 API, §7 테스트, §8 완료 기준) · [docs/database.md](../../database.md)

## Global Constraints

- 도메인 모듈(`modules/account`, `modules/transfer`)은 **Spring · JPA · Netty · Kafka 의존성 없음**. build 파일 `dependencies { }` 가 비어 있어야 한다.
- 스키마는 **Flyway 만** 바꾼다. JPA `ddl-auto: validate`.
- DB 시각은 **KST 벽시계 `DATETIME(6)`**. 도메인은 `Instant`. 필요한 설정 4개(서버 `+09:00`, JDBC `connectionTimeZone=Asia/Seoul&forceConnectionTimeZoneToSession=true`, `hibernate.jdbc.time_zone: Asia/Seoul`, `hibernate.type.preferred_instant_jdbc_type: TIMESTAMP`). 마지막 설정이 없으면 Hibernate 7 이 Instant 를 UTC 로 쓴다 (계획 작성 시 스파이크로 확인).
- 금액 `Long` 원 단위, 1 ~ 10,000,000. 적요는 EUC-KR 20바이트 이내. `Idempotency-Key` 1~64자 필수.
- 오류 코드: `IDEMPOTENCY_KEY_REQUIRED`(400) · `INVALID_AMOUNT`(400) · `INVALID_MEMO`(400) · `ACCOUNT_NOT_VERIFIED`(422) · `IDEMPOTENCY_KEY_REUSED`(422) · `TRANSFER_NOT_FOUND`(404).
- 이용기관 거래번호: `T` + KST `yyMMdd` + `[0-9A-Z]` 13자 = 20자.
- 주석 · 커밋은 한국어, 주석은 "왜"를 쓴다. 커밋 `type(scope): 요약` + 끝에 `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- 예전 회사 코드 · 식별자를 쓰지 않는다.
- Windows 에서 테스트는 Docker Desktop 이 켜져 있어야 한다 (Testcontainers). Git Bash 에서 `docker` 가 PATH 에 없으면 `export PATH="/c/Users/user/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`.

## 파일 구조

| 파일 | 책임 |
|---|---|
| `modules/transfer/build.gradle.kts` | 의존성 없는 도메인 모듈 |
| `modules/transfer/src/main/kotlin/.../transfer/Transfer.kt` | 상태 · 엔티티 · 명령 · 이벤트 · 포트 · 예외 |
| `modules/transfer/src/main/kotlin/.../transfer/TransferService.kt` | 접수 규칙(검증 · 멱등 · 인증 계좌 · 거래번호 재시도), 거래번호 생성기 |
| `modules/account/.../AccountVerification.kt` (수정) | `VerificationRepository.existsVerified` 추가 |
| `apps/api/build.gradle.kts` (수정) | JPA · Flyway · MySQL · Testcontainers, `plugin.jpa` |
| `apps/api/src/main/resources/application.yml` (수정) | 데이터소스 · JPA · 시간대 |
| `apps/api/src/main/resources/db/migration/V1__init.sql` | 테이블 3개 |
| `apps/api/src/main/kotlin/.../api/persistence/AccountVerificationPersistence.kt` | 1원인증 엔티티 · Spring Data 리포지토리 · `JpaVerificationRepository` |
| `apps/api/src/main/kotlin/.../api/persistence/TransferPersistence.kt` | 이체 · 아웃박스 엔티티 · 리포지토리 · `JpaTransferStore` |
| `apps/api/src/main/kotlin/.../api/web/TransferController.kt` | 접수 · 조회 API |
| `apps/api/src/main/kotlin/.../api/config/AppConfiguration.kt` (수정) | 조립, `BankGatewayAdapter` 가 앱 실행기에서 후속 처리 |
| `apps/api/src/main/kotlin/.../api/web/ApiExceptionHandler.kt` (수정) | 이체 예외 → HTTP |
| `apps/api/src/test/kotlin/.../api/support/` | `MySqlTestContainer`, `ApiIntegrationTest` (공유 컨텍스트) |
| `http/transfer.http`, `docs/adr/0004-*.md`, 문서들 | 손으로 확인 · 결정 기록 · 안내 |

---

### Task 1: transfer 도메인 모듈과 상태 모델

**Files:**
- Create: `modules/transfer/build.gradle.kts`
- Modify: `settings.gradle.kts`
- Create: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/Transfer.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferStatusTest.kt`

**Interfaces:**
- Produces: `TransferStatus`(`canMoveTo`, `isFinal`), `Transfer`(`moveTo`), `RequestTransfer`, `TransferResult`, `TransferEvent.TransferRequested`, `TransferStore`, `StoreConflict.{DuplicateIdempotencyKey, DuplicateReference}`, `VerifiedAccounts`, `ReferenceGenerator`, `TransferException.*`, `MAX_TRANSFER_AMOUNT`

- [ ] **Step 1: 모듈 등록**

`settings.gradle.kts` 의 `include(":modules:account")` 줄 아래에 추가:
```kotlin
include(":modules:transfer")        // 출금이체 도메인 (Spring·JPA·Kafka 의존 없음)
```

`modules/transfer/build.gradle.kts`:
```kotlin
plugins { `java-library` }

// account 와 같이 의존성이 없다. 저장 · 메시징 · 은행 연결은 포트(인터페이스)로만 안다.
dependencies { }
```

- [ ] **Step 2: 실패하는 테스트 작성**

`TransferStatusTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.COMPLETED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.FAILED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.RECEIVED
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.SENDING
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus.UNKNOWN
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransferStatusTest {

    @Test
    fun `설계 문서 3장의 전이만 허용한다`() {
        val allowed = setOf(
            RECEIVED to SENDING,
            SENDING to RECEIVED, SENDING to COMPLETED, SENDING to FAILED, SENDING to UNKNOWN,
            UNKNOWN to COMPLETED, UNKNOWN to FAILED,
        )
        for (from in TransferStatus.entries) {
            for (to in TransferStatus.entries) {
                assertEquals((from to to) in allowed, from.canMoveTo(to), "$from -> $to")
            }
        }
    }

    @Test
    fun `COMPLETED 와 FAILED 만 종결 상태다`() {
        assertEquals(setOf(COMPLETED, FAILED), TransferStatus.entries.filter { it.isFinal }.toSet())
    }

    @Test
    fun `SENDING 이 되면 sendingAt 을 기록한다`() {
        val at = Instant.parse("2026-10-19T01:00:00Z")
        val moved = transfer(RECEIVED).moveTo(SENDING, at)

        assertEquals(SENDING, moved.status)
        assertEquals(at, moved.sendingAt)
        assertEquals(at, moved.updatedAt)
    }

    @Test
    fun `금지된 전이는 IllegalTransition`() {
        assertFailsWith<TransferException.IllegalTransition> { transfer(RECEIVED).moveTo(COMPLETED, Instant.now()) }
        assertFailsWith<TransferException.IllegalTransition> { transfer(COMPLETED).moveTo(FAILED, Instant.now()) }
        assertTrue(transfer(UNKNOWN).status.canMoveTo(COMPLETED))
        assertFalse(transfer(FAILED).status.canMoveTo(SENDING))
    }

    private fun transfer(status: TransferStatus) = Transfer(
        id = UUID.randomUUID(),
        idempotencyKey = "key",
        reference = "T2610190000000000001",
        requestHash = "h",
        bankCode = "090",
        accountNo = "1002123456789",
        amount = 1000,
        memo = "출금",
        status = status,
        createdAt = Instant.parse("2026-10-19T00:00:00Z"),
        updatedAt = Instant.parse("2026-10-19T00:00:00Z"),
    )
}
```

- [ ] **Step 3: 실패 확인**

Run: `./gradlew :modules:transfer:test`
Expected: FAIL — `Unresolved reference 'TransferStatus'` 등 컴파일 오류

- [ ] **Step 4: 도메인 모델 구현**

`Transfer.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import java.time.Instant
import java.util.UUID

/** 1회 출금 한도 (원). */
const val MAX_TRANSFER_AMOUNT: Long = 10_000_000

/**
 * 출금이체 상태. 허용 전이는 설계 문서 3장과 같다.
 * 타임아웃은 실패가 아니라 UNKNOWN 이다: 은행이 처리했을 수 있으므로 다시 보내지 않고 결과조회로 확정한다(P3-3).
 */
enum class TransferStatus {
    RECEIVED, SENDING, COMPLETED, FAILED, UNKNOWN;

    val isFinal: Boolean get() = this == COMPLETED || this == FAILED

    fun canMoveTo(next: TransferStatus): Boolean = next in allowedNext()

    private fun allowedNext(): Set<TransferStatus> = when (this) {
        RECEIVED -> setOf(SENDING)
        // SENDING -> RECEIVED 는 "전문을 쓰기도 전에 연결이 없었다"(은행에 안 감)일 때만 쓴다 (P3-3 재시도)
        SENDING -> setOf(RECEIVED, COMPLETED, FAILED, UNKNOWN)
        UNKNOWN -> setOf(COMPLETED, FAILED)
        COMPLETED, FAILED -> emptySet()
    }
}

/** 출금이체 한 건. 서비스 모계좌 -> 1원인증을 마친 사용자 계좌. */
data class Transfer(
    val id: UUID,
    val idempotencyKey: String,
    /** 이용기관 거래번호 20자. 은행에 보내는 업무 키 (전문번호와 다르다). */
    val reference: String,
    /** 같은 키로 다른 내용을 보냈는지 판단하는 해시. */
    val requestHash: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
    val status: TransferStatus,
    val bankResponseCode: String? = null,
    val sendingAt: Instant? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    fun moveTo(next: TransferStatus, at: Instant): Transfer {
        if (!status.canMoveTo(next)) throw TransferException.IllegalTransition(status, next)
        return copy(status = next, updatedAt = at, sendingAt = if (next == TransferStatus.SENDING) at else sendingAt)
    }
}

data class RequestTransfer(
    val idempotencyKey: String,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
)

/** 접수 결과. created = false 면 같은 키로 이미 접수된 건을 그대로 돌려준 것이다. */
data class TransferResult(val transfer: Transfer, val created: Boolean)

sealed interface TransferEvent {
    val transferId: UUID
    val occurredAt: Instant

    data class TransferRequested(override val transferId: UUID, override val occurredAt: Instant) : TransferEvent
}

/**
 * 이체 저장 포트. 도메인은 "저장과 이벤트 기록은 함께 성공하거나 함께 실패한다"는 계약만 안다.
 * 구현(같은 DB 트랜잭션 + 아웃박스 테이블)은 apps/api 의 어댑터가 맡는다.
 */
interface TransferStore {
    /** UNIQUE 위반이면 [StoreConflict] 를 던진다. */
    fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer

    fun findById(id: UUID): Transfer?

    fun findByIdempotencyKey(key: String): Transfer?
}

/** 저장소가 UNIQUE 제약 위반을 도메인 말로 알려 줄 때 쓴다. */
sealed class StoreConflict(message: String) : RuntimeException(message) {
    class DuplicateIdempotencyKey(key: String) : StoreConflict("idempotency key '$key' already exists")
    class DuplicateReference(reference: String) : StoreConflict("reference '$reference' already exists")
}

/** "이 계좌는 1원인증을 마쳤나?" transfer 모듈은 account 모듈을 import 하지 않는다. 연결은 apps/api 에서 한다. */
fun interface VerifiedAccounts {
    fun isVerified(bankCode: String, accountNo: String): Boolean
}

fun interface ReferenceGenerator {
    fun next(): String
}

sealed class TransferException(message: String) : RuntimeException(message) {
    class IdempotencyKeyRequired : TransferException("Idempotency-Key header is required (1-64 chars)")
    class InvalidAmount(amount: Long) : TransferException("amount must be 1..$MAX_TRANSFER_AMOUNT, was $amount")
    class InvalidMemo : TransferException("memo must fit in 20 bytes of EUC-KR")
    class AccountNotVerified : TransferException("account is not verified")
    class IdempotencyKeyReused : TransferException("idempotency key was already used with a different request")
    class TransferNotFound(id: UUID) : TransferException("transfer $id not found")
    class IllegalTransition(from: TransferStatus, to: TransferStatus) : TransferException("cannot move from $from to $to")
}
```

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :modules:transfer:test`
Expected: PASS (4 tests)

- [ ] **Step 6: 커밋**

```bash
git add settings.gradle.kts modules/transfer
git commit -m "feat(transfer): 출금이체 도메인 모듈과 상태 전이 규칙

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 접수 서비스 (검증 · 멱등 · 인증 계좌 · 거래번호)

**Files:**
- Create: `modules/transfer/src/main/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferService.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/TransferServiceTest.kt`
- Test: `modules/transfer/src/test/kotlin/io/github/hyeokjinon/banktransfer/transfer/RandomReferenceGeneratorTest.kt`

**Interfaces:**
- Consumes: Task 1 의 모든 타입
- Produces: `TransferService(store, verifiedAccounts, referenceGenerator, clock = Clock.systemUTC(), maxReferenceAttempts = 3)` · `request(RequestTransfer): TransferResult` · `get(UUID): Transfer` · `TransferService.requestHash(RequestTransfer): String` (internal) · `RandomReferenceGenerator(prefix: Char = 'T', clock: Clock = Clock.systemUTC(), random: SecureRandom = SecureRandom()) : ReferenceGenerator`

- [ ] **Step 1: 실패하는 테스트 작성**

`TransferServiceTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TransferServiceTest {

    /** DB 대신 쓰는 가짜 저장소. UNIQUE 위반 상황(동시 요청 · 거래번호 충돌)을 흉내 낼 수 있다. */
    private class FakeStore : TransferStore {
        val transfers = linkedMapOf<UUID, Transfer>()
        val events = mutableListOf<TransferEvent>()

        /** 지정하면 다음 저장 직전에 "다른 요청이 먼저 같은 키로 저장한" 상황을 만든다. */
        var raceWinner: Transfer? = null

        /** 남은 횟수만큼 거래번호 충돌을 낸다. */
        var referenceConflicts = 0

        override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer {
            raceWinner?.let { winner ->
                raceWinner = null
                transfers[winner.id] = winner
                throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
            }
            if (referenceConflicts > 0) {
                referenceConflicts--
                throw StoreConflict.DuplicateReference(transfer.reference)
            }
            transfers[transfer.id] = transfer
            events += event
            return transfer
        }

        override fun findById(id: UUID) = transfers[id]

        override fun findByIdempotencyKey(key: String) = transfers.values.firstOrNull { it.idempotencyKey == key }
    }

    private val store = FakeStore()
    private val verified = mutableSetOf("090" to "1002123456789")
    private var referenceSeq = 0
    private val clock = Clock.fixed(Instant.parse("2026-10-19T01:00:00Z"), ZoneOffset.UTC)
    private val service = TransferService(
        store = store,
        verifiedAccounts = { bankCode, accountNo -> (bankCode to accountNo) in verified },
        referenceGenerator = { "T261019" + "%013d".format(++referenceSeq) },
        clock = clock,
    )

    private fun command(key: String = "key-1", amount: Long = 50_000, memo: String = "출금") =
        RequestTransfer(key, "090", "1002123456789", amount, memo)

    @Test
    fun `처음 요청은 RECEIVED 로 접수하고 이벤트를 함께 남긴다`() {
        val result = service.request(command())

        assertTrue(result.created)
        assertEquals(TransferStatus.RECEIVED, result.transfer.status)
        assertEquals("T2610190000000000001", result.transfer.reference)
        assertEquals(listOf<TransferEvent>(TransferEvent.TransferRequested(result.transfer.id, clock.instant())), store.events)
    }

    @Test
    fun `같은 키 같은 내용이면 기존 건을 돌려주고 새로 만들지 않는다`() {
        val first = service.request(command()).transfer
        val again = service.request(command())

        assertFalse(again.created)
        assertEquals(first.id, again.transfer.id)
        assertEquals(1, store.transfers.size)
        assertEquals(1, store.events.size)
    }

    @Test
    fun `같은 키에 다른 내용이면 IdempotencyKeyReused`() {
        service.request(command())

        assertFailsWith<TransferException.IdempotencyKeyReused> { service.request(command(amount = 60_000)) }
    }

    @Test
    fun `인증되지 않은 계좌면 접수하지 않는다`() {
        verified.clear()

        assertFailsWith<TransferException.AccountNotVerified> { service.request(command()) }
        assertTrue(store.transfers.isEmpty())
    }

    @Test
    fun `금액은 1원 이상 1천만원 이하`() {
        for (amount in listOf(0L, -1L, MAX_TRANSFER_AMOUNT + 1)) {
            assertFailsWith<TransferException.InvalidAmount> { service.request(command(key = "k$amount", amount = amount)) }
        }
        assertTrue(service.request(command(key = "max", amount = MAX_TRANSFER_AMOUNT)).created)
    }

    @Test
    fun `적요는 EUC-KR 20바이트까지, 인코딩할 수 없는 문자는 거절`() {
        assertTrue(service.request(command(key = "m1", memo = "가".repeat(10))).created)   // 20바이트
        assertFailsWith<TransferException.InvalidMemo> { service.request(command(key = "m2", memo = "가".repeat(11))) }
        assertFailsWith<TransferException.InvalidMemo> { service.request(command(key = "m3", memo = "😀")) }
    }

    @Test
    fun `키가 비었거나 64자를 넘으면 IdempotencyKeyRequired`() {
        assertFailsWith<TransferException.IdempotencyKeyRequired> { service.request(command(key = "")) }
        assertFailsWith<TransferException.IdempotencyKeyRequired> { service.request(command(key = "a".repeat(65))) }
    }

    @Test
    fun `같은 키가 동시에 들어와 저장에서 지면 이긴 쪽 건을 돌려준다`() {
        val now = clock.instant()
        val winner = Transfer(
            id = UUID.randomUUID(), idempotencyKey = "key-1", reference = "T2610199999999999999",
            requestHash = TransferService.requestHash(command()), bankCode = "090", accountNo = "1002123456789",
            amount = 50_000, memo = "출금", status = TransferStatus.RECEIVED, createdAt = now, updatedAt = now,
        )
        store.raceWinner = winner

        val result = service.request(command())

        assertFalse(result.created)
        assertEquals(winner.id, result.transfer.id)
        assertTrue(store.events.isEmpty())
    }

    @Test
    fun `거래번호가 겹치면 새 번호로 다시 시도한다`() {
        store.referenceConflicts = 1

        val result = service.request(command())

        assertTrue(result.created)
        assertEquals("T2610190000000000002", result.transfer.reference)
    }

    @Test
    fun `없는 id 는 TransferNotFound`() {
        assertFailsWith<TransferException.TransferNotFound> { service.get(UUID.randomUUID()) }
    }
}
```

`RandomReferenceGeneratorTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.transfer

import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class RandomReferenceGeneratorTest {

    // UTC 10-18 15:30 = KST 10-19 00:30 → 날짜는 KST 기준 261019
    private val clock = Clock.fixed(Instant.parse("2026-10-18T15:30:00Z"), ZoneOffset.UTC)

    @Test
    fun `접두 + KST 날짜 + 대문자·숫자 13자 = 20자`() {
        val reference = RandomReferenceGenerator(prefix = 'T', clock = clock).next()

        assertTrue(Regex("T261019[0-9A-Z]{13}").matches(reference), reference)
    }

    @Test
    fun `호출마다 다른 번호`() {
        val generator = RandomReferenceGenerator(clock = clock)

        assertNotEquals(generator.next(), generator.next())
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:transfer:test`
Expected: FAIL — `Unresolved reference 'TransferService'`, `'RandomReferenceGenerator'`

- [ ] **Step 3: 구현**

`TransferService.kt`:
```kotlin
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
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:transfer:test`
Expected: PASS (16 tests = 4 + 10 + 2)

- [ ] **Step 5: 커밋**

```bash
git add modules/transfer
git commit -m "feat(transfer): 출금이체 접수 서비스 — 멱등키, 인증 계좌 확인, 거래번호 발급

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: account — "인증된 계좌인가?" 조회

**Files:**
- Modify: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerification.kt` (`VerificationRepository`)
- Modify: `modules/account/src/main/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationService.kt` (`InMemoryVerificationRepository`)
- Test: `modules/account/src/test/kotlin/io/github/hyeokjinon/banktransfer/account/AccountVerificationServiceTest.kt`

**Interfaces:**
- Produces: `VerificationRepository.existsVerified(bankCode: String, accountNo: String): Boolean`

- [ ] **Step 1: 실패하는 테스트 작성**

`AccountVerificationServiceTest.kt` 에서 저장소를 필드로 꺼낸다. 기존
```kotlin
    private val service = AccountVerificationService(
        bank = bank,
        repository = InMemoryVerificationRepository(),
```
를 다음으로 바꾼다:
```kotlin
    private val repository = InMemoryVerificationRepository()
    private val service = AccountVerificationService(
        bank = bank,
        repository = repository,
```
그리고 클래스 끝(마지막 `}` 앞)에 추가:
```kotlin
    @Test
    fun `인증을 마친 계좌만 existsVerified 가 true`() {
        val v = request().get()
        assertEquals(false, repository.existsVerified("090", "1002123456789"))

        service.confirm(v.id, "1234")

        assertEquals(true, repository.existsVerified("090", "1002123456789"))
        assertEquals(false, repository.existsVerified("090", "3333012345678"))
        assertEquals(false, repository.existsVerified("004", "1002123456789"))
    }
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :modules:account:test`
Expected: FAIL — `Unresolved reference 'existsVerified'`

- [ ] **Step 3: 구현**

`AccountVerification.kt` 의 `interface VerificationRepository` 안, `update` 아래에 추가:
```kotlin

    /** 이 계좌로 끝난 인증(VERIFIED)이 있는가. 출금 받는 계좌 검증에 쓴다. */
    fun existsVerified(bankCode: String, accountNo: String): Boolean
```

`AccountVerificationService.kt` 의 `InMemoryVerificationRepository` 주석과 본문 끝에 추가 — 주석 `/** 1단계용 저장소. 다음 단계에서 JPA + MySQL 로 바꾼다. */` 를 `/** 도메인 테스트용 저장소. 앱은 MySQL 저장소(apps/api JpaVerificationRepository)를 쓴다. */` 로 바꾸고, `update` 아래에:
```kotlin

    override fun existsVerified(bankCode: String, accountNo: String): Boolean =
        store.values.any { it.bankCode == bankCode && it.accountNo == accountNo && it.status == VerificationStatus.VERIFIED }
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :modules:account:test`
Expected: PASS (9 tests)

- [ ] **Step 5: 커밋**

```bash
git add modules/account
git commit -m "feat(account): 인증 완료 계좌 조회 existsVerified 추가

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: MySQL · Flyway 기반과 공유 통합 테스트 환경

**Files:**
- Modify: `build.gradle.kts` (root, `plugin.jpa` 버전 선언)
- Modify: `apps/api/build.gradle.kts`
- Modify: `apps/api/src/main/resources/application.yml`
- Create: `apps/api/src/main/resources/db/migration/V1__init.sql`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/MySqlTestContainer.kt`
- Create: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`
- Modify: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/AccountVerificationApiTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt`

**Interfaces:**
- Produces: `ApiIntegrationTest` (abstract, `@SpringBootTest(RANDOM_PORT)` + MySQL 컨테이너 + 공유 `bank: MockBankServer`, `client: RestClient`, `waitForBankConnection()`), `MySqlTestContainer`

- [ ] **Step 1: 의존성과 플러그인**

`build.gradle.kts` (root) `plugins` 블록에 한 줄 추가:
```kotlin
    kotlin("plugin.jpa") version "2.3.21" apply false
```

`apps/api/build.gradle.kts` 전체를 다음으로 바꾼다:
```kotlin
plugins {
    kotlin("plugin.spring")
    kotlin("plugin.jpa")          // @Entity 에 인자 없는 생성자를 만들어 준다
    id("org.springframework.boot")
}

// Hibernate 가 엔티티를 프록시로 감쌀 수 있게 열어 둔다 (Kotlin 클래스는 기본이 final)
allOpen {
    annotation("jakarta.persistence.Entity")
}

dependencies {
    implementation(project(":modules:account"))
    implementation(project(":modules:transfer"))
    implementation(project(":modules:bank-gateway"))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-mysql")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation(project(":apps:mock-bank"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
}

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs("-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
}
```

- [ ] **Step 2: 설정**

`application.yml` 의 `spring:` 블록을 다음으로 바꾼다 (`bank`, `account`, `management` 는 그대로):
```yaml
spring:
  application:
    name: bank-transfer
  mvc:
    problemdetails:
      enabled: true   # 검증 실패(400)도 ProblemDetail 형식으로 응답
  datasource:
    # 로컬은 infra/docker-compose.yml 의 MySQL (값은 infra/.env.example 과 같다). 시각은 KST 로 저장한다 (docs/database.md §2)
    url: jdbc:mysql://${MYSQL_HOST:localhost}:${MYSQL_PORT:3306}/${MYSQL_DATABASE:bank_transfer}?connectionTimeZone=Asia/Seoul&forceConnectionTimeZoneToSession=true
    username: ${MYSQL_USER:bank}
    password: ${MYSQL_PASSWORD:local-dev-only}
  jpa:
    open-in-view: false        # 트랜잭션 밖(뷰 렌더링)에서 지연 로딩하지 않는다
    hibernate:
      ddl-auto: validate       # 스키마는 Flyway 만 바꾼다. 엔티티와 다르면 기동 실패
    properties:
      hibernate.jdbc.time_zone: Asia/Seoul
      # Hibernate 7 은 Instant 를 기본으로 UTC 로 쓴다. 위 time_zone 을 따르게 하려면 이 설정이 필요하다
      hibernate.type.preferred_instant_jdbc_type: TIMESTAMP
```

- [ ] **Step 3: 마이그레이션**

`apps/api/src/main/resources/db/migration/V1__init.sql`:
```sql
-- P3-1 초기 스키마. 정의와 이유는 docs/database.md. 이미 적용된 이 파일은 고치지 않는다 (변경은 V2__ 로).

CREATE TABLE account_verification (
  id              CHAR(36)     NOT NULL PRIMARY KEY,
  bank_code       VARCHAR(3)   NOT NULL,
  account_no      VARCHAR(16)  NOT NULL,
  holder_name     VARCHAR(20)  NOT NULL,
  code_hash       CHAR(64)     NOT NULL,
  status          VARCHAR(10)  NOT NULL,
  failed_attempts INT          NOT NULL,
  expires_at      DATETIME(6)  NOT NULL,
  verified_at     DATETIME(6)  NULL,
  created_at      DATETIME(6)  NOT NULL,
  INDEX idx_verification_account (bank_code, account_no, status)
);

CREATE TABLE transfer (
  id                 CHAR(36)     NOT NULL PRIMARY KEY,
  idempotency_key    VARCHAR(64)  NOT NULL,
  reference          CHAR(20)     NOT NULL,
  request_hash       CHAR(64)     NOT NULL,
  bank_code          VARCHAR(3)   NOT NULL,
  account_no         VARCHAR(16)  NOT NULL,
  amount             BIGINT       NOT NULL,
  memo               VARCHAR(20)  NOT NULL,
  status             VARCHAR(10)  NOT NULL,
  bank_response_code VARCHAR(4)   NULL,
  sending_at         DATETIME(6)  NULL,
  inquiry_attempts   INT          NOT NULL DEFAULT 0,
  created_at         DATETIME(6)  NOT NULL,
  updated_at         DATETIME(6)  NOT NULL,
  version            BIGINT       NOT NULL,
  UNIQUE KEY uk_transfer_idempotency (idempotency_key),
  UNIQUE KEY uk_transfer_reference (reference),
  INDEX idx_transfer_status (status, sending_at)
);

CREATE TABLE outbox_event (
  id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  aggregate_id  CHAR(36)     NOT NULL,
  event_type    VARCHAR(50)  NOT NULL,
  payload       JSON         NOT NULL,
  created_at    DATETIME(6)  NOT NULL,
  published_at  DATETIME(6)  NULL,
  INDEX idx_outbox_unpublished (published_at, id)
);
```

- [ ] **Step 4: 공유 테스트 환경**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/MySqlTestContainer.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.mysql.MySQLContainer

/**
 * 통합 테스트용 MySQL. infra/docker-compose.yml 과 같은 버전 · 문자셋 · 시간대로 띄운다.
 * @ServiceConnection 이 spring.datasource.* 를 이 컨테이너로 바꿔 준다.
 * URL 파라미터는 컨테이너 주소에 다시 붙여야 한다 (application.yml 의 url 은 덮어써진다).
 */
@TestConfiguration(proxyBeanMethods = false)
class MySqlTestContainer {

    @Bean
    @ServiceConnection
    fun mysql(): MySQLContainer = MySQLContainer("mysql:8.4")
        .withDatabaseName("bank_transfer")
        .withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_0900_ai_ci", "--default-time-zone=+09:00")
        .withUrlParam("connectionTimeZone", "Asia/Seoul")
        .withUrlParam("forceConnectionTimeZoneToSession", "true")
}
```

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/support/ApiIntegrationTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.support

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient

/**
 * 모든 Spring 통합 테스트의 부모. 설정이 같으므로 스프링 테스트 컨텍스트가 하나로 캐시되어
 * MySQL 컨테이너와 모의 은행도 한 번만 뜬다. 테스트끼리 DB 를 공유하므로 키 · 계좌번호는 테스트마다 새로 만든다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MySqlTestContainer::class)
abstract class ApiIntegrationTest {

    companion object {
        /** 9990000000001 은 응답하지 않는 계좌 (타임아웃 재현). 컨텍스트와 수명이 같으므로 닫지 않는다. */
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

    @BeforeEach
    fun waitForBankConnection() {
        val deadline = System.currentTimeMillis() + 5_000
        while (!gateway.isConnected) {
            check(System.currentTimeMillis() < deadline) { "gateway did not connect to mock bank" }
            Thread.sleep(20)
        }
    }
}
```

- [ ] **Step 5: 기존 API 테스트를 공유 환경으로 옮기기**

`AccountVerificationApiTest.kt` 의 클래스 선언부부터 `waitForBankConnection()` 끝까지(어노테이션, companion, port, gateway, client, `@BeforeEach`)를 다음으로 바꾼다. `mapType`, `post`, 테스트 4개는 그대로 둔다:
```kotlin
/**
 * 실제 HTTP -> Spring MVC -> 도메인 -> Netty 게이트웨이 -> 모의 은행(TCP) 까지 한 번에 검증한다.
 */
class AccountVerificationApiTest : ApiIntegrationTest() {

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}
```
import 정리: `ApiIntegrationTest` 추가(`io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest`), 쓰지 않게 된 `AfterAll`, `BeforeEach`, `Autowired`, `Value`, `SpringBootTest`, `DynamicPropertyRegistry`, `DynamicPropertySource`, `RestClient`, `BankGatewayClient`, `MockBankServer` import 삭제.

- [ ] **Step 6: 실패하는 테스트 작성**

`apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/DatabaseSetupTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.Test
import kotlin.test.assertEquals

class DatabaseSetupTest : ApiIntegrationTest() {

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @Test
    fun `Flyway 가 V1 테이블을 만들었다`() {
        val tables = jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() " +
                "AND table_name IN ('account_verification', 'transfer', 'outbox_event') ORDER BY table_name",
            String::class.java,
        )
        assertEquals(listOf("account_verification", "outbox_event", "transfer"), tables)
    }

    @Test
    fun `DB 세션 시간대는 KST`() {
        assertEquals("Asia/Seoul", jdbc.queryForObject("SELECT @@session.time_zone", String::class.java))
    }
}
```

- [ ] **Step 7: 실행 — 기존 테스트 포함 전부 통과 확인**

Run: `./gradlew :apps:api:test`
Expected: PASS (AccountVerificationApiTest 4 + DatabaseSetupTest 2). 첫 실행은 `mysql:8.4` 이미지를 내려받아 오래 걸릴 수 있다.
실패 시: `Could not find a valid Docker environment` → Docker Desktop 실행 확인. `Schema-validation` 오류 → 이 단계에서는 엔티티가 없으므로 나오면 안 된다.

- [ ] **Step 8: 커밋**

```bash
git add build.gradle.kts apps/api
git commit -m "feat(api): MySQL · Flyway V1 스키마, Testcontainers 공유 통합 테스트 환경

- 시각 KST 저장 설정 (JDBC · Hibernate jdbc.time_zone · preferred_instant_jdbc_type)
- 모든 Spring 통합 테스트가 ApiIntegrationTest 하나의 컨텍스트(MySQL 1개, 모의 은행 1개)를 공유

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 1원인증 저장소 MySQL 이전 + 은행 응답 후속 처리를 앱 실행기로

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/AccountVerificationPersistence.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaVerificationRepositoryTest.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/config/BankGatewayAdapterTest.kt`

**Interfaces:**
- Consumes: `VerificationRepository` (Task 3 포함), `ApiIntegrationTest` (Task 4)
- Produces: `AccountVerificationJpaRepository`, `JpaVerificationRepository(jpa, tx: TransactionTemplate, clock: Clock)`, `BankGatewayAdapter(client, executor: Executor)`

**왜 실행기를 바꾸나:** 1원인증의 `repository.save` 는 은행 응답 future 의 `thenApply` 안에서 돈다. 지금은 그 future 를 Netty 이벤트 루프가 완료하므로, JDBC 저장(블로킹)이 이벤트 루프에서 실행된다. 이벤트 루프가 막히면 같은 루프의 모든 은행 통신이 멈춘다. 어댑터가 `thenApplyAsync(…, executor)` 로 후속 처리를 앱 실행기(Spring `applicationTaskExecutor`)로 넘긴다.

- [ ] **Step 1: 실패하는 테스트 작성**

`JpaVerificationRepositoryTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JpaVerificationRepositoryTest : ApiIntegrationTest() {

    @Autowired
    private lateinit var repository: VerificationRepository

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    private fun newVerification(
        accountNo: String = "77" + Random.nextLong(10_000_000_000, 99_999_999_999),
        expiresAt: Instant = Instant.parse("2026-10-19T00:05:00.123456789Z"),
    ) = AccountVerification(
        id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
        codeHash = "a".repeat(64), status = VerificationStatus.SENT, failedAttempts = 0, expiresAt = expiresAt,
    )

    @Test
    fun `저장한 값과 다시 읽은 값이 같다 (마이크로초 단위)`() {
        val saved = repository.save(newVerification())

        assertEquals(saved, repository.findById(saved.id))
        assertEquals(Instant.parse("2026-10-19T00:05:00.123456Z"), saved.expiresAt)   // DATETIME(6) 에 맞춰 자른다
    }

    @Test
    fun `시각은 KST 벽시계로 저장된다`() {
        val saved = repository.save(newVerification(expiresAt = Instant.parse("2026-10-19T00:00:00Z")))

        val raw = jdbc.queryForObject(
            "SELECT DATE_FORMAT(expires_at, '%Y-%m-%d %H:%i:%s') FROM account_verification WHERE id = ?",
            String::class.java, saved.id.toString(),
        )
        assertEquals("2026-10-19 09:00:00", raw)
    }

    @Test
    fun `같은 건을 동시에 update 해도 시도 횟수가 정확하다 (FOR UPDATE)`() {
        val saved = repository.save(newVerification())
        val pool = Executors.newFixedThreadPool(10)
        val start = CountDownLatch(1)
        repeat(10) {
            pool.execute {
                start.await()
                repository.update(saved.id) { it.copy(failedAttempts = it.failedAttempts + 1) }
            }
        }
        start.countDown()
        pool.shutdown()
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))

        assertEquals(10, repository.findById(saved.id)!!.failedAttempts)
    }

    @Test
    fun `VERIFIED 가 된 계좌만 existsVerified`() {
        val saved = repository.save(newVerification())
        assertFalse(repository.existsVerified("090", saved.accountNo))

        repository.update(saved.id) { it.copy(status = VerificationStatus.VERIFIED, verifiedAt = Instant.now()) }

        assertTrue(repository.existsVerified("090", saved.accountNo))
        assertFalse(repository.existsVerified("004", saved.accountNo))
    }

    @Test
    fun `없는 id 는 update 가 null`() {
        assertEquals(null, repository.update(UUID.randomUUID()) { it })
    }
}
```

`BankGatewayAdapterTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.config

import io.github.hyeokjinon.banktransfer.gateway.BankGatewayClient
import io.github.hyeokjinon.banktransfer.gateway.BankGatewayConfig
import io.github.hyeokjinon.banktransfer.mockbank.MockBankServer
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class BankGatewayAdapterTest {

    @Test
    fun `은행 응답 뒤 처리는 Netty 이벤트 루프가 아니라 앱 실행기로 넘긴다`() {
        MockBankServer().start().use { bank ->
            BankGatewayClient(BankGatewayConfig(host = "localhost", port = bank.boundPort)).start().use { client ->
                val deadline = System.currentTimeMillis() + 5_000
                while (!client.isConnected) {
                    check(System.currentTimeMillis() < deadline) { "not connected" }
                    Thread.sleep(20)
                }
                val pool = Executors.newSingleThreadExecutor()
                val dispatched = AtomicInteger()
                val executor = Executor { task -> dispatched.incrementAndGet(); pool.execute(task) }

                val name = BankGatewayAdapter(client, executor).inquireHolderName("090", "1002123456789").get(5, TimeUnit.SECONDS)

                assertEquals("홍길동", name)
                assertEquals(1, dispatched.get())
                pool.shutdown()
            }
        }
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test`
Expected: FAIL — 컴파일 오류 `No value passed for parameter 'executor'` (BankGatewayAdapterTest). JpaVerificationRepositoryTest 는 아직 InMemory 빈이라 KST · FOR UPDATE 테스트가 실패해야 한다 — 컴파일 오류로 함께 멈춘다.

- [ ] **Step 3: 영속성 구현**

`apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/AccountVerificationPersistence.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.LockModeType
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/** DATETIME(6) 은 마이크로초까지만 담는다. 저장 전에 맞춰 잘라야 저장한 값과 다시 읽은 값이 같다. */
internal fun Instant.toDbPrecision(): Instant = truncatedTo(ChronoUnit.MICROS)

@Entity
@Table(name = "account_verification")
class AccountVerificationEntity(
    @Id @Column(columnDefinition = "char(36)") var id: String = "",
    var bankCode: String = "",
    var accountNo: String = "",
    var holderName: String = "",
    @Column(columnDefinition = "char(64)") var codeHash: String = "",
    @Enumerated(EnumType.STRING) var status: VerificationStatus = VerificationStatus.SENT,
    var failedAttempts: Int = 0,
    var expiresAt: Instant = Instant.EPOCH,
    var verifiedAt: Instant? = null,
    var createdAt: Instant = Instant.EPOCH,
) {
    fun toDomain() = AccountVerification(
        id = UUID.fromString(id), bankCode = bankCode, accountNo = accountNo, holderName = holderName,
        codeHash = codeHash, status = status, failedAttempts = failedAttempts, expiresAt = expiresAt, verifiedAt = verifiedAt,
    )

    /** 바뀔 수 있는 값만 옮긴다. id · 계좌 · 코드 해시는 만들 때 정해지고 바뀌지 않는다. */
    fun applyChanges(v: AccountVerification) {
        status = v.status
        failedAttempts = v.failedAttempts
        expiresAt = v.expiresAt.toDbPrecision()
        verifiedAt = v.verifiedAt?.toDbPrecision()
    }

    companion object {
        fun from(v: AccountVerification, createdAt: Instant) = AccountVerificationEntity(
            id = v.id.toString(), bankCode = v.bankCode, accountNo = v.accountNo, holderName = v.holderName,
            codeHash = v.codeHash, status = v.status, failedAttempts = v.failedAttempts,
            expiresAt = v.expiresAt.toDbPrecision(), verifiedAt = v.verifiedAt?.toDbPrecision(),
            createdAt = createdAt.toDbPrecision(),
        )
    }
}

interface AccountVerificationJpaRepository : JpaRepository<AccountVerificationEntity, String> {

    /** SELECT … FOR UPDATE. 트랜잭션이 끝날 때까지 다른 요청은 이 행을 기다린다. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select v from AccountVerificationEntity v where v.id = :id")
    fun findForUpdate(@Param("id") id: String): AccountVerificationEntity?

    fun existsByBankCodeAndAccountNoAndStatus(bankCode: String, accountNo: String, status: VerificationStatus): Boolean
}

/**
 * 1원인증 저장소 (MySQL). update 는 행을 FOR UPDATE 로 잠근 채 읽고-바꾸고-쓴다:
 * 같은 건에 확인 요청이 동시에 와도 한 요청씩 순서대로 시도 횟수를 갱신한다 (메모리 저장소의 computeIfPresent 와 같은 보장).
 */
class JpaVerificationRepository(
    private val jpa: AccountVerificationJpaRepository,
    private val tx: TransactionTemplate,
    private val clock: Clock,
) : VerificationRepository {

    override fun save(verification: AccountVerification): AccountVerification =
        tx.execute { jpa.save(AccountVerificationEntity.from(verification, clock.instant())).toDomain() }!!

    override fun findById(id: UUID): AccountVerification? = jpa.findById(id.toString()).orElse(null)?.toDomain()

    override fun update(id: UUID, change: (AccountVerification) -> AccountVerification): AccountVerification? =
        tx.execute {
            val entity = jpa.findForUpdate(id.toString()) ?: return@execute null
            entity.applyChanges(change(entity.toDomain()))
            entity.toDomain()   // 커밋 때 변경 감지(dirty checking)로 UPDATE 된다
        }

    override fun existsVerified(bankCode: String, accountNo: String): Boolean =
        jpa.existsByBankCodeAndAccountNoAndStatus(bankCode, accountNo, VerificationStatus.VERIFIED)
}
```

- [ ] **Step 4: 조립 변경**

`AppConfiguration.kt`:

1) `BankGatewayAdapter` 를 다음으로 바꾼다:
```kotlin
/**
 * 조립(wiring)은 여기서만 한다. account 모듈은 BankPort 인터페이스만 알고,
 * 실제로 Netty 게이트웨이를 붙이는 건 이 어댑터다 (포트-어댑터 구조).
 *
 * 은행 응답 future 는 Netty 이벤트 루프가 완료한다. 그 뒤의 처리(DB 저장 등 블로킹)가 이벤트 루프에서 돌지 않도록
 * thenApplyAsync 로 앱 실행기에 넘긴다. 이벤트 루프가 막히면 같은 루프의 모든 은행 통신이 멈춘다.
 */
class BankGatewayAdapter(private val client: BankGatewayClient, private val executor: Executor) : BankPort {
    override fun inquireHolderName(bankCode: String, accountNo: String): CompletableFuture<String?> =
        client.inquireName(bankCode, accountNo).thenApplyAsync({ res ->
            when (res.header.responseCode) {
                ResponseCodes.OK -> res.holderName
                ResponseCodes.ACCOUNT_NOT_FOUND -> null
                else -> throw BankRejectedException(res.header.responseCode)
            }
        }, executor)

    override fun transfer(bankCode: String, accountNo: String, amount: Long, memo: String): CompletableFuture<Unit> =
        client.transfer(bankCode, accountNo, amount, memo).thenApplyAsync({ res ->
            if (!res.header.isOk) throw BankRejectedException(res.header.responseCode)
        }, executor)
}
```

2) `bankPort`, `verificationRepository` 빈을 다음으로 바꾼다:
```kotlin
    @Bean
    fun bankPort(client: BankGatewayClient, @Qualifier("applicationTaskExecutor") executor: Executor): BankPort =
        BankGatewayAdapter(client, executor)

    @Bean
    fun verificationRepository(jpa: AccountVerificationJpaRepository, tx: TransactionTemplate): VerificationRepository =
        JpaVerificationRepository(jpa, tx, Clock.systemUTC())
```

3) import 정리: 추가 `org.springframework.beans.factory.annotation.Qualifier`, `org.springframework.transaction.support.TransactionTemplate`, `java.util.concurrent.Executor`, `io.github.hyeokjinon.banktransfer.api.persistence.AccountVerificationJpaRepository`, `io.github.hyeokjinon.banktransfer.api.persistence.JpaVerificationRepository`. 삭제 `io.github.hyeokjinon.banktransfer.account.InMemoryVerificationRepository`.

- [ ] **Step 5: 통과 확인**

Run: `./gradlew :apps:api:test`
Expected: PASS (AccountVerificationApiTest 4, DatabaseSetupTest 2, JpaVerificationRepositoryTest 5, BankGatewayAdapterTest 1).
`applicationTaskExecutor` 빈을 못 찾는다는 오류가 나면: `./gradlew :apps:api:test --info` 의 빈 목록에서 `ThreadPoolTaskExecutor` 빈 이름을 확인해 `@Qualifier` 값을 맞춘다.

- [ ] **Step 6: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 1원인증 저장소를 MySQL 로 이전 (FOR UPDATE), 은행 응답 후속 처리를 앱 실행기로

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: 이체 저장소 — 트랜잭셔널 아웃박스

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/JpaTransferStoreTest.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (`transferStore` 빈)

**Interfaces:**
- Consumes: `TransferStore`, `Transfer`, `TransferEvent`, `StoreConflict` (Task 1), `toDbPrecision()` (Task 5)
- Produces: `TransferEntity`, `OutboxEventEntity`, `TransferJpaRepository.findByIdempotencyKey(key): TransferEntity?`, `OutboxEventJpaRepository.countByAggregateId(id: String): Long`, `JpaTransferStore(transfers, outbox, tx, payloadOf: (TransferEvent) -> String = ::outboxPayload)`

- [ ] **Step 1: 실패하는 테스트 작성**

`JpaTransferStoreTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import io.github.hyeokjinon.banktransfer.transfer.RandomReferenceGenerator
import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class JpaTransferStoreTest : ApiIntegrationTest() {

    @Autowired private lateinit var store: TransferStore
    @Autowired private lateinit var transfers: TransferJpaRepository
    @Autowired private lateinit var outbox: OutboxEventJpaRepository
    @Autowired private lateinit var tx: TransactionTemplate
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val references = RandomReferenceGenerator()

    private fun newTransfer(key: String = UUID.randomUUID().toString(), reference: String = references.next()): Transfer {
        val now = Instant.parse("2026-10-19T01:00:00.123456Z")
        return Transfer(
            id = UUID.randomUUID(), idempotencyKey = key, reference = reference, requestHash = "h".repeat(64),
            bankCode = "090", accountNo = "1002123456789", amount = 50_000, memo = "출금",
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
        assertEquals(transfer, store.findByIdempotencyKey(transfer.idempotencyKey))
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
        val broken = JpaTransferStore(transfers, outbox, tx, payloadOf = { "not-json" })
        val transfer = newTransfer()

        assertFails { broken.saveWithEvent(transfer, requested(transfer)) }
        assertNull(store.findById(transfer.id))
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*JpaTransferStoreTest*'`
Expected: FAIL — `Unresolved reference 'TransferJpaRepository'`, `'JpaTransferStore'`

- [ ] **Step 3: 구현**

`apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/persistence/TransferPersistence.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferEvent
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import io.github.hyeokjinon.banktransfer.transfer.TransferStore
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "transfer")
class TransferEntity(
    @Id @Column(columnDefinition = "char(36)") var id: String = "",
    var idempotencyKey: String = "",
    @Column(columnDefinition = "char(20)") var reference: String = "",
    @Column(columnDefinition = "char(64)") var requestHash: String = "",
    var bankCode: String = "",
    var accountNo: String = "",
    var amount: Long = 0,
    var memo: String = "",
    @Enumerated(EnumType.STRING) var status: TransferStatus = TransferStatus.RECEIVED,
    var bankResponseCode: String? = null,
    var sendingAt: Instant? = null,
    var inquiryAttempts: Int = 0,
    var createdAt: Instant = Instant.EPOCH,
    var updatedAt: Instant = Instant.EPOCH,
    /** 낙관적 락. null 이면 Spring Data 가 새 엔티티로 보고 INSERT(persist) 한다. */
    @Version var version: Long? = null,
) {
    fun toDomain() = Transfer(
        id = UUID.fromString(id), idempotencyKey = idempotencyKey, reference = reference, requestHash = requestHash,
        bankCode = bankCode, accountNo = accountNo, amount = amount, memo = memo, status = status,
        bankResponseCode = bankResponseCode, sendingAt = sendingAt, createdAt = createdAt, updatedAt = updatedAt,
    )

    companion object {
        fun from(t: Transfer) = TransferEntity(
            id = t.id.toString(), idempotencyKey = t.idempotencyKey, reference = t.reference, requestHash = t.requestHash,
            bankCode = t.bankCode, accountNo = t.accountNo, amount = t.amount, memo = t.memo, status = t.status,
            bankResponseCode = t.bankResponseCode, sendingAt = t.sendingAt?.toDbPrecision(),
            createdAt = t.createdAt.toDbPrecision(), updatedAt = t.updatedAt.toDbPrecision(),
        )
    }
}

/** 트랜잭셔널 아웃박스. published_at 이 NULL 이면 아직 Kafka 로 나가지 않은 이벤트다 (발행은 P3-2). */
@Entity
@Table(name = "outbox_event")
class OutboxEventEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) var id: Long? = null,
    @Column(columnDefinition = "char(36)") var aggregateId: String = "",
    var eventType: String = "",
    @JdbcTypeCode(SqlTypes.JSON) var payload: String = "{}",
    var createdAt: Instant = Instant.EPOCH,
    var publishedAt: Instant? = null,
)

interface TransferJpaRepository : JpaRepository<TransferEntity, String> {
    fun findByIdempotencyKey(idempotencyKey: String): TransferEntity?
}

interface OutboxEventJpaRepository : JpaRepository<OutboxEventEntity, Long> {
    fun countByAggregateId(aggregateId: String): Long
}

/**
 * 메시지는 "이 이체를 처리하라"는 신호만 담는다. 컨슈머는 DB 에서 최신 상태를 읽는다.
 * eventId 는 outbox_event.id (INSERT 때 정해짐) 라 여기 넣지 않고 발행할 때(P3-2) 붙인다.
 */
internal fun outboxPayload(event: TransferEvent): String = when (event) {
    is TransferEvent.TransferRequested ->
        """{"transferId":"${event.transferId}","occurredAt":"${event.occurredAt}"}"""
}

/**
 * 이체 저장소 (MySQL). saveWithEvent 는 transfer 와 outbox_event 를 한 트랜잭션에 쓴다:
 * 둘 다 커밋되거나 둘 다 롤백되므로 "이체는 있는데 처리 신호가 없는" 상태가 생기지 않는다.
 * UNIQUE 위반은 제약 이름을 보고 도메인 예외([StoreConflict])로 바꿔 던진다.
 */
class JpaTransferStore(
    private val transfers: TransferJpaRepository,
    private val outbox: OutboxEventJpaRepository,
    private val tx: TransactionTemplate,
    private val payloadOf: (TransferEvent) -> String = ::outboxPayload,
) : TransferStore {

    override fun saveWithEvent(transfer: Transfer, event: TransferEvent): Transfer =
        try {
            tx.execute {
                val saved = transfers.saveAndFlush(TransferEntity.from(transfer))
                outbox.saveAndFlush(
                    OutboxEventEntity(
                        aggregateId = transfer.id.toString(),
                        eventType = event::class.simpleName!!,
                        payload = payloadOf(event),
                        createdAt = event.occurredAt.toDbPrecision(),
                    ),
                )
                saved.toDomain()
            }!!
        } catch (e: DataIntegrityViolationException) {
            // MySQL 메시지 예: Duplicate entry 'k1' for key 'transfer.uk_transfer_idempotency'
            val message = e.mostSpecificCause.message.orEmpty()
            when {
                "uk_transfer_idempotency" in message -> throw StoreConflict.DuplicateIdempotencyKey(transfer.idempotencyKey)
                "uk_transfer_reference" in message -> throw StoreConflict.DuplicateReference(transfer.reference)
                else -> throw e
            }
        }

    override fun findById(id: UUID): Transfer? = transfers.findById(id.toString()).orElse(null)?.toDomain()

    override fun findByIdempotencyKey(key: String): Transfer? = transfers.findByIdempotencyKey(key)?.toDomain()
}
```

`AppConfiguration.kt` 에 빈 추가 (import: `io.github.hyeokjinon.banktransfer.api.persistence.JpaTransferStore`, `...persistence.TransferJpaRepository`, `...persistence.OutboxEventJpaRepository`, `io.github.hyeokjinon.banktransfer.transfer.TransferStore`):
```kotlin
    @Bean
    fun transferStore(transfers: TransferJpaRepository, outbox: OutboxEventJpaRepository, tx: TransactionTemplate): TransferStore =
        JpaTransferStore(transfers, outbox, tx)
```

- [ ] **Step 4: 통과 확인**

Run: `./gradlew :apps:api:test --tests '*JpaTransferStoreTest*'`
Expected: PASS (4 tests)

- [ ] **Step 5: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 이체 저장소 — transfer 와 outbox_event 를 한 트랜잭션에 저장, UNIQUE 위반을 도메인 예외로

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 접수 · 조회 API

**Files:**
- Create: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/TransferController.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/web/ApiExceptionHandler.kt`
- Modify: `apps/api/src/main/kotlin/io/github/hyeokjinon/banktransfer/api/config/AppConfiguration.kt` (`transferService` 빈)
- Test: `apps/api/src/test/kotlin/io/github/hyeokjinon/banktransfer/api/TransferApiTest.kt`

**Interfaces:**
- Consumes: `TransferService`, `RandomReferenceGenerator`, `VerifiedAccounts` (Task 1·2), `VerificationRepository.existsVerified` (Task 3), `TransferStore` 빈 (Task 6), `ApiIntegrationTest` (Task 4)
- Produces: `POST /api/v1/transfers` (헤더 `Idempotency-Key`), `GET /api/v1/transfers/{id}`, `TransferResponse`

- [ ] **Step 1: 실패하는 테스트 작성**

`TransferApiTest.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api

import io.github.hyeokjinon.banktransfer.account.AccountVerification
import io.github.hyeokjinon.banktransfer.account.VerificationRepository
import io.github.hyeokjinon.banktransfer.account.VerificationStatus
import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class TransferApiTest : ApiIntegrationTest() {

    @Autowired private lateinit var verifications: VerificationRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    private val mapType = object : ParameterizedTypeReference<Map<String, Any?>>() {}

    private data class Response(val status: Int, val body: Map<String, Any?>, val location: String?)

    private fun post(key: String?, body: Map<String, Any>): Response =
        client.post().uri("/api/v1/transfers")
            .contentType(MediaType.APPLICATION_JSON)
            .apply { if (key != null) header("Idempotency-Key", key) }
            .body(body)
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), r.headers.location?.toString()) }

    private fun get(id: Any?): Response =
        client.get().uri("/api/v1/transfers/$id")
            .exchange { _, r -> Response(r.statusCode.value(), r.bodyTo(mapType) ?: emptyMap(), null) }

    /** 1원인증을 마친 계좌를 DB 에 만든다. 테스트끼리 DB 를 공유하므로 계좌번호는 매번 새로. */
    private fun verifiedAccount(): String {
        val accountNo = "55" + Random.nextLong(10_000_000_000, 99_999_999_999)
        verifications.save(
            AccountVerification(
                id = UUID.randomUUID(), bankCode = "090", accountNo = accountNo, holderName = "홍길동",
                codeHash = "0".repeat(64), status = VerificationStatus.VERIFIED, failedAttempts = 0,
                expiresAt = Instant.now(), verifiedAt = Instant.now(),
            ),
        )
        return accountNo
    }

    private fun body(accountNo: String, amount: Long = 50_000) =
        mapOf("bankCode" to "090", "accountNo" to accountNo, "amount" to amount, "memo" to "출금")

    @Test
    fun `인증된 계좌로 요청하면 202 RECEIVED, 조회하면 같은 건`() {
        val created = post(UUID.randomUUID().toString(), body(verifiedAccount()))

        assertEquals(202, created.status)
        assertEquals("RECEIVED", created.body["status"])
        assertEquals("/api/v1/transfers/${created.body["id"]}", created.location)

        val found = get(created.body["id"])
        assertEquals(200, found.status)
        assertEquals(created.body["id"], found.body["id"])
        assertEquals(50_000, (found.body["amount"] as Number).toInt())
    }

    @Test
    fun `같은 키 같은 내용으로 다시 보내면 200 과 같은 id`() {
        val key = UUID.randomUUID().toString()
        val account = verifiedAccount()
        val first = post(key, body(account))
        val again = post(key, body(account))

        assertEquals(200, again.status)
        assertEquals(first.body["id"], again.body["id"])
    }

    @Test
    fun `같은 키에 금액을 바꾸면 422 IDEMPOTENCY_KEY_REUSED`() {
        val key = UUID.randomUUID().toString()
        val account = verifiedAccount()
        post(key, body(account))

        val reused = post(key, body(account, amount = 70_000))

        assertEquals(422, reused.status)
        assertEquals("IDEMPOTENCY_KEY_REUSED", reused.body["code"])
    }

    @Test
    fun `인증되지 않은 계좌는 422 ACCOUNT_NOT_VERIFIED`() {
        val response = post(UUID.randomUUID().toString(), body("1101234567890"))

        assertEquals(422, response.status)
        assertEquals("ACCOUNT_NOT_VERIFIED", response.body["code"])
    }

    @Test
    fun `키 헤더가 없으면 400 IDEMPOTENCY_KEY_REQUIRED`() {
        val response = post(null, body(verifiedAccount()))

        assertEquals(400, response.status)
        assertEquals("IDEMPOTENCY_KEY_REQUIRED", response.body["code"])
    }

    @Test
    fun `금액이 0 이면 400 INVALID_AMOUNT`() {
        val response = post(UUID.randomUUID().toString(), body(verifiedAccount(), amount = 0))

        assertEquals(400, response.status)
        assertEquals("INVALID_AMOUNT", response.body["code"])
    }

    @Test
    fun `없는 id 는 404 TRANSFER_NOT_FOUND`() {
        val response = get(UUID.randomUUID())

        assertEquals(404, response.status)
        assertEquals("TRANSFER_NOT_FOUND", response.body["code"])
    }

    @Test
    fun `같은 키로 동시에 10건 보내도 이체 1건 · 이벤트 1건`() {
        val key = UUID.randomUUID().toString()
        val request = body(verifiedAccount())
        val pool = Executors.newFixedThreadPool(10)
        val start = CountDownLatch(1)

        val responses = (1..10).map { pool.submit(Callable { start.await(); post(key, request) }) }
        start.countDown()
        val results = responses.map { it.get() }
        pool.shutdown()

        assertEquals(1, results.count { it.status == 202 })
        assertEquals(9, results.count { it.status == 200 })
        assertEquals(1, results.map { it.body["id"] }.toSet().size)
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM transfer WHERE idempotency_key = ?", Int::class.java, key))
        assertEquals(
            1,
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_event o JOIN transfer t ON o.aggregate_id = t.id WHERE t.idempotency_key = ?",
                Int::class.java, key,
            ),
        )
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `./gradlew :apps:api:test --tests '*TransferApiTest*'`
Expected: FAIL — 대부분 404 (엔드포인트 없음). 예: `expected: <202> but was: <404>`

- [ ] **Step 3: 컨트롤러 구현**

`TransferController.kt`:
```kotlin
package io.github.hyeokjinon.banktransfer.api.web

import io.github.hyeokjinon.banktransfer.transfer.RequestTransfer
import io.github.hyeokjinon.banktransfer.transfer.Transfer
import io.github.hyeokjinon.banktransfer.transfer.TransferService
import io.github.hyeokjinon.banktransfer.transfer.TransferStatus
import jakarta.validation.Valid
import jakarta.validation.constraints.Pattern
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.time.Instant
import java.util.UUID

data class TransferRequestBody(
    @field:Pattern(regexp = "\\d{3}", message = "은행코드는 숫자 3자리")
    val bankCode: String,
    @field:Pattern(regexp = "\\d{10,16}", message = "계좌번호는 숫자 10~16자리")
    val accountNo: String,
    /** 금액 범위(1 ~ 1천만원)는 도메인이 검사한다 → INVALID_AMOUNT */
    val amount: Long,
    /** 적요 길이(EUC-KR 20바이트)는 도메인이 검사한다 → INVALID_MEMO */
    val memo: String = "",
)

data class TransferResponse(
    val id: UUID,
    val status: TransferStatus,
    val bankCode: String,
    val accountNo: String,
    val amount: Long,
    val memo: String,
    val bankResponseCode: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun from(t: Transfer) = TransferResponse(
            t.id, t.status, t.bankCode, t.accountNo, t.amount, t.memo, t.bankResponseCode, t.createdAt, t.updatedAt,
        )
    }
}

@RestController
@RequestMapping("/api/v1/transfers")
class TransferController(private val service: TransferService) {

    /**
     * 접수만 하고 바로 돌려준다(202 Accepted). 은행 처리는 P3-2 의 Kafka 컨슈머가 한다.
     * 같은 Idempotency-Key 로 같은 내용을 다시 보내면 새로 만들지 않고 기존 건을 200 으로 돌려준다.
     * 헤더가 없으면 빈 문자열로 넘겨 도메인이 IDEMPOTENCY_KEY_REQUIRED 로 거절하게 한다 (규칙은 한 곳에).
     */
    @PostMapping
    fun request(
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @Valid @RequestBody body: TransferRequestBody,
    ): ResponseEntity<TransferResponse> {
        val result = service.request(
            RequestTransfer(idempotencyKey.orEmpty(), body.bankCode, body.accountNo, body.amount, body.memo),
        )
        val response = TransferResponse.from(result.transfer)
        return if (result.created) {
            ResponseEntity.accepted().location(URI.create("/api/v1/transfers/${response.id}")).body(response)
        } else {
            ResponseEntity.ok(response)
        }
    }

    @GetMapping("/{id}")
    fun get(@PathVariable id: UUID): TransferResponse = TransferResponse.from(service.get(id))
}
```

- [ ] **Step 4: 예외 매핑**

`ApiExceptionHandler.kt`:
- import 추가: `io.github.hyeokjinon.banktransfer.transfer.TransferException`
- `@ExceptionHandler(` 목록에 `TransferException::class,` 추가 (`BankRejectedException::class,` 아래)
- `when (cause)` 의 `is BankRejectedException -> …` 줄 아래에 추가:
```kotlin
            is TransferException.IdempotencyKeyRequired -> HttpStatus.BAD_REQUEST to "IDEMPOTENCY_KEY_REQUIRED"
            is TransferException.InvalidAmount -> HttpStatus.BAD_REQUEST to "INVALID_AMOUNT"
            is TransferException.InvalidMemo -> HttpStatus.BAD_REQUEST to "INVALID_MEMO"
            is TransferException.AccountNotVerified -> HttpStatus.UNPROCESSABLE_CONTENT to "ACCOUNT_NOT_VERIFIED"
            is TransferException.IdempotencyKeyReused -> HttpStatus.UNPROCESSABLE_CONTENT to "IDEMPOTENCY_KEY_REUSED"
            is TransferException.TransferNotFound -> HttpStatus.NOT_FOUND to "TRANSFER_NOT_FOUND"
```

- [ ] **Step 5: 조립**

`AppConfiguration.kt` 에 빈 추가 (import: `io.github.hyeokjinon.banktransfer.transfer.TransferService`, `...transfer.RandomReferenceGenerator`, `...transfer.VerifiedAccounts`):
```kotlin
    /** transfer 모듈은 account 모듈을 모른다. "인증된 계좌인가?"를 1원인증 저장소에 연결하는 곳은 여기뿐이다. */
    @Bean
    fun transferService(store: TransferStore, verifications: VerificationRepository) = TransferService(
        store = store,
        verifiedAccounts = VerifiedAccounts { bankCode, accountNo -> verifications.existsVerified(bankCode, accountNo) },
        referenceGenerator = RandomReferenceGenerator(prefix = 'T'),
        clock = Clock.systemUTC(),
    )
```

- [ ] **Step 6: 통과 확인 (전체)**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL. `apps/api` 테스트: AccountVerificationApiTest 4, DatabaseSetupTest 2, JpaVerificationRepositoryTest 5, BankGatewayAdapterTest 1, JpaTransferStoreTest 4, TransferApiTest 8. 전체 테스트 수를 기록해 둔다 (Task 8 문서에 쓴다).

- [ ] **Step 7: 커밋**

```bash
git add apps/api
git commit -m "feat(api): 출금이체 접수 · 조회 API (Idempotency-Key, 202 Accepted)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: 손으로 확인 · 결정 기록 · 문서

**Files:**
- Create: `http/transfer.http`
- Create: `docs/adr/0004-idempotency-key-and-transactional-outbox.md`
- Modify: `docs/database.md`, `README.md`, `ARCHITECTURE.md`, `OPERATION.md`, `SETUP.md`, `CHANGELOG.md`, `HANDOVER.md`

- [ ] **Step 1: HTTP 요청 파일**

`http/transfer.http`:
```http
### IntelliJ HTTP Client 로 출금이체 접수 확인하기
### 준비: infra 를 띄우고 (docker compose ... up -d --wait), 실행 구성 "0. Mock Bank + API" 실행.
### 출금은 1원인증을 마친(VERIFIED) 계좌로만 된다 → 먼저 account-verification.http 의 1)·2) 로 1002123456789 를 인증한다.

### 1) 접수 → 202 RECEIVED, 응답 id 저장
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 50000,
  "memo": "출금"
}

> {%
    client.global.set("transferId", response.body.id);
    client.log("transferId = " + response.body.id + " status = " + response.status);
%}

### 2) 조회 → 200 RECEIVED (은행 처리는 P3-2 부터)
GET http://localhost:8080/api/v1/transfers/{{transferId}}

### 3) 같은 키로 두 번 → 처음 202, 두 번째 200 같은 id (키를 고정해서 두 번 실행해 본다)
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
Idempotency-Key: fixed-key-001

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 10000,
  "memo": "멱등확인"
}

### 4) 같은 키에 금액만 바꿈 → 422 IDEMPOTENCY_KEY_REUSED (3 을 먼저 실행)
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
Idempotency-Key: fixed-key-001

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 20000,
  "memo": "멱등확인"
}

### 5) 인증 안 된 계좌 → 422 ACCOUNT_NOT_VERIFIED
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json
Idempotency-Key: {{$random.uuid}}

{
  "bankCode": "090",
  "accountNo": "1101234567890",
  "amount": 10000,
  "memo": "출금"
}

### 6) 키 없음 → 400 IDEMPOTENCY_KEY_REQUIRED
POST http://localhost:8080/api/v1/transfers
Content-Type: application/json

{
  "bankCode": "090",
  "accountNo": "1002123456789",
  "amount": 10000,
  "memo": "출금"
}
```

- [ ] **Step 2: ADR-0004**

`docs/adr/0004-idempotency-key-and-transactional-outbox.md`:
```markdown
# ADR-0004 출금 접수는 Idempotency-Key + DB UNIQUE 로 멱등하게, 처리 신호는 트랜잭셔널 아웃박스로

- 상태: 제안 (2026-10-19) — PR 리뷰 후 채택
- 단계: P3-1 · 설계: [P3 설계 문서](../superpowers/specs/2026-10-19-p3-transfer-design.md) §4 · [database.md](../database.md)

## 맥락
- 클라이언트는 타임아웃 · 네트워크 끊김 때 같은 출금 요청을 다시 보낸다. 서버가 두 번 접수하면 두 번 출금된다.
- 접수(DB 저장)와 처리 요청(Kafka 발행)을 따로 하면, 둘 중 하나만 성공하는 순간이 생긴다: 저장했는데 메시지가 없으면 영원히 처리되지 않고, 메시지는 갔는데 저장이 롤백되면 없는 이체를 처리한다.
- 예전 시스템은 요청 저장 후 별도로 Kafka 로 보냈고, 빠진 건은 결과조회와 일마감 대사로 사람이 맞췄다.

## 결정
1. `POST /api/v1/transfers` 는 `Idempotency-Key` 헤더(1~64자)가 **필수**다.
2. `transfer.idempotency_key` 에 **UNIQUE**. 같은 키가 오면 `request_hash`(은행·계좌·금액·적요의 SHA-256)를 비교해
   같으면 기존 건(200), 다르면 422 `IDEMPOTENCY_KEY_REUSED`.
3. 동시에 같은 키가 오면 DB 가 하나만 통과시킨다. 진 쪽은 UNIQUE 위반을 도메인 예외로 받아 이긴 쪽 건을 돌려준다.
   (앱 메모리 검사가 아니라 DB 가 마지막 방어선 → 인스턴스가 여러 대여도 같다)
4. 이체 저장과 `outbox_event` 기록을 **한 트랜잭션**에 쓴다. 도메인은 포트 `TransferStore.saveWithEvent` 의 "함께 성공/실패" 계약만 안다.
5. 아웃박스 메시지는 처리 신호(`transferId`, `occurredAt`)만 담는다. 컨슈머는 DB 에서 최신 상태를 읽는다.

## 결과
- 증명 테스트: 같은 키 동시 10건 → 이체 1 · 이벤트 1 (`TransferApiTest`), 아웃박스 실패 시 이체도 없음 (`JpaTransferStoreTest`).
- 발행(P3-2)은 "적어도 한 번"이 된다 → 컨슈머 멱등(상태 조건부 UPDATE)이 필요하다 (ADR-0005 에서).
- 한계: 멱등키 보관 기간이 무한하다(이체 행과 같이 산다). 키 만료 정책이 필요해지면 별도 테이블로 분리한다.

## 검토한 다른 선택지
| 선택지 | 버린 이유 |
|---|---|
| 서버가 요청 본문 해시로 중복 판단 (키 없음) | 같은 금액을 의도적으로 두 번 보내는 정상 요청을 막는다 |
| Redis 로 키 선점 (SETNX) | DB 와 Redis 사이에 다시 원자성 문제가 생긴다. DB 하나로 충분하다 |
| 저장 후 바로 Kafka 발행 (아웃박스 없음) | 발행 실패 · 앱 장애 시 "저장됐는데 처리 안 됨"이 생긴다 |
| Kafka 트랜잭션 + DB 트랜잭션 묶기 | 두 자원의 분산 트랜잭션이 필요하다. 아웃박스가 단순하고 설명 가능하다 |
```

- [ ] **Step 3: database.md 갱신**

- 맨 위 기준 문구 `> 기준: P3 설계(2026-10-19). 현재 \`main\` 에는 아직 테이블이 없고(1원인증은 메모리 저장소), **P3-1 에서 Flyway \`V1__init.sql\` 로 생성**한다.` 를 `> 기준: P3-1 (Flyway \`V1__init.sql\` 적용). 1원인증 · 이체 · 아웃박스 테이블이 MySQL 에 있다.` 로 바꾼다.
- §1 표에 행 추가: `| 시간대 설정 | JDBC \`connectionTimeZone=Asia/Seoul\` · Hibernate \`jdbc.time_zone=Asia/Seoul\` + \`type.preferred_instant_jdbc_type=TIMESTAMP\` | 마지막 설정이 없으면 Hibernate 7 이 Instant 를 UTC 로 쓴다 |`
- §2 계층 표의 Hibernate 행 값을 `\`Asia/Seoul\` + \`hibernate.type.preferred_instant_jdbc_type: TIMESTAMP\`` 로 바꾼다.
- §4-3 `payload` 설명을 `\`{"transferId", "occurredAt"}\`. eventId 는 이 행의 \`id\` 라 발행(P3-2) 때 메시지에 붙인다` 로 바꾼다.
- 변경 이력에 행 추가: `| 2026-10-19 | V1 적용 (P3-1). payload 에서 eventId 제외, Hibernate Instant 설정 추가 | PR (P3-1) |`

- [ ] **Step 4: 안내 문서 갱신**

- `README.md`
  - "지금 되는 것"에 항목 추가: `- **출금이체 접수 API (P3-1)**: \`Idempotency-Key\` 로 멱등 접수(202), 1원인증 계좌만 허용, 이체와 처리 신호(아웃박스)를 한 트랜잭션에 저장. MySQL(Flyway) 사용`
  - 테스트 수 문구를 Task 7 Step 6 에서 기록한 숫자로 바꾼다 (`테스트 29개` 가 나오는 모든 곳: README, `.run` 설명 줄, 터미널 절).
  - 모듈 표에 행 추가: `| \`modules/transfer\` | 출금이체 도메인: 상태 전이, 멱등 접수, 거래번호. **Spring·JPA 를 모른다** | 없음 |`, `apps/api` 행 의존에 `transfer` 추가.
  - 로드맵 P3 줄 아래에 들여쓰기 항목: `  - [x] P3-1 접수: Idempotency-Key · 아웃박스 · 1원인증 MySQL 이전 → [ADR-0004](docs/adr/0004-idempotency-key-and-transactional-outbox.md)`, `  - [ ] P3-2 처리: 폴링 퍼블리셔 → Kafka → 컨슈머 → 은행`, `  - [ ] P3-3 실패: 재시도 · DLT · 결과조회`
  - 실행 절(IntelliJ, 터미널) 첫 줄에: `> P3-1 부터 API 는 MySQL 이 필요하다. 먼저 \`docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait\``
  - 기술 스택에 `Spring Data JPA (Hibernate 7) · Flyway · MySQL 8.4 · Testcontainers` 추가.
- `ARCHITECTURE.md`: §1 그림의 `REPO[(InMemory 저장소)]` 를 `REPO[(MySQL<br/>Flyway)]` 로, §2 모듈 표에 `modules/transfer` 행 추가, 의존 방향에 `apps/api ──▶ modules/transfer (도메인, 의존 없음)` 추가, 소스 지도에 `api/persistence/`, `api/web/TransferController.kt`, `modules/transfer/` 추가, §6 제목을 `## 6. P3 진행 상황` 으로 바꾸고 첫 줄에 `- P3-1 접수 ✅ (ADR-0004). P3-2 · P3-3 진행 예정.`, ADR 색인에 0004 추가.
- `OPERATION.md`: §1 표 API 행 비고에 `MySQL 필요 (기동 시 Flyway 마이그레이션)`, §2 설정 표에 `spring.datasource.url` (`MYSQL_HOST`·`MYSQL_PORT`·`MYSQL_DATABASE` 환경변수), `spring.datasource.username/password` (`MYSQL_USER`·`MYSQL_PASSWORD`) 행 추가, §4 오류 코드 표에 6개 코드 추가 (Global Constraints 와 같다), §6 장애 대응에 `| 기동 시 \`Communications link failure\` | MySQL 실행 여부 | infra 먼저 \`up -d --wait\` |`, `| 기동 시 \`Schema-validation\` / Flyway 체크섬 오류 | 엔티티 · 마이그레이션 변경 | 적용된 V 파일을 고치지 않았는지 확인. 로컬은 \`down -v\` 로 초기화 |` 추가.
- `SETUP.md`: §2-3 IntelliJ 4번 앞에 `3-1. 인프라 먼저: \`docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait\` (API 가 MySQL 을 쓴다)` 추가, §2-2 문구에 `테스트는 Testcontainers 로 MySQL 을 띄우므로 Docker Desktop 이 켜져 있어야 한다` 추가, §4 체크리스트에 `- [ ] \`http/transfer.http\` 1) → 202, API 재시작 후 2) 조회 → 같은 건 (DB 에 남음)` 추가.
- `CHANGELOG.md`: `## [Unreleased]` 아래에 새 절:
```markdown
## [P3-1] 출금이체 접수 — 2026-10-xx (PR #N)
### Added
- `modules/transfer`: 상태 전이 규칙, 멱등 접수 서비스, 이용기관 거래번호 발급 (Spring·JPA 의존 없음).
- `POST /api/v1/transfers` (Idempotency-Key 필수, 202/200), `GET /api/v1/transfers/{id}`.
- MySQL + Flyway `V1__init.sql` (account_verification · transfer · outbox_event), 시각 KST 저장.
- 트랜잭셔널 아웃박스: 이체와 `TransferRequested` 이벤트를 한 트랜잭션에 저장.
- Testcontainers MySQL 통합 테스트 (공유 컨텍스트), ADR-0004, `http/transfer.http`.
### Changed
- 1원인증 저장소: 메모리 → MySQL (`SELECT … FOR UPDATE`). 재시작해도 인증 기록이 남는다.
- 은행 응답 뒤 처리를 Netty 이벤트 루프 대신 앱 실행기에서 실행 (`thenApplyAsync`).
- API 실행에 MySQL 이 필요하다.
```
  날짜와 PR 번호는 PR 을 만든 뒤 채운다.
- `HANDOVER.md`: 마지막 갱신 날짜, §1 표의 P3 행을 `| P3-1 접수 | ✅ 완료 (PR #N) | … |` 와 `| P3-2 처리 · P3-3 실패 | ⬜ 다음 | 설계 문서 §5·§6 |` 로, §3 다음 할 일을 `1. P3-2 구현 계획 작성 (폴링 퍼블리셔 · Kafka · 컨슈머)` 등으로, §4 의 "1원인증 저장소가 아직 메모리" 줄 삭제.

- [ ] **Step 5: 전체 빌드와 손 확인**

```bash
./gradlew build
docker compose -f infra/docker-compose.yml --env-file infra/.env up -d --wait
./gradlew :apps:mock-bank:run --args="9090"    # 터미널 1
./gradlew :apps:api:bootRun                    # 터미널 2
```
확인 (IntelliJ HTTP Client):
1. `account-verification.http` 1)·2) → `VERIFIED`
2. `transfer.http` 1) → 202, 2) → 200 `RECEIVED`, 3) 두 번 → 202 다음 200, 4) → 422, 5) → 422, 6) → 400
3. API 를 껐다 켠 뒤 `transfer.http` 2) → 같은 건이 그대로 (DB 에 남음)
4. MySQL 에서: `SELECT status, COUNT(*) FROM transfer GROUP BY status;`, `SELECT id, aggregate_id, published_at FROM outbox_event;` → published_at 은 NULL (발행은 P3-2)

- [ ] **Step 6: 커밋 · PR**

```bash
git add http docs README.md ARCHITECTURE.md OPERATION.md SETUP.md CHANGELOG.md HANDOVER.md
git commit -m "docs: P3-1 접수 — ADR-0004, transfer.http, DB · 안내 문서 갱신

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/p3-1-transfer-intake
gh pr create --base main --title "P3-1: 출금이체 접수 (Idempotency-Key · 트랜잭셔널 아웃박스 · MySQL)" --body-file <작성한 본문>
```
PR 본문: 무엇을 / 왜(멱등키 · 아웃박스 · FOR UPDATE · 이벤트 루프에서 블로킹 금지 · KST 설정) / 검증(테스트 수, 증명 테스트 목록, 손 확인 결과) / 리뷰 체크리스트(ADR-0004, 오류 코드, database.md) — 끝에 `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
PR 번호가 나오면 CHANGELOG · HANDOVER 의 `#N`, 날짜를 채워 추가 커밋.
```
