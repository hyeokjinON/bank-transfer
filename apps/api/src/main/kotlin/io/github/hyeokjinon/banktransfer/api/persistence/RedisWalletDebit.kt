package io.github.hyeokjinon.banktransfer.api.persistence

import io.github.hyeokjinon.banktransfer.transfer.StoreConflict
import org.redisson.api.RLock
import org.redisson.api.RedissonClient
import org.redisson.client.RedisException
import org.redisson.config.Config
import org.redisson.config.ConstantDelay
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Redis 분산락 (P4-2): 접수 트랜잭션 **바깥**에서 사용자별 Redisson 락을 잡고, 그 안에서 트랜잭션을 실행하고, 커밋이 끝난 뒤 푼다.
 *
 * 락은 DB 앞에서 같은 사용자 요청을 줄 세우는 문으로만 쓴다. 안쪽([inner])은 기준선과 같은 조건부 UPDATE 다 —
 * 안쪽에서 "읽고 → 비교 → 값 쓰기"를 하면, 락을 잡지 않는 환불 · 충전이 그 사이 더한 금액을 덮어써 사라진다(lost update).
 * 그래서 정확성은 DB(조건부 UPDATE + CHECK)가 지키고, 락이 만료된 뒤 작업이 끝나는 경우(GC 멈춤 등)에도 잔고는 틀리지 않는다.
 *
 * - 임대 시간을 주지 않는다 → Redisson watchdog 이 30초씩 연장하고, 앱이 죽으면 연장이 멈춰 만료된다.
 * - [waitTime] 안에 못 잡으면 WalletBusy(503). Redis 에 닿지 않아도 WalletBusy — 돈이 걸린 판단을 락 없이 진행하지 않는다 (fail-closed).
 * - 해제 실패는 결과를 바꾸지 않는다: 커밋은 이미 끝났다. 로그만 남기고, 락은 연장이 멈춰 임대 만료(30초)로 풀린다.
 */
class RedisWalletDebit(
    private val redisson: RedissonClient,
    private val inner: WalletDebit,
    private val waitTime: Duration = Duration.ofSeconds(3),
) : WalletDebit {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun <T> around(userId: String, transaction: () -> T): T {
        val lock = redisson.getLock(lockKey(userId))
        val acquired = try {
            lock.tryLock(waitTime.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: RedisException) {
            // 클라이언트에게는 락 경합과 같은 503 이지만 운영자는 구분해야 한다 → ERROR
            log.error("redis unavailable for wallet lock of {}, rejecting (fail-closed)", userId, e)
            throw StoreConflict.WalletBusy(userId)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw StoreConflict.WalletBusy(userId)
        }
        if (!acquired) {
            // 핫스팟에서는 흔한 결과라 INFO (부하 측정은 WARN 으로 돌린다)
            log.info("wallet lock of {} not acquired within {}, answering WALLET_BUSY", userId, waitTime)
            throw StoreConflict.WalletBusy(userId)
        }
        try {
            // 접수 트랜잭션은 이 안에서 시작 · 커밋된다 → 아래 unlock 은 커밋 뒤다 (JpaTransferStore 가 바깥 트랜잭션을 거부해 보장한다)
            return transaction()
        } finally {
            unlockQuietly(lock, userId)
        }
    }

    override fun withdraw(userId: String, transferId: UUID, amount: Long, at: Instant): Boolean =
        inner.withdraw(userId, transferId, amount, at)

    private fun unlockQuietly(lock: RLock, userId: String) {
        try {
            lock.unlock()
        } catch (e: RedisException) {
            log.error("failed to unlock wallet lock of {} after commit; it expires by lease", userId, e)
        } catch (e: IllegalMonitorStateException) {
            // 연장이 끊겨(긴 GC 멈춤 등) 이미 만료됐다. 안쪽이 조건부 UPDATE 라 잔고는 틀리지 않았다 (ADR-0009)
            log.error("wallet lock of {} expired before unlock", userId, e)
        } catch (e: RuntimeException) {
            // 알려진 두 경우 말고도 finally 에서 던지면 커밋된 접수가 500 이 되거나 트랜잭션의 진짜 예외를 가린다 → 로그만 남기고 삼킨다
            log.error("unexpected failure while unlocking wallet lock of {}; it expires by lease", userId, e)
        }
    }

    companion object {
        /** 사용자별 락 이름. 운영자가 redis-cli 로 볼 때도 이 이름이다. */
        fun lockKey(userId: String): String = "lock:wallet:$userId"
    }
}

/**
 * Redisson 단일 서버 설정 (P4-2). 앱(WalletDebitConfiguration)과 테스트가 같은 값을 쓴다.
 * - lazyInitialization: 기동 때 Redis 에 붙지 않는다 → Redis 가 내려가 있어도 앱은 뜨고, 차감 요청만 503 이 된다 (fail-closed).
 * - 연결 · 명령 응답 대기 1초, 재시도 1번 (기본 10초 · 3초 · 4번) → 장애 때 수 초 안에 503 이 난다.
 */
internal fun redissonConfig(address: String): Config = Config().apply {
    setLazyInitialization(true)
    useSingleServer()
        .setAddress(address)
        .setConnectTimeout(1_000)
        .setTimeout(1_000)
        .setRetryAttempts(1)
        .setRetryDelay(ConstantDelay(Duration.ofMillis(200)))
}
