package io.github.hyeokjinon.banktransfer.api.support

import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * [count] 개의 작업을 고정 스레드 풀에서 **동시에 출발**시키고 모두 끝날 때까지 기다린 뒤 결과를 순서대로 돌려준다 (P4-1 동시성 테스트).
 * 스레드가 모두 준비된 뒤 래치 하나로 한꺼번에 풀어 준다 — 먼저 만든 스레드가 먼저 끝나 버려 경합이 사라지지 않게.
 * P4-2 에서 차감 전략(비관적 · 낙관적 · Redis 분산락)을 바꿔도 같은 도우미로 정확성부터 확인한다.
 */
fun <T> concurrently(count: Int, timeout: Duration = Duration.ofSeconds(60), task: (index: Int) -> T): List<T> {
    val pool = Executors.newFixedThreadPool(count)
    val ready = CountDownLatch(count)
    val start = CountDownLatch(1)
    try {
        val futures = (0 until count).map { i ->
            pool.submit(Callable {
                ready.countDown()
                start.await()
                task(i)
            })
        }
        check(ready.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) { "$count threads were not ready within $timeout" }
        start.countDown()
        return futures.map { it.get(timeout.toMillis(), TimeUnit.MILLISECONDS) }
    } finally {
        pool.shutdownNow()
    }
}
