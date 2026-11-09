package io.github.hyeokjinon.banktransfer.api.experiment

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * P7 진행 조건 실험 V3 (설계 §4). Micrometer `jvm.threads.live` 는 `ThreadMXBean.threadCount` 를 읽는다 (JvmThreadMetrics).
 * 가상 스레드 200개가 기다리는 동안 이 값이 200 만큼 늘지 않으면, 측정의 "스레드 최대" 는 플랫폼 스레드만 센 값이다 — 결과 문서 열 이름에 반영.
 * 캐리어(ForkJoinPool)는 코어 수까지 늘 수 있어 차이 100 미만이면 "세지 않음" 으로 본다.
 */
class VirtualThreadMetricsExperimentTest {

    @Test
    fun `V3 jvm threads live (ThreadMXBean threadCount) 는 가상 스레드를 세지 않는다`() {
        val mx = ManagementFactory.getThreadMXBean()
        val before = mx.threadCount
        val release = CountDownLatch(1)
        val started = CountDownLatch(200)
        val threads = (1..200).map { Thread.ofVirtual().start { started.countDown(); release.await() } }
        try {
            assertTrue(started.await(10, TimeUnit.SECONDS))
            val during = mx.threadCount
            println("[P7 실험 V3] 플랫폼 스레드 $before → $during (가상 스레드 200개가 기다리는 중)")
            assertTrue(during - before < 100, "가상 스레드를 센 것으로 보인다: $before → $during")
        } finally {
            release.countDown()
            threads.forEach { it.join() }
        }
    }
}
