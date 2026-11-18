package io.github.hyeokjinon.banktransfer.api.metrics

import io.github.hyeokjinon.banktransfer.api.support.ApiIntegrationTest
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertTrue

/** P9a 노출 (스펙 §3): 업무 지표 · 공통 태그 · 기본 지표(JVM · HTTP · Hikari · Tomcat · Kafka 컨슈머 lag)가 Prometheus 형식으로. */
class PrometheusEndpointTest : ApiIntegrationTest() {

    private fun scrape(): String = client.get().uri("/actuator/prometheus").retrieve().body(String::class.java)!!

    @Test
    fun `업무 지표 · 공통 태그 · 기본 지표가 나온다`() {
        client.get().uri("/actuator/health").retrieve().toBodilessEntity()
        val text = scrape()

        listOf(
            "banktransfer_transfer_accepted_total{", "banktransfer_transfer_bank_result_total{", "banktransfer_transfer_completed_seconds_bucket{",
            "banktransfer_transfer_gave_up_total{", "banktransfer_transfer_dlt_total{", "banktransfer_transfer_reconciled_total{",
            "banktransfer_outbox_published_total{", "banktransfer_bank_maintenance{",
            "banktransfer_transfer_open{", "banktransfer_transfer_open_oldest_seconds{", "banktransfer_transfer_unknown_exhausted{",
            "banktransfer_outbox_unpublished{", "banktransfer_outbox_unpublished_oldest_seconds{", "banktransfer_metrics_db_query_seconds_count{",
            "http_server_requests_seconds_bucket{", "jvm_memory_used_bytes{", "hikaricp_connections_active{", "tomcat_threads_busy_threads{",
            "process_start_time_seconds{",
        ).forEach { assertTrue(it in text, "없음: $it") }
        assertTrue("""application="bank-transfer"""" in text, "공통 태그 application 이 없다")
    }

    @Test
    fun `http 히스토그램 버킷은 5ms ~ 10s 안 - 위험 B2`() {
        repeat(2) { client.get().uri("/actuator/health").retrieve().toBodilessEntity() }

        val les = scrape().lineSequence()
            .filter { it.startsWith("http_server_requests_seconds_bucket{") && """uri="/actuator/health"""" in it }
            .mapNotNull { Regex("""le="([^"]+)"""").find(it)?.groupValues?.get(1) }
            .filter { it != "+Inf" }.map { it.toDouble() }.toList()

        assertTrue(les.isNotEmpty(), "버킷이 없다 — percentiles-histogram 설정 확인")
        assertTrue(les.min() >= 0.005, "가장 작은 버킷 ${les.min()}")
        assertTrue(les.max() <= 10.0, "가장 큰 버킷 ${les.max()}")
    }

    @Test
    fun `Kafka 컨슈머 lag 시계열이 있다 - Boot 자동 구성의 Micrometer 리스너`() {
        // KafkaClientMetrics 는 새 Kafka 지표를 주기적으로(기본 60초) 다시 묶는다 — 넉넉히 기다린다
        await.atMost(Duration.ofSeconds(90)) untilAsserted {
            assertTrue("kafka_consumer_fetch_manager_records_lag_max{" in scrape(), "컨슈머 lag 시계열이 없다")
        }
    }
}
