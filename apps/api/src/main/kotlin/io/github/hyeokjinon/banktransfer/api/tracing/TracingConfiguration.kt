package io.github.hyeokjinon.banktransfer.api.tracing

import io.github.hyeokjinon.banktransfer.tracing.ActuatorSpans
import io.github.hyeokjinon.banktransfer.tracing.EmptyReconcileSpans
import io.github.hyeokjinon.banktransfer.tracing.ScrubbingSpanExporter
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.exporter.SpanExportingPredicate
import io.micrometer.tracing.propagation.Propagator
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.SpanExporters
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * api 추적 조립 (P9b). 구간 · 전파 · 로그 상관은 Boot 자동 구성(Micrometer Tracing + OTel 브리지)이 하고, 여기는 내보내기 앞단과 손으로 잇는 곳의 도구만 둔다.
 * - spanExporters: Boot 의 같은 이름 빈(@ConditionalOnMissingBean)을 대신해 모든 내보내기를 지우개로 감싼다 (E9). 감싸는 본문은 modules/tracing 의 [ScrubbingSpanExporter.wrapAll] 하나 (게이트웨이와 공유).
 *   OTLP 내보내기는 management.opentelemetry.tracing.export.otlp.endpoint 가 있을 때만 생긴다 — kind 의 app-config 만 준다 (E3).
 * - actuatorSpans: 헬스 프로브 · Prometheus 수집 요청 구간은 내보내지 않는다 (지표는 그대로).
 * - emptyReconcileSpans: 건수가 모두 0 인 정리 작업 root 는 내보내지 않는다.
 */
@Configuration(proxyBeanMethods = false)
class TracingConfiguration {

    @Bean
    fun spanExporters(exporters: ObjectProvider<SpanExporter>): SpanExporters =
        SpanExporters.of(ScrubbingSpanExporter.wrapAll(exporters.orderedStream().toList()))

    @Bean
    fun actuatorSpans(): SpanExportingPredicate = ActuatorSpans()

    /** 할 일 없던 정리 작업 root(10초마다 Pod 당 하나)는 내보내지 않는다 — 건이 있는 실행만 trace 로 남는다. */
    @Bean
    fun emptyReconcileSpans(): SpanExportingPredicate = EmptyReconcileSpans()

    /** 아웃박스 traceparent 쓰기 · 복원 (스펙 §4-1). Tracer · Propagator 가 없으면(추적을 뺀 조립) NOOP. */
    @Bean
    fun outboxTracing(tracer: ObjectProvider<Tracer>, propagator: ObjectProvider<Propagator>) =
        OutboxTracing(tracer.getIfAvailable { Tracer.NOOP }, propagator.getIfAvailable { Propagator.NOOP })

    /** 정리 작업 root · 건별 구간 (스펙 §4-2). */
    @Bean
    fun reconcileTracing(tracer: ObjectProvider<Tracer>) = ReconcileTracing(tracer.getIfAvailable { Tracer.NOOP })

    @Bean
    fun traceTags(tracer: ObjectProvider<Tracer>) = TraceTags(tracer.getIfAvailable { Tracer.NOOP })
}
