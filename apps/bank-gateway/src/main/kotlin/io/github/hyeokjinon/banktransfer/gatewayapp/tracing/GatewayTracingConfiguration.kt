package io.github.hyeokjinon.banktransfer.gatewayapp.tracing

import io.github.hyeokjinon.banktransfer.tracing.ActuatorSpans
import io.github.hyeokjinon.banktransfer.tracing.ScrubbingSpanExporter
import io.micrometer.tracing.exporter.SpanExportingPredicate
import io.opentelemetry.sdk.trace.export.SpanExporter
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.SpanExporters
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/** 게이트웨이 추적 조립 (P9b) — api 의 TracingConfiguration 과 같은 앞단(감싸는 본문은 modules/tracing 의 wrapAll 공유). 은행 호출 구간은 BankRequestTracing (GatewayConfiguration 이 조립). */
@Configuration(proxyBeanMethods = false)
class GatewayTracingConfiguration {

    @Bean
    fun spanExporters(exporters: ObjectProvider<SpanExporter>): SpanExporters =
        SpanExporters.of(ScrubbingSpanExporter.wrapAll(exporters.orderedStream().toList()))

    @Bean
    fun actuatorSpans(): SpanExportingPredicate = ActuatorSpans()
}
