/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.runtime.telemetry;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenTelemetry export (design §12). Spring Boot builds the SDK from these beans; its own OTLP exporters and the
 * Micrometer OTLP registry are switched off in application.yml, so everything leaves through the filters here.
 *
 * <p>With {@code sdh.telemetry.otlp.endpoint} unset, no exporters exist: telemetry stays in-process and audit events
 * still go to the console log.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
public class TelemetryConfiguration {

    /** The OTLP/HTTP URL for a signal path such as {@code /v1/logs}; also used by the admin commands. */
    public static String url(CommonSettings.Otlp otlp, String path) {
        String base = otlp.endpoint().endsWith("/") ? otlp.endpoint().substring(0, otlp.endpoint().length() - 1)
                : otlp.endpoint();
        return base + path;
    }

    public static Map<String, String> headers(CommonSettings.Otlp otlp) {
        return otlp.headers() == null ? Map.of() : otlp.headers();
    }

    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    SpanExporter sdhSpanExporter(ConnectorSettings props) {
        CommonSettings.Otlp o = props.telemetry().otlp();
        var b = OtlpHttpSpanExporter.builder().setEndpoint(url(o, "/v1/traces")).setTimeout(o.timeout());
        headers(o).forEach(b::addHeader);
        return new AllowListSpanExporter(b.build());
    }

    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    LogRecordExporter sdhLogRecordExporter(ConnectorSettings props) {
        CommonSettings.Otlp o = props.telemetry().otlp();
        var b = OtlpHttpLogRecordExporter.builder().setEndpoint(url(o, "/v1/logs")).setTimeout(o.timeout());
        headers(o).forEach(b::addHeader);
        return new AuditOnlyLogRecordExporter(b.build());
    }

    /**
     * Boot builds no OpenTelemetry meter provider (its metrics go through Micrometer), so metrics recorded with the
     * OpenTelemetry API need this one. Every metric attribute comes from the allow-listed {@code Fields}.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    SdkMeterProvider sdhMeterProvider(ConnectorSettings props, Resource resource) {
        CommonSettings.Otlp o = props.telemetry().otlp();
        var b = OtlpHttpMetricExporter.builder().setEndpoint(url(o, "/v1/metrics")).setTimeout(o.timeout());
        headers(o).forEach(b::addHeader);
        return SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(b.build())
                        .setInterval(props.telemetry().metricInterval()).build())
                .build();
    }

    /**
     * Installs the SDK for connector-core. Each application adds its own gauges to the result, such as the file
     * connector's inbound directory age.
     */
    @Bean
    Telemetry telemetry(OpenTelemetry openTelemetry) {
        return Telemetry.install(openTelemetry);
    }
}
