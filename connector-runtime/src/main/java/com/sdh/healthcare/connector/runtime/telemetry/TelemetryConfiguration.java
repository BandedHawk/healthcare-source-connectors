/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporterBuilder;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenTelemetry export (design §12). Spring Boot builds the SDK from these
 * beans; its own OTLP trace and log exporters are switched off in
 * {@code application.yml}, so everything leaves through the filters here.
 * The Micrometer OTLP registry exports the JVM's metrics only, through its
 * own allow-list (§12.2).
 * <p>
 * With {@code sdh.telemetry.otlp.endpoint} unset, no exporters exist:
 * telemetry stays in the process, and audit events still go to the console
 * log.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
public class TelemetryConfiguration
{

    /** Created by Spring. */
    public TelemetryConfiguration()
    {
    }

    /**
     * The OTLP/HTTP URL for a signal; also used by the admin commands.
     *
     * @param otlp the export settings
     * @param path the signal's path, such as {@code /v1/logs}
     * @return the endpoint, without a trailing slash, then the path
     */
    public static String url(CommonSettings.Otlp otlp, String path)
    {
        String base = otlp.endpoint().endsWith("/")
                ? otlp.endpoint().substring(0, otlp.endpoint().length() - 1)
                : otlp.endpoint();
        return base + path;
    }

    /**
     * The headers to send with each export.
     *
     * @param otlp the export settings
     * @return the headers; empty if none are set
     */
    public static Map<String, String> headers(CommonSettings.Otlp otlp)
    {
        return otlp.headers() == null ? Map.of() : otlp.headers();
    }

    /**
     * Exports spans with only allow-listed attributes.
     *
     * @param settings the application's settings
     * @return the exporter
     */
    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    SpanExporter sdhSpanExporter(ConnectorSettings settings)
    {
        CommonSettings.Otlp otlp = settings.telemetry().otlp();
        OtlpHttpSpanExporterBuilder exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(url(otlp, "/v1/traces"))
                .setTimeout(otlp.timeout());
        headers(otlp).forEach(exporter::addHeader);
        return new AllowListSpanExporter(exporter.build());
    }

    /**
     * Exports audit events only, with only allow-listed attributes.
     *
     * @param settings the application's settings
     * @return the exporter
     */
    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    LogRecordExporter sdhLogRecordExporter(ConnectorSettings settings)
    {
        CommonSettings.Otlp otlp = settings.telemetry().otlp();
        OtlpHttpLogRecordExporterBuilder exporter =
                OtlpHttpLogRecordExporter.builder()
                        .setEndpoint(url(otlp, "/v1/logs"))
                        .setTimeout(otlp.timeout());
        headers(otlp).forEach(exporter::addHeader);
        return new AuditOnlyLogRecordExporter(exporter.build());
    }

    /**
     * Boot builds no OpenTelemetry meter provider (its metrics go through
     * Micrometer), so metrics recorded with the OpenTelemetry API need this
     * one. Every metric attribute comes from the allow-listed
     * {@code Fields}.
     *
     * @param settings the application's settings
     * @param resource describes the service
     * @return the meter provider
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    SdkMeterProvider sdhMeterProvider(ConnectorSettings settings,
            Resource resource)
    {
        CommonSettings.Otlp otlp = settings.telemetry().otlp();
        OtlpHttpMetricExporterBuilder exporter =
                OtlpHttpMetricExporter.builder()
                        .setEndpoint(url(otlp, "/v1/metrics"))
                        .setTimeout(otlp.timeout());
        headers(otlp).forEach(exporter::addHeader);
        return SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(
                        PeriodicMetricReader.builder(exporter.build())
                                .setInterval(
                                        settings.telemetry().metricInterval())
                                .build())
                .build();
    }

    /**
     * The Micrometer OTLP registry's settings, from {@code sdh.telemetry}
     * (design §12.2); Boot's own, from
     * {@code management.otlp.metrics.export}, back off.
     *
     * @param settings the application's settings
     * @param resource describes the service
     * @return the registry's settings
     */
    @Bean
    OtlpConfig sdhOtlpConfig(ConnectorSettings settings, Resource resource)
    {
        return JvmMetricsExport.config(settings.telemetry(), resource);
    }

    /**
     * Lets only the JVM's meters, with only the JVM's tags, into the OTLP
     * registry (design §12.2).
     *
     * @return the customizer
     */
    @Bean
    MeterRegistryCustomizer<OtlpMeterRegistry> sdhJvmMetricsOnly()
    {
        return registry -> registry.config()
                .meterFilter(JvmMetricsExport.filter());
    }

    /** The longest a flush waits for the collector. */
    private static final Duration FLUSH_WAIT = Duration.ofSeconds(2);

    /**
     * Installs the SDK for connector-core. Each application adds its own
     * gauges to the result.
     *
     * @param openTelemetry the SDK Spring Boot built
     * @return the installed telemetry
     */
    @Bean
    Telemetry telemetry(OpenTelemetry openTelemetry)
    {
        return Telemetry.install(openTelemetry, () -> flush(openTelemetry));
    }

    /**
     * Exports the SDK's buffered audit events and metrics, waiting a short
     * while at most. Spans are left to their batches: those of a process
     * that dies are of no use without their ends.
     *
     * @param openTelemetry the SDK
     */
    static void flush(OpenTelemetry openTelemetry)
    {
        if (openTelemetry instanceof OpenTelemetrySdk sdk)
        {
            sdk.getSdkLoggerProvider().forceFlush()
                    .join(FLUSH_WAIT.toMillis(), TimeUnit.MILLISECONDS);
            sdk.getSdkMeterProvider().forceFlush()
                    .join(FLUSH_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
