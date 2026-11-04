/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.telemetry.AuditOnlyLogRecordExporter;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import java.util.concurrent.TimeUnit;

/**
 * Audit events from admin commands reach the same places as the
 * connector's (design §12): always the console ({@code sdh.audit} logger),
 * and the OTLP collector when {@code sdh.telemetry.otlp.endpoint} is set. A
 * short-lived command can't wait for batches, so events are exported one
 * by one and flushed before exit.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AdminTelemetry implements AutoCloseable
{

    /** The OTLP log provider, or null when only the console is used. */
    private final SdkLoggerProvider logs;

    /**
     * Wraps a log provider.
     *
     * @param logs the provider, or null
     */
    private AdminTelemetry(SdkLoggerProvider logs)
    {
        this.logs = logs;
    }

    /**
     * Installs OTLP export of audit events when an endpoint is configured.
     *
     * @param properties the connector configuration
     * @return the telemetry, to close before exit
     */
    static AdminTelemetry install(ConnectorProperties properties)
    {
        CommonSettings.Otlp otlp = properties.telemetry().otlp();
        if (otlp.endpoint() == null || otlp.endpoint().isBlank())
        {
            return new AdminTelemetry(null);
        }
        OtlpHttpLogRecordExporterBuilder exporter =
                OtlpHttpLogRecordExporter.builder()
                        .setEndpoint(TelemetryConfiguration.url(otlp,
                                "/v1/logs"))
                        .setTimeout(otlp.timeout());
        TelemetryConfiguration.headers(otlp).forEach(exporter::addHeader);
        SdkLoggerProvider logs = SdkLoggerProvider.builder()
                .setResource(Resource.getDefault().merge(Resource.create(
                        Attributes.of(AttributeKey.stringKey("service.name"),
                                "file-connector"))))
                .addLogRecordProcessor(SimpleLogRecordProcessor.create(
                        new AuditOnlyLogRecordExporter(exporter.build())))
                .build();
        Telemetry.install(
                OpenTelemetrySdk.builder().setLoggerProvider(logs).build());
        return new AdminTelemetry(logs);
    }

    /** Flushes and shuts down OTLP export, waiting up to 10 s for each. */
    @Override
    public void close()
    {
        if (logs != null)
        {
            logs.forceFlush().join(10, TimeUnit.SECONDS);
            logs.shutdown().join(10, TimeUnit.SECONDS);
            Telemetry.install(OpenTelemetry.noop());
        }
    }
}
