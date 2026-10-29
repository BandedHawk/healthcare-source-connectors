package com.sdh.healthcare.connector.file.telemetry;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenTelemetry export (design §12). Spring Boot builds the SDK from these beans; its own OTLP exporters and the
 * Micrometer OTLP registry are switched off in application.yml, so everything leaves through the filters here.
 *
 * <p>With {@code sdh.telemetry.otlp.endpoint} unset, no exporters exist: telemetry stays in-process and audit events
 * still go to the console log.
 */
@Configuration
public class TelemetryConfiguration {

    private static String url(ConnectorProperties.Otlp otlp, String path) {
        String base = otlp.endpoint().endsWith("/") ? otlp.endpoint().substring(0, otlp.endpoint().length() - 1)
                : otlp.endpoint();
        return base + path;
    }

    private static Map<String, String> headers(ConnectorProperties.Otlp otlp) {
        return otlp.headers() == null ? Map.of() : otlp.headers();
    }

    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    SpanExporter sdhSpanExporter(ConnectorProperties props) {
        ConnectorProperties.Otlp o = props.telemetry().otlp();
        var b = OtlpHttpSpanExporter.builder().setEndpoint(url(o, "/v1/traces")).setTimeout(o.timeout());
        headers(o).forEach(b::addHeader);
        return new AllowListSpanExporter(b.build());
    }

    @Bean
    @ConditionalOnProperty("sdh.telemetry.otlp.endpoint")
    LogRecordExporter sdhLogRecordExporter(ConnectorProperties props) {
        ConnectorProperties.Otlp o = props.telemetry().otlp();
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
    SdkMeterProvider sdhMeterProvider(ConnectorProperties props, Resource resource) {
        ConnectorProperties.Otlp o = props.telemetry().otlp();
        var b = OtlpHttpMetricExporter.builder().setEndpoint(url(o, "/v1/metrics")).setTimeout(o.timeout());
        headers(o).forEach(b::addHeader);
        return SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(b.build())
                        .setInterval(props.telemetry().metricInterval()).build())
                .build();
    }

    /** Installs the SDK for connector-core and adds the inbound directory gauge. */
    @Bean
    Telemetry telemetry(OpenTelemetry openTelemetry, ConnectorProperties props) {
        Telemetry t = Telemetry.install(openTelemetry);
        Path inbound = props.file().inboundDir();
        t.gauge("sdh.inbound.oldest_file_age", "s", "Age of the oldest file waiting in the inbound directory",
                () -> oldestAgeSeconds(inbound));
        return t;
    }

    /** Seconds since the oldest waiting file was last modified; 0 if none. Hidden files and markers don't count. */
    static long oldestAgeSeconds(Path dir) {
        if (dir == null) {
            return 0;
        }
        try (Stream<Path> s = Files.list(dir)) {
            long oldest = s.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith(".")
                            && !p.getFileName().toString().endsWith(".done"))
                    .mapToLong(p -> {
                        try {
                            return Files.getLastModifiedTime(p).toMillis();
                        } catch (IOException e) {
                            return Long.MAX_VALUE;
                        }
                    })
                    .min().orElse(Long.MAX_VALUE);
            return oldest == Long.MAX_VALUE ? 0 : Math.max(0, (Instant.now().toEpochMilli() - oldest) / 1000);
        } catch (IOException e) {
            return 0;
        }
    }
}
