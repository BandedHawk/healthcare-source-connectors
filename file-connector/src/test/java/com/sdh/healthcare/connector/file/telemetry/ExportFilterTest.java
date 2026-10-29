package com.sdh.healthcare.connector.file.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

class ExportFilterTest {

    @Test
    void spansLoseUnlistedAttributesAndExceptionEvents() {
        InMemorySpanExporter out = InMemorySpanExporter.create();
        try (SdkTracerProvider tp = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(new AllowListSpanExporter(out))).build()) {
            Span s = tp.get("t").spanBuilder("ingest.file")
                    .setAttribute("sdh.file_id", "01a1")
                    .setAttribute("sdh.seq", 7L)
                    .setAttribute("file.name", "smith-john.csv")
                    .setAttribute("code.stacktrace", "at parse(…)")
                    .startSpan();
            s.recordException(new IllegalArgumentException("Patient Smith, DOB 1970-01-01"));
            s.end();
            // Read before close: shutting down an in-memory exporter clears it.
            SpanData span = out.getFinishedSpanItems().getFirst();
            assertThat(span.getAttributes().asMap().keySet()).extracting(AttributeKey::getKey)
                    .containsExactlyInAnyOrder("sdh.file_id", "sdh.seq");
            assertThat(span.getEvents()).isEmpty();
            assertThat(span.toString()).doesNotContain("Smith").doesNotContain("smith");
        }
    }

    @Test
    void onlyAuditEventsAreExportedAndOnlyWithListedAttributes() {
        InMemoryLogRecordExporter out = InMemoryLogRecordExporter.create();
        try (SdkLoggerProvider lp = SdkLoggerProvider.builder()
                .addLogRecordProcessor(SimpleLogRecordProcessor.create(new AuditOnlyLogRecordExporter(out))).build()) {
            Logger log = lp.get("t");
            log.logRecordBuilder().setBody("Could not read /data/inbound/smith-john.csv").emit();
            log.logRecordBuilder().setEventName("library.event").setBody("smith").emit();
            log.logRecordBuilder().setEventName("sdh.audit.file.completed").setBody("anything")
                    .setAttribute("sdh.file_id", "01a1").setAttribute("file.path", "/data/inbound/smith-john.csv")
                    .emit();
            assertThat(out.getFinishedLogRecordItems()).singleElement().satisfies(l -> {
                assertThat(l.getEventName()).isEqualTo("sdh.audit.file.completed");
                assertThat(l.getAttributes().asMap().keySet()).extracting(AttributeKey::getKey)
                        .containsExactly("sdh.file_id");
                assertThat(l.getBodyValue().asString()).isEqualTo("file.completed");
            });
        }
    }

    @Test
    void inboundAgeGaugeIgnoresHiddenFilesAndMarkers(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        assertThat(TelemetryConfiguration.oldestAgeSeconds(dir)).isZero();
        var hidden = java.nio.file.Files.writeString(dir.resolve(".tmp"), "x");
        java.nio.file.Files.setLastModifiedTime(hidden,
                java.nio.file.attribute.FileTime.from(java.time.Instant.now().minusSeconds(9999)));
        var f = java.nio.file.Files.writeString(dir.resolve("a.csv"), "x");
        java.nio.file.Files.setLastModifiedTime(f,
                java.nio.file.attribute.FileTime.from(java.time.Instant.now().minusSeconds(120)));
        assertThat(TelemetryConfiguration.oldestAgeSeconds(dir)).isBetween(119L, 130L);
    }
}
