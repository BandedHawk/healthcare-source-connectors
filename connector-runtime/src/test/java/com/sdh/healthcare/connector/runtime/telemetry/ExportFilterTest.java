/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

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

/**
 * Tests the telemetry export filters ({@link AllowListSpanExporter},
 * {@link AuditOnlyLogRecordExporter}): spans keep only listed attributes
 * and lose exception events, and only audit events are exported.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ExportFilterTest
{

    /** A span keeps only listed attributes, and no exception events. */
    @Test
    void spansLoseUnlistedAttributesAndExceptionEvents()
    {
        InMemorySpanExporter out = InMemorySpanExporter.create();
        try (SdkTracerProvider tracers = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(
                        new AllowListSpanExporter(out)))
                .build())
        {
            Span started = tracers.get("t").spanBuilder("ingest.file")
                    .setAttribute("sdh.item_id", "01a1")
                    .setAttribute("sdh.seq", 7L)
                    .setAttribute("file.name", "smith-john.csv")
                    .setAttribute("code.stacktrace", "at parse(…)")
                    .startSpan();
            started.recordException(new IllegalArgumentException(
                    "Patient Smith, DOB 1970-01-01"));
            started.end();
            // Read before close: shutting down an in-memory exporter clears
            // it.
            SpanData span = out.getFinishedSpanItems().getFirst();
            assertThat(span.getAttributes().asMap().keySet())
                    .extracting(AttributeKey::getKey)
                    .containsExactlyInAnyOrder("sdh.item_id", "sdh.seq");
            assertThat(span.getEvents()).isEmpty();
            assertThat(span.toString()).doesNotContain("Smith")
                    .doesNotContain("smith");
        }
    }

    /** Only audit events leave, with only their listed attributes. */
    @Test
    void onlyAuditEventsAreExportedAndOnlyWithListedAttributes()
    {
        InMemoryLogRecordExporter out = InMemoryLogRecordExporter.create();
        try (SdkLoggerProvider loggers = SdkLoggerProvider.builder()
                .addLogRecordProcessor(SimpleLogRecordProcessor.create(
                        new AuditOnlyLogRecordExporter(out)))
                .build())
        {
            Logger log = loggers.get("t");
            log.logRecordBuilder()
                    .setBody("Could not read /data/inbound/smith-john.csv")
                    .emit();
            log.logRecordBuilder().setEventName("library.event")
                    .setBody("smith").emit();
            log.logRecordBuilder().setEventName("sdh.audit.item.completed")
                    .setBody("anything")
                    .setAttribute("sdh.item_id", "01a1")
                    .setAttribute("file.path", "/data/inbound/smith-john.csv")
                    .emit();
            assertThat(out.getFinishedLogRecordItems()).singleElement()
                    .satisfies(record ->
                    {
                        assertThat(record.getEventName())
                                .isEqualTo("sdh.audit.item.completed");
                        assertThat(record.getAttributes().asMap().keySet())
                                .extracting(AttributeKey::getKey)
                                .containsExactly("sdh.item_id");
                        assertThat(record.getBodyValue().asString())
                                .isEqualTo("item.completed");
                    });
        }
    }
}
