/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Captures the audit events of one test at a time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AuditCapture implements BeforeEachCallback, AfterEachCallback
{

    /** The prefix of every audit event's name. */
    private static final String PREFIX = "sdh.audit.";

    /** Keeps the events. */
    private InMemoryLogRecordExporter events;

    /** The SDK installed for the test. */
    private OpenTelemetrySdk sdk;

    /** How many times telemetry was flushed. */
    private final AtomicInteger flushes = new AtomicInteger();

    /** Creates the extension; nothing is installed until a test starts. */
    AuditCapture()
    {
    }

    /**
     * Installs a fresh SDK.
     *
     * @param context the test's context
     */
    @Override
    public void beforeEach(ExtensionContext context)
    {
        events = InMemoryLogRecordExporter.create();
        sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build();
        flushes.set(0);
        Telemetry.install(sdk, flushes::incrementAndGet);
    }

    /**
     * Puts the no-op telemetry back and closes the SDK.
     *
     * @param context the test's context
     */
    @Override
    public void afterEach(ExtensionContext context)
    {
        Telemetry.install(OpenTelemetry.noop());
        sdk.close();
    }

    /**
     * The audit events so far, by name without the prefix, in order.
     *
     * @return the names
     */
    List<String> names()
    {
        return events.getFinishedLogRecordItems().stream()
                .map(LogRecordData::getEventName)
                .filter(name -> name != null && name.startsWith(PREFIX))
                .map(name -> name.substring(PREFIX.length()))
                .toList();
    }

    /**
     * How many times telemetry was flushed.
     *
     * @return the count
     */
    int flushes()
    {
        return flushes.get();
    }

    /**
     * The attributes of the first audit event of a name.
     *
     * @param name the event's name, without the prefix
     * @return its attributes, by key
     * @throws AssertionError if there was no such event
     */
    Map<String, Object> attributes(String name)
    {
        LogRecordData event = events.getFinishedLogRecordItems().stream()
                .filter(record -> (PREFIX + name).equals(
                        record.getEventName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No " + name));
        Map<String, Object> attributes = new LinkedHashMap<>();
        event.getAttributes().forEach((key, value) ->
                attributes.put(key.getKey(), value));
        return attributes;
    }
}
