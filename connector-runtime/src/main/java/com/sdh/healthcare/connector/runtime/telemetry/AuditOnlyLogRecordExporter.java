/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Value;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.logs.data.Body;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Collection;
import java.util.List;

/**
 * Exports only audit events ({@code sdh.audit.*}), and only their
 * allow-listed attributes (design §12). Free-text log lines, from any
 * library, never leave the host this way: their text could contain a path
 * or record content.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AuditOnlyLogRecordExporter implements LogRecordExporter
{

    /** The event-name prefix of audit events. */
    static final String PREFIX = "sdh.audit.";

    /** Exports the audit events. */
    private final LogRecordExporter delegate;

    /**
     * Wraps an exporter.
     *
     * @param delegate exports the audit events
     */
    public AuditOnlyLogRecordExporter(LogRecordExporter delegate)
    {
        this.delegate = delegate;
    }

    /**
     * Exports the audit events among the records, with only their allowed
     * attributes, and drops the rest.
     *
     * @param logs the records
     * @return the delegate's result; success if there were no audit events
     */
    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs)
    {
        List<LogRecordData> audit = logs.stream()
                .filter(log -> log.getEventName() != null
                        && log.getEventName().startsWith(PREFIX))
                .map(log -> (LogRecordData) new Cleaned(log,
                        AllowListSpanExporter.filter(log.getAttributes())))
                .toList();
        return audit.isEmpty()
                ? CompletableResultCode.ofSuccess() : delegate.export(audit);
    }

    /**
     * Flushes the delegate.
     *
     * @return its result
     */
    @Override
    public CompletableResultCode flush()
    {
        return delegate.flush();
    }

    /**
     * Shuts the delegate down.
     *
     * @return its result
     */
    @Override
    public CompletableResultCode shutdown()
    {
        return delegate.shutdown();
    }

    /**
     * A record with filtered attributes. The body of an audit record is its
     * event name, without the prefix.
     *
     * @param original   the record
     * @param attributes its allowed attributes
     */
    private record Cleaned(LogRecordData original, Attributes attributes)
            implements LogRecordData
    {

        @Override
        public Resource getResource()
        {
            return original.getResource();
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo()
        {
            return original.getInstrumentationScopeInfo();
        }

        @Override
        public long getTimestampEpochNanos()
        {
            return original.getTimestampEpochNanos();
        }

        @Override
        public long getObservedTimestampEpochNanos()
        {
            return original.getObservedTimestampEpochNanos();
        }

        @Override
        public SpanContext getSpanContext()
        {
            return original.getSpanContext();
        }

        @Override
        public Severity getSeverity()
        {
            return original.getSeverity();
        }

        @Override
        public String getSeverityText()
        {
            return original.getSeverityText();
        }

        @Override
        @SuppressWarnings("deprecation")
        public Body getBody()
        {
            return Body.string(
                    original.getEventName().substring(PREFIX.length()));
        }

        @Override
        public Value<?> getBodyValue()
        {
            return Value.of(
                    original.getEventName().substring(PREFIX.length()));
        }

        @Override
        public Attributes getAttributes()
        {
            return attributes;
        }

        @Override
        public int getTotalAttributeCount()
        {
            return attributes.size();
        }

        @Override
        public String getEventName()
        {
            return original.getEventName();
        }
    }
}
