package com.sdh.healthcare.connector.file.telemetry;

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

/**
 * Exports only audit events ({@code sdh.audit.*}) and only their allow-listed attributes (design §12). Free-text log
 * lines, from any library, never leave the host this way: their text could contain a path or record content.
 */
public final class AuditOnlyLogRecordExporter implements LogRecordExporter {
    static final String PREFIX = "sdh.audit.";

    private final LogRecordExporter delegate;

    public AuditOnlyLogRecordExporter(LogRecordExporter delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableResultCode export(Collection<LogRecordData> logs) {
        var audit = logs.stream()
                .filter(l -> l.getEventName() != null && l.getEventName().startsWith(PREFIX))
                .map(l -> (LogRecordData) new Cleaned(l, AllowListSpanExporter.filter(l.getAttributes())))
                .toList();
        return audit.isEmpty() ? CompletableResultCode.ofSuccess() : delegate.export(audit);
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    /** The record with filtered attributes. The body of an audit record is its event name. */
    private record Cleaned(LogRecordData in, Attributes attrs) implements LogRecordData {
        @Override
        public Resource getResource() {
            return in.getResource();
        }

        @Override
        public InstrumentationScopeInfo getInstrumentationScopeInfo() {
            return in.getInstrumentationScopeInfo();
        }

        @Override
        public long getTimestampEpochNanos() {
            return in.getTimestampEpochNanos();
        }

        @Override
        public long getObservedTimestampEpochNanos() {
            return in.getObservedTimestampEpochNanos();
        }

        @Override
        public SpanContext getSpanContext() {
            return in.getSpanContext();
        }

        @Override
        public Severity getSeverity() {
            return in.getSeverity();
        }

        @Override
        public String getSeverityText() {
            return in.getSeverityText();
        }

        @Override
        @SuppressWarnings("deprecation")
        public Body getBody() {
            return Body.string(in.getEventName().substring(PREFIX.length()));
        }

        @Override
        public Value<?> getBodyValue() {
            return Value.of(in.getEventName().substring(PREFIX.length()));
        }

        @Override
        public Attributes getAttributes() {
            return attrs;
        }

        @Override
        public int getTotalAttributeCount() {
            return attrs.size();
        }

        @Override
        public String getEventName() {
            return in.getEventName();
        }
    }
}
