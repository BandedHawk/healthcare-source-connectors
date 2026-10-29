package com.sdh.healthcare.connector.file.telemetry;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;

/**
 * Last line of PHI defence for traces (design §12): whatever produced a span, only allow-listed attribute keys are
 * exported. Span events (including exception events, whose messages could hold PHI) and link attributes are dropped.
 */
public final class AllowListSpanExporter implements SpanExporter {
    private final SpanExporter delegate;

    public AllowListSpanExporter(SpanExporter delegate) {
        this.delegate = delegate;
    }

    static Attributes filter(Attributes in) {
        AttributesBuilder b = Attributes.builder();
        in.forEach((k, v) -> {
            if (Attr.ALLOWED_KEYS.contains(k.getKey())) {
                @SuppressWarnings("unchecked")
                io.opentelemetry.api.common.AttributeKey<Object> key = (io.opentelemetry.api.common.AttributeKey<Object>) k;
                b.put(key, v);
            }
        });
        return b.build();
    }

    static SpanData clean(SpanData span) {
        Attributes attrs = filter(span.getAttributes());
        return new DelegatingSpanData(span) {
            @Override
            public Attributes getAttributes() {
                return attrs;
            }

            @Override
            public int getTotalAttributeCount() {
                return attrs.size();
            }

            @Override
            public List<EventData> getEvents() {
                return List.of();
            }

            @Override
            public int getTotalRecordedEvents() {
                return 0;
            }

            @Override
            public List<LinkData> getLinks() {
                return span.getLinks().stream().map(l -> LinkData.create(l.getSpanContext())).toList();
            }
        };
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        return delegate.export(spans.stream().map(AllowListSpanExporter::clean).toList());
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }
}
