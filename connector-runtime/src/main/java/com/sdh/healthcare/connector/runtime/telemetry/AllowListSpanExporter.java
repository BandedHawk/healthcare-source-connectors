/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import io.opentelemetry.api.common.AttributeKey;
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
 * The last line of PHI defence for traces (design §12): whatever produced a
 * span, only allow-listed attribute keys are exported. Span events
 * (including exception events, whose messages could hold PHI) and link
 * attributes are dropped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AllowListSpanExporter implements SpanExporter
{

    /** Exports the cleaned spans. */
    private final SpanExporter delegate;

    /**
     * Wraps an exporter.
     *
     * @param delegate exports the cleaned spans
     */
    public AllowListSpanExporter(SpanExporter delegate)
    {
        this.delegate = delegate;
    }

    /**
     * Keeps only the allow-listed attributes ({@link Attr#ALLOWED_KEYS}).
     *
     * @param attributes the attributes
     * @return those with allowed keys
     */
    static Attributes filter(Attributes attributes)
    {
        AttributesBuilder kept = Attributes.builder();
        attributes.forEach((key, value) ->
        {
            if (Attr.ALLOWED_KEYS.contains(key.getKey()))
            {
                @SuppressWarnings("unchecked")
                AttributeKey<Object> anyKey = (AttributeKey<Object>) key;
                kept.put(anyKey, value);
            }
        });
        return kept.build();
    }

    /**
     * A span with only allowed attributes, no events, and links without
     * attributes.
     *
     * @param span the span
     * @return the cleaned span
     */
    static SpanData clean(SpanData span)
    {
        Attributes attributes = filter(span.getAttributes());
        return new DelegatingSpanData(span)
        {
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
            public List<EventData> getEvents()
            {
                return List.of();
            }

            @Override
            public int getTotalRecordedEvents()
            {
                return 0;
            }

            @Override
            public List<LinkData> getLinks()
            {
                return span.getLinks().stream()
                        .map(link -> LinkData.create(link.getSpanContext()))
                        .toList();
            }
        };
    }

    /**
     * Exports the spans, cleaned.
     *
     * @param spans the spans
     * @return the delegate's result
     */
    @Override
    public CompletableResultCode export(Collection<SpanData> spans)
    {
        return delegate.export(
                spans.stream().map(AllowListSpanExporter::clean).toList());
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
}
