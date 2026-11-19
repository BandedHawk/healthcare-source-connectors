/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * The attributes of a span, a metric or an audit event. The keys can only be
 * {@link Attr}s, and the values only whole numbers or strings, so nothing
 * else can reach telemetry. Each method that adds a value returns a new
 * instance. Immutable.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Fields
{

    /** No attributes. */
    private static final Fields EMPTY = new Fields(Map.of());

    /**
     * How many frames of a failure's innermost cause {@link #failure}
     * keeps: the throw and the two calls that led to it, enough to place a
     * check such as {@code Preconditions.checkState} in its caller.
     */
    static final int FRAMES = 3;

    /**
     * The values, by attribute, in the order they were added: each a
     * {@link Long} or a {@link String}.
     */
    private final Map<Attr, Object> values;

    /**
     * Creates attributes from their values.
     *
     * @param values the values, by attribute; not copied
     */
    private Fields(Map<Attr, Object> values)
    {
        this.values = values;
    }

    /**
     * No attributes.
     *
     * @return the empty instance
     */
    public static Fields of()
    {
        return EMPTY;
    }

    /**
     * One attribute.
     *
     * @param attribute the attribute
     * @param value     its value, as {@link #with(Attr, Object)} takes it
     * @return the attributes
     */
    public static Fields of(Attr attribute, Object value)
    {
        return EMPTY.with(attribute, value);
    }

    /**
     * Adds an attribute. A {@link Number} is recorded as a whole number
     * (a long), and anything else (a UUID, an enum) as its string form.
     *
     * @param attribute the attribute; replaces any value it already has
     * @param value     its value; null adds nothing
     * @return these attributes with the new one
     */
    public Fields with(Attr attribute, Object value)
    {
        if (value == null)
        {
            return this;
        }
        Map<Attr, Object> combined = new LinkedHashMap<>(values);
        combined.put(attribute, value instanceof Number number
                ? (Object) number.longValue()
                : value.toString());
        return new Fields(combined);
    }

    /**
     * Adds other attributes.
     *
     * @param other the attributes to add; they replace any of these with the
     *              same key
     * @return these attributes with the others
     */
    public Fields with(Fields other)
    {
        Map<Attr, Object> combined = new LinkedHashMap<>(values);
        combined.putAll(other.values);
        return new Fields(combined);
    }

    /**
     * Adds what can be told of a failure without its messages, which could
     * hold PHI: its class ({@link Attr#REASON}), the classes of its chain
     * of causes ({@link Attr#CAUSES}), and the top {@value #FRAMES} frames
     * of the innermost cause ({@link Attr#THROWN_AT}), which name only code.
     * No message, of the failure or any cause, is read. A chain that comes
     * round to a cause already seen ends there.
     *
     * @param failure the failure; null adds nothing
     * @return these attributes with the failure's
     */
    public Fields failure(Throwable failure)
    {
        if (failure == null)
        {
            return this;
        }
        StringJoiner causes = new StringJoiner(",");
        Set<Throwable> seen = Collections.newSetFromMap(
                new IdentityHashMap<>());
        Throwable innermost = failure;
        for (Throwable cause = failure; cause != null && seen.add(cause);
                cause = cause.getCause())
        {
            causes.add(cause.getClass().getSimpleName());
            innermost = cause;
        }
        StringJoiner frames = new StringJoiner(",");
        StackTraceElement[] trace = innermost.getStackTrace();
        for (int i = 0; i < Math.min(FRAMES, trace.length); i++)
        {
            frames.add(trace[i].getClassName() + "." + trace[i].getMethodName()
                    + (trace[i].getLineNumber() >= 0
                            ? ":" + trace[i].getLineNumber() : ""));
        }
        Fields added = with(Attr.REASON, failure.getClass().getSimpleName())
                .with(Attr.CAUSES, causes.toString());
        return trace.length == 0 ? added
                : added.with(Attr.THROWN_AT, frames.toString());
    }

    /**
     * Turns the attributes into OpenTelemetry's form.
     *
     * @return the OpenTelemetry attributes
     */
    public Attributes toAttributes()
    {
        AttributesBuilder builder = Attributes.builder();
        values.forEach((attribute, value) ->
        {
            if (value instanceof Long number)
            {
                builder.put(attribute.key(), number);
            }
            else
            {
                builder.put(attribute.key(), (String) value);
            }
        });
        return builder.build();
    }

    /**
     * Writes the attributes for the console log: {@code key=value} pairs,
     * each after a space, with the key's {@code sdh.} prefix left off.
     *
     * @return the pairs, such as {@code " item_id=... seq=3"}; empty if
     *         there are none
     */
    String toLogString()
    {
        StringBuilder text = new StringBuilder();
        values.forEach((attribute, value) -> text.append(' ')
                .append(attribute.key().substring("sdh.".length()))
                .append('=').append(value));
        return text.toString();
    }
}
