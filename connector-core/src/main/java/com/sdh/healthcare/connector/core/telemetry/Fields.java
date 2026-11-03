/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.LinkedHashMap;
import java.util.Map;

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
     * @return the pairs, such as {@code " file_id=... seq=3"}; empty if
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
