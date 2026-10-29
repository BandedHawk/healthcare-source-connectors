package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.LinkedHashMap;
import java.util.Map;

/** Telemetry attributes, restricted to {@link Attr} keys and to strings and numbers. Immutable. */
public final class Fields {
    private static final Fields EMPTY = new Fields(Map.of());

    private final Map<Attr, Object> values;

    private Fields(Map<Attr, Object> values) {
        this.values = values;
    }

    public static Fields of() {
        return EMPTY;
    }

    public static Fields of(Attr a, Object v) {
        return EMPTY.with(a, v);
    }

    /** Adds a value: {@link Number}s are recorded as longs, anything else (UUIDs, enums) as its string form. */
    public Fields with(Attr a, Object v) {
        if (v == null) {
            return this;
        }
        Map<Attr, Object> m = new LinkedHashMap<>(values);
        m.put(a, v instanceof Number n ? (Object) n.longValue() : v.toString());
        return new Fields(m);
    }

    public Fields with(Fields other) {
        Map<Attr, Object> m = new LinkedHashMap<>(values);
        m.putAll(other.values);
        return new Fields(m);
    }

    public Attributes toAttributes() {
        AttributesBuilder b = Attributes.builder();
        values.forEach((k, v) -> {
            if (v instanceof Long l) {
                b.put(k.key(), l);
            } else {
                b.put(k.key(), (String) v);
            }
        });
        return b.build();
    }

    /** {@code key=value} pairs for the console log, short key names. */
    String toLogString() {
        StringBuilder sb = new StringBuilder();
        values.forEach((k, v) -> sb.append(' ').append(k.key().substring(4)).append('=').append(v));
        return sb.toString();
    }
}
