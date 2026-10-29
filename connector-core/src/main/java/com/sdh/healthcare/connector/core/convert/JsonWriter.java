package com.sdh.healthcare.connector.core.convert;

import java.nio.charset.StandardCharsets;

/**
 * Minimal deterministic JSON writer for the converters: compact (no whitespace), keys in the order written,
 * strings escaped per RFC 8259 with non-ASCII left as UTF-8. The caller keeps the structure valid.
 */
final class JsonWriter {
    private final StringBuilder sb;
    /** Whether the next value or key in the current container needs a comma before it. */
    private boolean comma;

    JsonWriter(int capacity) {
        sb = new StringBuilder(capacity);
    }

    JsonWriter beginObject() {
        separate();
        sb.append('{');
        comma = false;
        return this;
    }

    JsonWriter endObject() {
        sb.append('}');
        comma = true;
        return this;
    }

    JsonWriter beginArray() {
        separate();
        sb.append('[');
        comma = false;
        return this;
    }

    JsonWriter endArray() {
        sb.append(']');
        comma = true;
        return this;
    }

    /** Writes a key; the next call writes its value. */
    JsonWriter key(String k) {
        separate();
        string(k);
        sb.append(':');
        comma = false;
        return this;
    }

    JsonWriter value(String v) {
        separate();
        string(v);
        comma = true;
        return this;
    }

    /** A literal JSON value (true, false or a number); the caller guarantees it is valid JSON. */
    JsonWriter literal(String v) {
        separate();
        sb.append(v);
        comma = true;
        return this;
    }

    private void separate() {
        if (comma) {
            sb.append(',');
        }
    }

    private void string(String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    byte[] toBytes() {
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
