package com.sdh.healthcare.connector.core.split;

import java.util.HashMap;
import java.util.Map;

/**
 * Run-time split settings (design §6.2, §14).
 *
 * @param boundary        the format's section boundary; null means the splitter's default
 * @param options         format-specific options, keyed as in configuration (e.g. {@code csv.header})
 * @param limits          size and count limits
 * @param readBufferBytes read buffer size; 4 MiB by default (design §13)
 */
public record SplitConfig(String boundary, Map<String, String> options, SplitLimits limits, int readBufferBytes) {
    public static final int DEFAULT_READ_BUFFER = 4 << 20;

    public SplitConfig {
        options = Map.copyOf(options);
    }

    public static SplitConfig defaults(String format) {
        return new SplitConfig(null, Map.of(), SplitLimits.defaultsFor(format), DEFAULT_READ_BUFFER);
    }

    public SplitConfig withBoundary(String b) {
        return new SplitConfig(b, options, limits, readBufferBytes);
    }

    public SplitConfig withOption(String key, String value) {
        Map<String, String> m = new HashMap<>(options);
        m.put(key, value);
        return new SplitConfig(boundary, m, limits, readBufferBytes);
    }

    public SplitConfig withLimits(SplitLimits l) {
        return new SplitConfig(boundary, options, l, readBufferBytes);
    }

    public SplitConfig withReadBuffer(int bytes) {
        return new SplitConfig(boundary, options, limits, bytes);
    }

    public String boundaryOr(String fallback) {
        return boundary == null || boundary.isBlank() ? fallback : boundary;
    }

    public String option(String key, String fallback) {
        String v = options.get(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    public boolean booleanOption(String key, boolean fallback) {
        String v = options.get(key);
        return v == null || v.isBlank() ? fallback : Boolean.parseBoolean(v.trim());
    }

    public int intOption(String key, int fallback) {
        String v = options.get(key);
        return v == null || v.isBlank() ? fallback : Integer.parseInt(v.trim());
    }
}
