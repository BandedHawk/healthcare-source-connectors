package com.sdh.healthcare.connector.core.telemetry;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The only attribute keys telemetry may carry (design §12): identifiers, codes, offsets and counts, never content,
 * filenames or exception messages. {@link Fields} accepts nothing else, and the application's exporters drop any
 * other key that reaches them from elsewhere.
 */
public enum Attr {
    FILE_ID("sdh.file_id"),
    FORMAT("sdh.format"),
    STATE("sdh.state"),
    STAGE("sdh.stage"),
    ERROR_CODE("sdh.error_code"),
    RESULT("sdh.result"),
    REASON("sdh.reason"),
    SEQ("sdh.seq"),
    OFFSET("sdh.offset"),
    COUNT("sdh.count"),
    BYTES("sdh.bytes"),
    EPOCH("sdh.epoch"),
    TABLE("sdh.table"),
    ARCHIVE("sdh.archive");

    private final String key;

    Attr(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    /** Allowed keys, plus {@code event.name} which OpenTelemetry uses to name events. */
    public static final Set<String> ALLOWED_KEYS = Arrays.stream(values()).map(Attr::key)
            .collect(Collectors.collectingAndThen(Collectors.toSet(), s -> {
                s.add("event.name");
                return Set.copyOf(s);
            }));
}
