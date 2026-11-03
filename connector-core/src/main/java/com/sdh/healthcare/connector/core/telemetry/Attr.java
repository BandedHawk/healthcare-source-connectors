/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The only attribute keys telemetry may carry (design §12): identifiers,
 * codes, offsets and counts, never content, filenames or exception messages.
 * {@link Fields} accepts nothing else, and the application's exporters drop
 * any other key that reaches them from elsewhere.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum Attr
{

    /** The file's ID, a UUID: never its name. */
    FILE_ID("sdh.file_id"),

    /** The file's format, such as {@code ccda} or {@code x12}. */
    FORMAT("sdh.format"),

    /** The state of a file's checkpoint, such as {@code ARCHIVING}. */
    STATE("sdh.state"),

    /**
     * The stage of processing, such as {@code intake}, {@code split} or
     * {@code archive}.
     */
    STAGE("sdh.stage"),

    /** An {@link ErrorCode}. */
    ERROR_CODE("sdh.error_code"),

    /** The outcome of an operation, such as {@code ok} or {@code failed}. */
    RESULT("sdh.result"),

    /**
     * Why something happened, such as an exception's class name: never an
     * exception's message.
     */
    REASON("sdh.reason"),

    /** A section's sequence number in its file, from 0. */
    SEQ("sdh.seq"),

    /** A byte offset in the file. */
    OFFSET("sdh.offset"),

    /** How many of something, such as sections or messages. */
    COUNT("sdh.count"),

    /** A size in bytes. */
    BYTES("sdh.bytes"),

    /** The lease epoch, which goes up each time an instance takes the lease. */
    EPOCH("sdh.epoch"),

    /** The name of an Iceberg table. */
    TABLE("sdh.table"),

    /** Where a file was archived, such as {@code FAILED_FILES}. */
    ARCHIVE("sdh.archive"),

    /**
     * Who ran an operator command (design §11.4): an account name, never a
     * person's details.
     */
    OPERATOR("sdh.operator"),

    /**
     * A byte position within a section, such as where conversion failed
     * (design §6.4).
     */
    POSITION("sdh.position"),

    /**
     * The name of a landing intake (design §5.3), such as
     * {@code claims-sftp}: chosen in configuration, never a path or host.
     */
    INTAKE("sdh.intake"),

    /** A landed item's ID, a UUID: never its name or path. */
    ITEM_ID("sdh.item_id"),

    /** What kind of item was landed, such as {@code file}. */
    KIND("sdh.kind");

    /**
     * The keys telemetry may carry: those of the attributes above, and
     * {@code event.name}, which OpenTelemetry uses to name events.
     */
    public static final Set<String> ALLOWED_KEYS = allowedKeys();

    /** The attribute's key, as exporters see it. */
    private final String key;

    /**
     * Creates an attribute.
     *
     * @param key the attribute's key
     */
    Attr(String key)
    {
        this.key = key;
    }

    /**
     * The attribute's key, as exporters see it.
     *
     * @return the key, such as {@code sdh.file_id}
     */
    public String key()
    {
        return key;
    }

    /**
     * Collects the allowed keys.
     *
     * @return every attribute's key, and {@code event.name}
     */
    private static Set<String> allowedKeys()
    {
        Set<String> keys = Arrays.stream(values()).map(Attr::key)
                .collect(Collectors.toCollection(HashSet::new));
        keys.add("event.name");
        return Set.copyOf(keys);
    }
}
