/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import java.util.Arrays;

/**
 * What kind of thing a landed item is (design §5.3.2). It is recorded with
 * the item and sent in {@code item.landed}, so the splitter knows, for
 * example, that a message batch has a record index.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum ItemKind
{

    /** A file from a directory or share. */
    FILE("file"),

    /** An object from a bucket. */
    OBJECT("object"),

    /** The body of an HTTP push. */
    PUSH("push"),

    /** A batch of messages from a queue. */
    MESSAGE_BATCH("message-batch"),

    /** A data file of an Iceberg table. */
    TABLE_FILE("table-file");

    /** The kind's name in tables and messages. */
    private final String wireName;

    /**
     * Creates a kind.
     *
     * @param wireName its name in tables and messages
     */
    ItemKind(String wireName)
    {
        this.wireName = wireName;
    }

    /**
     * The kind's name in tables and messages.
     *
     * @return the name, such as {@code message-batch}
     */
    public String wireName()
    {
        return wireName;
    }

    /**
     * Finds a kind by its name in tables and messages.
     *
     * @param name the name, such as {@code file}
     * @return the kind
     * @throws IllegalArgumentException if no kind has that name
     */
    public static ItemKind fromWire(String name)
    {
        return Arrays.stream(values())
                .filter(kind -> kind.wireName.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown item kind"));
    }
}
