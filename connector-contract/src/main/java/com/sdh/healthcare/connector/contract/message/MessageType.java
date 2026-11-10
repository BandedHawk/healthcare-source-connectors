/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import java.util.Arrays;

/**
 * The kinds of message the connectors publish. From a landing node to the
 * split stage: an item was landed. Between the stages (STAGES.md §4): a
 * record was split, stored or failed, and an item was split or converted.
 * To an intake's producer: an item was received, and its result. Each has
 * a fixed name used on the queue ({@link #wireName()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum MessageType
{

    /** A record was stored. */
    RECORD_STORED("record.stored"),

    /** A landing node landed an item, for its splitter. */
    ITEM_LANDED("item.landed"),

    /** A landing node's receipt for an item, for its producer. */
    ITEM_RECEIVED("item.received"),

    /** An item's final result, for its producer. */
    ITEM_RESULT("item.result"),

    /** The split stage stored a record as cut, for the convert stage. */
    RECORD_SPLIT("record.split"),

    /** The split stage finished an item, for the convert stage. */
    ITEM_SPLIT("item.split"),

    /** The convert stage stored every record of an item. */
    ITEM_CONVERTED("item.converted"),

    /** The transform stage mapped a record and stored the output. */
    RECORD_TRANSFORMED("record.transformed"),

    /** The transform stage stored every record of an item. */
    ITEM_TRANSFORMED("item.transformed"),

    /**
     * A stage handled every record of an item once, and some failed and are
     * still open; sent again whenever its counts change (STAGES.md §15.6).
     */
    ITEM_PROGRESS("item.progress"),

    /**
     * A stage stored a record before, and has now produced different output
     * for it; for the stage's dead-letter queue.
     */
    RECORD_CONFLICT("record.conflict"),

    /**
     * A stage's own request to process a record again (STAGES.md §15.8.4),
     * on its own input, signed by itself.
     */
    RECORD_REPROCESS("record.reprocess"),

    /**
     * The split stage's own request to split an item again, as its next
     * generation (STAGES.md §15.8.4), on its own input, signed by itself.
     */
    ITEM_REPROCESS("item.reprocess");

    /** The name used on the queue. */
    private final String wireName;

    /**
     * Creates a type.
     *
     * @param wireName the name used on the queue
     */
    MessageType(String wireName)
    {
        this.wireName = wireName;
    }

    /**
     * The name used on the queue.
     *
     * @return the name, such as {@code record.stored}
     */
    public String wireName()
    {
        return wireName;
    }

    /**
     * The type a queue name stands for.
     *
     * @param name the name used on the queue
     * @return the type
     * @throws IllegalArgumentException if the name is unknown
     */
    public static MessageType fromWire(String name)
    {
        return Arrays.stream(values())
                .filter(type -> type.wireName.equals(name))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException("Unknown message type"));
    }
}
