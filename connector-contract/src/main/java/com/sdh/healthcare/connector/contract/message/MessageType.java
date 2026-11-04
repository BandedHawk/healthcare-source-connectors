/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import java.util.Arrays;

/**
 * The kinds of message the connectors publish. About a file, in the order
 * they happen: the file started, a section was stored or failed, and the
 * file completed. From a landing node to its splitter: an item was landed.
 * To an intake's producer: an item was received, and its result. Each has
 * a fixed name used on the queue ({@link #wireName()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum MessageType
{

    /** A file was picked up, or resumed with a new run. */
    FILE_STARTED("file.started"),

    /** A section was stored. */
    SECTION_STORED("section.stored"),

    /** A section failed conversion, and was stored as split. */
    SECTION_FAILED("section.failed"),

    /** Every section of a file was stored and published. */
    FILE_COMPLETED("file.completed"),

    /** A landing node landed an item, for its splitter. */
    ITEM_LANDED("item.landed"),

    /** A landing node's receipt for an item, for its producer. */
    ITEM_RECEIVED("item.received"),

    /** An item's final result, for its producer. */
    ITEM_RESULT("item.result");

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
     * @return the name, such as {@code file.started}
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
