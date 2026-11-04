/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.contract.message;

import java.util.Arrays;

/**
 * The kinds of message the connectors publish. About a file, in the order they happen: the file
 * started, a section was stored or failed, and the file completed. From a landing node to its
 * splitter: an item was landed. To an intake's producer: an item was received, and its result.
 * Each has a fixed name used on the queue ({@link #wireName()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum MessageType {
    FILE_STARTED("file.started"),
    SECTION_STORED("section.stored"),
    SECTION_FAILED("section.failed"),
    FILE_COMPLETED("file.completed"),
    ITEM_LANDED("item.landed"),
    ITEM_RECEIVED("item.received"),
    ITEM_RESULT("item.result");

    private final String wireName;

    MessageType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static MessageType fromWire(String name) {
        return Arrays.stream(values())
                .filter(t -> t.wireName.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown message type"));
    }
}
