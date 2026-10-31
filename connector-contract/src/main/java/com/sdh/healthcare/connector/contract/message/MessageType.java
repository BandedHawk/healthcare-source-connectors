/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.contract.message;

import java.util.Arrays;

/**
 * The four kinds of message the connector publishes about a file, in the order they happen: the
 * file started, a section was stored or failed, and the file completed. Each has a fixed name used
 * on the queue ({@link #wireName()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum MessageType {
    FILE_STARTED("file.started"),
    SECTION_STORED("section.stored"),
    SECTION_FAILED("section.failed"),
    FILE_COMPLETED("file.completed");

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
