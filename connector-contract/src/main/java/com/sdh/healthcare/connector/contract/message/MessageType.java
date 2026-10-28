package com.sdh.healthcare.connector.contract.message;

import java.util.Arrays;

public enum MessageType {
    FILE_STARTED("file.started"),
    SECTION_STORED("section.stored"),
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
