/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link InboundGauge}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class InboundGaugeTest {

    @Test
    void inboundAgeGaugeIgnoresHiddenFilesAndMarkers(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        assertThat(InboundGauge.oldestAgeSeconds(dir)).isZero();
        var hidden = java.nio.file.Files.writeString(dir.resolve(".tmp"), "x");
        java.nio.file.Files.setLastModifiedTime(hidden,
                java.nio.file.attribute.FileTime.from(java.time.Instant.now().minusSeconds(9999)));
        var f = java.nio.file.Files.writeString(dir.resolve("a.csv"), "x");
        java.nio.file.Files.setLastModifiedTime(f,
                java.nio.file.attribute.FileTime.from(java.time.Instant.now().minusSeconds(120)));
        assertThat(InboundGauge.oldestAgeSeconds(dir)).isBetween(119L, 130L);
    }
}
