/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for the checks in {@link MaintenanceScheduler}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class MaintenanceSchedulerTest
{

    /** A retention below the minimum is refused, naming the setting. */
    @Test
    void refusesRetentionBelowTheMinimum()
    {
        assertThatThrownBy(() -> MaintenanceScheduler.retention(
                "sdh.maintenance.retention.landed-items", Duration.ofDays(6),
                Duration.ofDays(7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("sdh.maintenance.retention.landed-items"
                        + " must be at least 7 day(s)");
    }

    /** No retention keeps rows forever; the minimum itself is allowed. */
    @Test
    void acceptsNoRetentionAndTheMinimum()
    {
        assertThat(MaintenanceScheduler.retention("r", null,
                Duration.ofDays(7))).isEmpty();
        assertThat(MaintenanceScheduler.retention("r", Duration.ofDays(7),
                Duration.ofDays(7))).contains(Duration.ofDays(7));
    }

    /** The interval must be positive. */
    @Test
    void refusesANonPositiveInterval()
    {
        assertThatThrownBy(() -> new MaintenanceScheduler.Schedule(true,
                Duration.ZERO, true, Duration.ofHours(24)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
