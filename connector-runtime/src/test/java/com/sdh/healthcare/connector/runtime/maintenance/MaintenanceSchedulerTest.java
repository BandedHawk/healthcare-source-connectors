/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for the checks in {@link MaintenanceScheduler}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class MaintenanceSchedulerTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    MaintenanceSchedulerTest()
    {
    }

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

    /**
     * Passes put together take turns while both are due, each told of its
     * own purge's run; one not due is passed over.
     */
    @Test
    void passesTakeTurns()
    {
        Counting rekey = new Counting();
        Counting purge = new Counting();
        MaintenanceScheduler.Pass both = MaintenanceScheduler.Pass.of(rekey,
                purge);
        List<TableMaintenance.Purge> runs = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            TableMaintenance.Purge due = both.due().orElseThrow();
            runs.add(due);
            both.ran(due, List.of());
        }
        assertThat(runs).containsExactly(rekey.purge, purge.purge,
                rekey.purge, purge.purge);
        assertThat(rekey.ran).isEqualTo(2);
        assertThat(purge.ran).isEqualTo(2);
        rekey.due = false;
        TableMaintenance.Purge due = both.due().orElseThrow();
        assertThat(due).isSameAs(purge.purge);
        both.failed(due, new IllegalStateException("down"));
        assertThat(purge.failed).isEqualTo(1);
        purge.due = false;
        assertThat(both.due()).isEmpty();
    }

    /** A pass that counts what it is told. */
    static final class Counting implements MaintenanceScheduler.Pass
    {

        /** Its purge, which does nothing. */
        final TableMaintenance.Purge purge = (table, now) -> 0;

        /** Whether it is due. */
        boolean due = true;

        /** Runs told of. */
        int ran;

        /** Failures told of. */
        int failed;

        /** Creates the pass. */
        Counting()
        {
        }

        @Override
        public Optional<TableMaintenance.Purge> due()
        {
            return due ? Optional.of(purge) : Optional.empty();
        }

        @Override
        public void ran(TableMaintenance.Purge given,
                List<TableMaintenance.Report> reports)
        {
            assertThat(given).isSameAs(purge);
            ran++;
        }

        @Override
        public void failed(TableMaintenance.Purge given,
                RuntimeException failure)
        {
            assertThat(given).isSameAs(purge);
            failed++;
        }
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
