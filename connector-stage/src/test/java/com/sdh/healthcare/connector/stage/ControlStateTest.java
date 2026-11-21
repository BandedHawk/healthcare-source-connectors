/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The pause's state (CONTROL-API-DESIGN.md §9), and the watch that tells a
 * pod's input of it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlStateTest
{

    /** A reason, synthetic, within the reason's characters. */
    static final String REASON = "Ticket OPS-77: upgrade window";

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlStateTest()
    {
    }

    /**
     * Without its table, nothing is paused and nothing to resume. A pause
     * keeps who, when and why; pausing again keeps the first; resuming
     * clears it, once. Each scope is its own. A reason outside its
     * characters is refused.
     */
    @Test
    void pausesAndResumesAScope()
    {
        String schema = StageTestSupport.freshSchema();
        ControlState state = new ControlState(StageTestSupport.database(),
                schema, "convert/test");
        ControlState other = new ControlState(StageTestSupport.database(),
                schema, "transform/test");
        assertThat(state.paused()).isEmpty();
        assertThat(state.resume()).isEmpty();

        ControlState.Paused paused = state.pause("olga", REASON);
        assertThat(paused.scope()).isEqualTo("convert/test");
        assertThat(paused.pausedBy()).isEqualTo("olga");
        assertThat(paused.reason()).isEqualTo(REASON);
        assertThat(paused.pausedAt()).isNotNull();
        assertThat(state.paused()).contains(paused);
        assertThat(other.paused()).isEmpty();

        assertThat(state.pause("oscar", "Another reason")).isEqualTo(paused);

        assertThat(state.resume()).contains(paused);
        assertThat(state.paused()).isEmpty();
        assertThat(state.resume()).isEmpty();
        assertThat(state.pause("oscar", "Another reason").pausedBy())
                .isEqualTo("oscar");

        assertThatThrownBy(() -> state.pause("olga", "line\nbreak"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> state.pause("olga", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ControlState(
                StageTestSupport.database(), "bad-schema", "convert/test"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The watch reads the state as it starts, before the pod takes work,
     * and tells each of every change, in order, once; it audits it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void tellsTheInputOfEachChange() throws Exception
    {
        AtomicReference<Optional<ControlState.Paused>> now =
                new AtomicReference<>(Optional.of(paused()));
        List<String> told = new ArrayList<>();
        try (PauseWatch watch = new PauseWatch(now::get, "convert/test",
                Duration.ofMillis(20), List.of(
                        paused -> told.add("input " + paused),
                        paused -> told.add("worker " + paused))))
        {
            watch.start();
            assertThat(watch.paused()).isTrue();
            assertThat(told).containsExactly("input true", "worker true");

            now.set(Optional.empty());
            waitFor(() -> !watch.paused());
            assertThat(told).containsExactly("input true", "worker true",
                    "input false", "worker false");
        }
        assertThat(audit.names()).containsSubsequence("stage.paused",
                "stage.resumed");
        assertThat(audit.attributes("stage.paused"))
                .containsEntry("sdh.control_scope", "convert/test");
    }

    /**
     * A read that fails keeps the state last read; one that is applied
     * but fails is told again at the next read, until it takes.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keepsTheStateWhenAReadOrAChangeFails() throws Exception
    {
        AtomicBoolean unreadable = new AtomicBoolean(true);
        AtomicBoolean refuses = new AtomicBoolean(true);
        List<Boolean> told = new ArrayList<>();
        PauseWatch.Reader reader = () ->
        {
            if (unreadable.get())
            {
                throw new IllegalStateException("down");
            }
            return Optional.of(paused());
        };
        try (PauseWatch watch = new PauseWatch(reader, "convert/test",
                Duration.ofMillis(20), List.of(paused ->
                {
                    synchronized (told)
                    {
                        told.add(paused);
                    }
                    if (refuses.get())
                    {
                        throw new IllegalStateException("not now");
                    }
                })))
        {
            watch.start();
            assertThat(watch.paused()).isFalse();
            assertThat(told).isEmpty();

            unreadable.set(false);
            waitFor(() ->
            {
                synchronized (told)
                {
                    return told.size() >= 2;
                }
            });
            assertThat(watch.paused()).isFalse();
            refuses.set(false);
            waitFor(watch::paused);
            synchronized (told)
            {
                assertThat(told).containsOnly(true);
            }
        }
    }

    /**
     * A pause, as read.
     *
     * @return it
     */
    static ControlState.Paused paused()
    {
        return new ControlState.Paused("convert/test", "olga",
                Instant.now(), REASON);
    }

    /**
     * Waits, for at most 5 seconds, for something to hold.
     *
     * @param holds what should hold
     * @throws InterruptedException if interrupted
     */
    static void waitFor(BooleanSupplier holds)
            throws InterruptedException
    {
        long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!holds.getAsBoolean() && System.nanoTime() < until)
        {
            Thread.sleep(10);
        }
        assertThat(holds.getAsBoolean()).as("held in time").isTrue();
    }
}
