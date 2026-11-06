/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.landing.TelemetryCapture;
import io.opentelemetry.api.common.AttributeKey;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link IntakeHealth}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IntakeHealthTest
{

    /** The reason attribute of {@code intake.unreachable}. */
    static final AttributeKey<String> REASON =
            AttributeKey.stringKey("sdh.reason");

    /** Captures the audit events. */
    @RegisterExtension
    final TelemetryCapture telemetry = new TelemetryCapture();

    /** The fake clock, in nanoseconds. */
    final AtomicLong now = new AtomicLong();

    /** The health under test. */
    final IntakeHealth health = new IntakeHealth("claims", now::get);

    /**
     * Moves the clock on.
     *
     * @param seconds how far
     */
    void advance(int seconds)
    {
        now.addAndGet(Duration.ofSeconds(seconds).toNanos());
    }

    /**
     * An outage is reported once per reason, timed from its start, and its
     * end once, after which the source counts as just answered.
     */
    @Test
    void reportsEachReasonOnceAndTimesTheOutage()
    {
        assertThat(health.reachable()).isTrue();
        advance(30);
        assertThat(health.secondsSinceAnswered()).isEqualTo(30);
        assertThat(health.outageSeconds()).isZero();

        health.unreachable(IntakeProblem.AUTH);
        advance(10);
        health.unreachable(IntakeProblem.AUTH);
        assertThat(health.reachable()).isFalse();
        assertThat(health.outageSeconds()).isEqualTo(10);
        health.unreachable(IntakeProblem.NETWORK);
        advance(5);
        assertThat(health.outageSeconds()).as("timed from the first reason")
                .isEqualTo(15);
        assertThat(health.secondsSinceAnswered()).isEqualTo(45);

        health.answered();
        health.answered();
        assertThat(health.reachable()).isTrue();
        assertThat(health.outageSeconds()).isZero();
        assertThat(health.secondsSinceAnswered()).isZero();
        assertThat(telemetry.events()).containsExactly("intake.unreachable",
                "intake.unreachable", "intake.reachable");
        assertThat(telemetry.attributesOf("intake.unreachable"))
                .extracting(attributes -> attributes.get(REASON))
                .containsExactly("AUTH", "NETWORK");
    }

    /** A directory's outage has no reason. */
    @Test
    void reportsADirectoryWithoutAReason()
    {
        health.unreachable((IntakeProblem) null);
        health.unreachable((IntakeProblem) null);
        assertThat(telemetry.attributesOf("intake.unreachable"))
                .singleElement()
                .satisfies(attributes ->
                        assertThat(attributes.get(REASON)).isNull());
    }
}
