/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How long a stage keeps its rows (CONTROL-API-DESIGN.md §11, P4): its
 * setting's retention, until an operator sets another through the control
 * API ({@link ControlState#setRetention}), within the bounds its settings
 * give. Maintenance reads it at each run, so a change takes effect at the
 * next run without a redeploy.
 *
 * <p>It errs on keeping: a retention below the minimum is taken as the
 * minimum, and one that can't be read keeps every row that run.
 *
 * @param setting the setting's retention; empty keeps rows forever
 * @param minimum the shortest allowed: the stage's floor, or the bounds'
 *                minimum if longer
 * @param maximum the longest allowed; empty allows keeping rows forever
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record RetentionPolicy(Optional<Duration> setting, Duration minimum,
        Optional<Duration> maximum)
{

    /** Logs a retention that can't be read, by the failure's class. */
    private static final Logger LOG = LoggerFactory.getLogger(
            RetentionPolicy.class);

    /**
     * Checks the parts.
     *
     * @param setting the setting's retention
     * @param minimum the shortest allowed
     * @param maximum the longest allowed
     */
    public RetentionPolicy
    {
        Objects.requireNonNull(setting, "setting");
        Objects.requireNonNull(minimum, "minimum");
        Objects.requireNonNull(maximum, "maximum");
    }

    /**
     * A stage's policy, from its settings.
     *
     * @param setting the setting's retention; null keeps rows forever
     * @param floor   the stage's own shortest retention
     * @param bounds  the bounds settings; null for none
     * @return the policy
     */
    public static RetentionPolicy of(Duration setting, Duration floor,
            CommonSettings.RetentionBounds bounds)
    {
        Duration minimum = floor;
        Optional<Duration> maximum = Optional.empty();
        if (bounds != null)
        {
            if (bounds.minimum() != null
                    && bounds.minimum().compareTo(floor) > 0)
            {
                minimum = bounds.minimum();
            }
            maximum = Optional.ofNullable(bounds.maximum());
        }
        return new RetentionPolicy(Optional.ofNullable(setting), minimum,
                maximum);
    }

    /**
     * The retention in force: the one an operator set, else the
     * setting's, never below the minimum.
     *
     * @param state the scope's state
     * @return how long rows are kept; empty keeps them forever
     * @throws IllegalStateException if the state can't be read
     */
    public Optional<Duration> inForce(ControlState state)
    {
        Optional<Duration> keep = state.retention()
                .map(ControlState.Retention::keep).orElse(setting);
        return keep.map(each -> each.compareTo(minimum) < 0 ? minimum
                : each);
    }

    /**
     * The retention for a maintenance run, read as it starts: one that
     * can't be read keeps every row this run.
     *
     * @param state the scope's state
     * @return reads it
     */
    public Supplier<Optional<Duration>> forRuns(ControlState state)
    {
        return () ->
        {
            try
            {
                return inForce(state);
            }
            catch (RuntimeException failure)
            {
                LOG.warn("maintenance.retention_unread scope={} error={}",
                        state.scope(), failure.getClass().getSimpleName());
                return Optional.empty();
            }
        };
    }

    /**
     * Checks a retention asked for is within the bounds.
     *
     * @param keep how long; empty keeps rows forever
     * @return it
     * @throws IllegalArgumentException if it isn't within them, naming
     *                                  them
     */
    public Optional<Duration> check(Optional<Duration> keep)
    {
        if (keep.isEmpty())
        {
            if (maximum.isPresent())
            {
                throw new IllegalArgumentException("Rows can't be kept"
                        + " forever here: the longest allowed is "
                        + maximum.get().toDays() + " day(s)");
            }
            return keep;
        }
        if (keep.get().compareTo(minimum) < 0)
        {
            throw new IllegalArgumentException("The shortest retention"
                    + " allowed is " + minimum.toDays() + " day(s)");
        }
        if (maximum.isPresent() && keep.get().compareTo(maximum.get()) > 0)
        {
            throw new IllegalArgumentException("The longest retention"
                    + " allowed is " + maximum.get().toDays() + " day(s)");
        }
        return keep;
    }
}
