/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import java.time.Duration;
import java.util.Set;

/**
 * The checked settings of a queue intake (design §5.3.16): how its broker
 * is received from, which {@code connector-broker} checks, and what the
 * landing node adds, the window in which a message delivered again is
 * recognised.
 *
 * @param receive     how the broker is received from
 * @param dedupWindow how far back, from the last landed batch, a message
 *                    delivered again is recognised by its ID
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record BrokerSettings(ReceiveSettings receive, Duration dedupWindow)
{

    /** Where the settings are configured, to name them in errors. */
    public static final String PREFIX = "sdh.landing.broker";

    /**
     * The message attribute or header in which a producer names the earlier
     * message a message replaces (design §5.3.8).
     */
    public static final String REPLACES_ATTRIBUTE = "sdh_replaces";

    /**
     * Checks the settings.
     *
     * @param broker the settings as configured
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing or would let
     *                                  a message be acknowledged before it
     *                                  is landed
     */
    public static BrokerSettings of(LandingProperties.Broker broker)
    {
        if (broker.dedupWindow().isNegative())
        {
            throw new IllegalArgumentException(
                    PREFIX + ".dedup-window must not be negative");
        }
        return new BrokerSettings(ReceiveSettings.of(broker.receive(), PREFIX,
                Set.of(REPLACES_ATTRIBUTE)), broker.dedupWindow());
    }
}
