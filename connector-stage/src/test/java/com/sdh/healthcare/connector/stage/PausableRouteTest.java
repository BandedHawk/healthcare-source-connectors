/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

/**
 * An input route a pause holds (CONTROL-API-DESIGN.md §9): not started
 * while paused, suspended by a pause, started again by a resume, and left
 * alone once stopped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PausableRouteTest
{

    /** The route's ID. */
    static final String ROUTE = "test-input";

    /** Creates the tests; JUnit runs each test on a new instance. */
    PausableRouteTest()
    {
    }

    /**
     * A route paused before it starts isn't started until resumed; a pause
     * then stops its exchanges, and a resume brings them back. A restart
     * while paused, or anything after it is stopped, leaves it as it is.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void holdsTheRouteWhilePaused() throws Exception
    {
        AtomicInteger taken = new AtomicInteger();
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            camel.getShutdownStrategy().setTimeout(5);
            camel.addRoutes(new RouteBuilder()
            {
                @Override
                public void configure()
                {
                    from("timer:test?period=10").routeId(ROUTE)
                            .autoStartup(false)
                            .process(exchange -> taken.incrementAndGet());
                }
            });
            camel.start();
            PausableRoute route = new PausableRoute(camel, ROUTE);

            route.paused(true);
            route.start();
            assertThat(status(camel)).isEqualTo(ServiceStatus.Stopped);
            route.restart();
            assertThat(status(camel)).isEqualTo(ServiceStatus.Stopped);

            route.paused(false);
            ControlStateTest.waitFor(() -> taken.get() > 2);

            route.paused(true);
            assertThat(status(camel).isStarted()).isFalse();
            route.paused(true);
            int held = taken.get();
            Thread.sleep(100);
            assertThat(taken.get()).isEqualTo(held);

            route.paused(false);
            ControlStateTest.waitFor(() -> taken.get() > held + 2);
            route.restart();
            assertThat(status(camel)).isEqualTo(ServiceStatus.Started);

            route.stop();
            route.paused(false);
            route.restart();
            assertThat(status(camel)).isEqualTo(ServiceStatus.Stopped);
        }
    }

    /**
     * The route's status.
     *
     * @param camel its context
     * @return the status
     */
    static ServiceStatus status(DefaultCamelContext camel)
    {
        return camel.getRouteController().getRouteStatus(ROUTE);
    }
}
