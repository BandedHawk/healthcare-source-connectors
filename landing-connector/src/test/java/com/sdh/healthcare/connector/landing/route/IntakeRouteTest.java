/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link IntakeRoute}, on Camel's file component and a real
 * directory.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IntakeRouteTest
{

    /** The source directory. */
    @TempDir
    Path directory;

    /**
     * Each listing is timed and its size passed on before its first file;
     * an empty listing has size 0.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void timesEachListing() throws Exception
    {
        IntakeService service = mock(IntakeService.class);
        Files.writeString(directory.resolve("a.csv"), "a");
        Files.writeString(directory.resolve("b.csv"), "b");
        Files.writeString(directory.resolve("c.csv"), "c");
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            camel.addRoutes(new IntakeRoute(settings(), service));
            camel.start();
            camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
            verify(service, timeout(10_000)).listingTook(anyLong(), eq(3));
            verify(service, timeout(10_000).times(3)).onListed(any());
            camel.getRouteController().stopRoute(IntakeRoute.ROUTE_ID);

            try (Stream<Path> files = Files.list(directory))
            {
                for (Path file : files.toList())
                {
                    Files.delete(file);
                }
            }
            camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
            verify(service, timeout(10_000)).listingTook(anyLong(), eq(0));
            verify(service, atLeastOnce()).onListingDone();
        }
    }

    /**
     * A read-only directory intake polling every 50 ms.
     *
     * @return the settings
     */
    SourceSettings settings()
    {
        return SourceSettings.of(new LandingProperties.Source(directory,
                "read-only", null, null, ".landed", "settled", Duration.ZERO,
                "*", false, Duration.ofMillis(50), "directory",
                Duration.ofMinutes(5), null, null, null, null));
    }
}
