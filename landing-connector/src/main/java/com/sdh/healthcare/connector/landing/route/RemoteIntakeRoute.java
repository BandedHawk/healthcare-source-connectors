/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.intake.IntakeUnavailableException;
import com.sdh.healthcare.connector.landing.intake.ListedItem;
import com.sdh.healthcare.connector.landing.intake.RemoteSource;
import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists a remote source on a timer and passes what it finds to the intake
 * service (design §5.3.4), as {@link IntakeRoute} does for a directory.
 * When the source can't be listed, the failure is reported and the listing
 * skipped; after refused credentials, permissions, host key or share, the
 * next try waits for {@code auth-retry-interval}, so retries can't lock the
 * account. If the connection breaks part-way through a listing, the rest
 * of it is skipped: the end of a listing is only reported for one that
 * was complete, so an unfinished item is never given up because of an
 * outage.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RemoteIntakeRoute extends RouteBuilder
{

    /** Logs listing failures, with no names or paths. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteIntakeRoute.class);

    /** The source's settings. */
    private final SourceSettings settings;

    /** Where items come from. */
    private final RemoteSource source;

    /** Lands what the route lists. */
    private final IntakeService service;

    /** The time, in nanoseconds. */
    private final LongSupplier nanoTime;

    /** No listing is tried before this, in {@link #nanoTime} terms. */
    private long quietUntil;

    /** Whether a listing has been tried since a quiet spell began. */
    private boolean quiet;

    /** Set once the node starts to stop: no more items are offered. */
    private volatile boolean stopping;

    /**
     * Creates the route.
     *
     * @param settings the source's settings
     * @param source   where items come from
     * @param service  lands what the route lists
     * @param nanoTime the time, in nanoseconds
     */
    public RemoteIntakeRoute(SourceSettings settings, RemoteSource source,
            IntakeService service, LongSupplier nanoTime)
    {
        this.settings = settings;
        this.source = source;
        this.service = service;
        this.nanoTime = nanoTime;
    }

    @Override
    public void configure()
    {
        from("timer:sdh-intake?delay=0&period="
                + settings.pollInterval().toMillis())
                .routeId(IntakeRoute.ROUTE_ID)
                .autoStartup(false)
                .process(exchange -> poll());
    }

    /** Lists the source once, unless it is in a quiet spell. */
    void poll()
    {
        if (quiet && nanoTime.getAsLong() - quietUntil < 0)
        {
            return;
        }
        quiet = false;
        List<ListedItem> items;
        long listingFrom = nanoTime.getAsLong();
        try
        {
            items = source.list();
        }
        catch (IntakeUnavailableException unavailable)
        {
            service.onListingFailed(unavailable.problem());
            if (unavailable.problem().slowRetry())
            {
                quiet = true;
                quietUntil = nanoTime.getAsLong()
                        + settings.authRetryInterval().toNanos();
            }
            return;
        }
        catch (IOException | RuntimeException failure)
        {
            LOG.warn("intake.list_failed error={}",
                    failure.getClass().getSimpleName());
            return;
        }
        service.listingTook(nanoTime.getAsLong() - listingFrom, items.size());
        for (ListedItem item : items)
        {
            if (stopping)
            {
                return;
            }
            service.onListed(item);
            if (!source.reachable())
            {
                return;
            }
        }
        service.onListingDone();
    }

    /**
     * Stops offering items, from the next one: the item being landed is
     * finished, and the rest of the listing is left for the next node, as
     * for a broken connection. A listing is a single exchange, so without
     * this a long one keeps the route from stopping until the pod is
     * killed, and its lease is left to run out instead of being released.
     */
    public void stopListing()
    {
        stopping = true;
    }
}
