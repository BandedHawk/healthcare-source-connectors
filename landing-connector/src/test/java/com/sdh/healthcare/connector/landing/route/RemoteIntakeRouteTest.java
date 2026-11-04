/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.intake.IntakeUnavailableException;
import com.sdh.healthcare.connector.landing.intake.ListedItem;
import com.sdh.healthcare.connector.landing.intake.RemoteSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for {@link RemoteIntakeRoute}'s polling, with a fake source and a
 * mocked service.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RemoteIntakeRouteTest
{

    /** Two items, as a listing would give them. */
    static final List<ListedItem> ITEMS = List.of(
            new ListedItem("a.csv", 1, 1_000),
            new ListedItem("b.csv", 1, 2_000));

    /** What each listing returns, or throws, in turn. */
    final Deque<Object> listings = new ArrayDeque<>();

    /** How many times the source was listed. */
    int listed;

    /** What the source says of its last call. */
    boolean reachable = true;

    /** The fake time, in nanoseconds. */
    final AtomicLong now = new AtomicLong();

    /** The mocked service. */
    IntakeService service;

    /** The route under test. */
    RemoteIntakeRoute route;

    /** Builds the route over the fake source. */
    @BeforeEach
    void setUp()
    {
        service = mock(IntakeService.class);
        RemoteSource source = new RemoteSource()
        {
            @Override
            @SuppressWarnings("unchecked")
            public List<ListedItem> list() throws IOException
            {
                listed++;
                Object next = listings.removeFirst();
                if (next instanceof IOException failure)
                {
                    throw failure;
                }
                return (List<ListedItem>) next;
            }

            @Override
            public InputStream open(ListedItem item, long offset)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void afterLanding(ListedItem item)
            {
            }

            @Override
            public boolean reachable()
            {
                return reachable;
            }

            @Override
            public void checkWritable()
            {
            }

            @Override
            public void close()
            {
            }
        };
        SourceSettings settings = SourceSettings.of(
                new LandingProperties.Source(Path.of("/in"), "read-only",
                        null, null, ".landed", "settled", Duration.ZERO, "*",
                        false, Duration.ofSeconds(10), "directory",
                        Duration.ofMinutes(5), null, null));
        route = new RemoteIntakeRoute(settings, source, service, now::get);
    }

    /** A whole listing offers each item, then reports the end. */
    @Test
    void offersEachItemThenTheEnd()
    {
        listings.add(ITEMS);
        route.poll();
        InOrder order = inOrder(service);
        order.verify(service).onListed(ITEMS.get(0));
        order.verify(service).onListed(ITEMS.get(1));
        order.verify(service).onListingDone();
    }

    /**
     * A connection that breaks while an item is landed ends the listing
     * without reporting its end, so an unfinished item isn't given up.
     */
    @Test
    void stopsAListingWhoseConnectionBroke()
    {
        listings.add(ITEMS);
        doAnswer(call ->
        {
            reachable = false;
            return null;
        }).when(service).onListed(ITEMS.get(0));
        route.poll();
        verify(service).onListed(ITEMS.get(0));
        verify(service, never()).onListed(ITEMS.get(1));
        verify(service, never()).onListingDone();
    }

    /**
     * After refused credentials, the next listing waits for the auth retry
     * interval, however often the timer fires.
     */
    @Test
    void waitsAfterRefusedCredentials()
    {
        listings.add(new IntakeUnavailableException(IntakeProblem.AUTH, null));
        listings.add(ITEMS);
        route.poll();
        verify(service).onListingFailed(IntakeProblem.AUTH);
        now.addAndGet(Duration.ofMinutes(4).toNanos());
        route.poll();
        assertThat(listed).isEqualTo(1);
        now.addAndGet(Duration.ofMinutes(1).toNanos());
        route.poll();
        assertThat(listed).isEqualTo(2);
        verify(service).onListingDone();
    }

    /** After a network failure, the next poll tries again. */
    @Test
    void retriesANetworkFailureAtTheNextPoll()
    {
        listings.add(new IntakeUnavailableException(IntakeProblem.NETWORK,
                null));
        listings.add(ITEMS);
        route.poll();
        verify(service).onListingFailed(IntakeProblem.NETWORK);
        verify(service, never()).onListed(any());
        route.poll();
        assertThat(listed).isEqualTo(2);
        verify(service).onListingDone();
    }
}
