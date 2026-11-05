/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.intake.ListedItem;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lists a directory source with Camel's file component (design §5.3.4) and
 * hands each file, oldest first, to {@link IntakeService}.
 * <ul>
 *   <li>Camel never moves or deletes files ({@code noop}); the service
 *       does, once an item is landed and announced.</li>
 *   <li>{@code idempotent=false}: every listing offers every file, and the
 *       ledger, not Camel, knows which are landed. Readiness needs this
 *       too: it counts how long a file has looked the same.</li>
 *   <li>An empty listing still sends one empty message, and the last file
 *       of a listing is marked, so the service knows when a listing
 *       ends.</li>
 *   <li>Hidden files are never offered: the directory claim and the
 *       startup probe are hidden. Neither are renamed items, markers, or
 *       a move target inside the source.</li>
 *   <li>A missing directory isn't created, and isn't an error to Camel:
 *       the service reports it as unreachable.</li>
 *   <li>The route doesn't start by itself: it starts after recovery.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IntakeRoute extends RouteBuilder
{

    /** The route's ID. */
    public static final String ROUTE_ID = "intake";

    /** The registry name of the poll strategy that times listings. */
    static final String LISTING_TIMER = "sdhListingTimer";

    /** Logs listing failures, with no names or paths. */
    private static final Logger LOG =
            LoggerFactory.getLogger(IntakeRoute.class);

    /** The source's settings. */
    private final SourceSettings settings;

    /** Lands what the route lists. */
    private final IntakeService service;

    /** When the current listing began, in nanoseconds; while one is timed. */
    private long listingFrom;

    /** Whether a listing has begun and its first file not yet arrived. */
    private boolean timing;

    /**
     * Creates the route.
     *
     * @param settings the source's settings
     * @param service  lands what the route lists
     */
    public IntakeRoute(SourceSettings settings, IntakeService service)
    {
        this.settings = settings;
        this.service = service;
    }

    /**
     * The Camel endpoint that lists the directory.
     *
     * @return the endpoint URI
     */
    public String uri()
    {
        StringBuilder uri = new StringBuilder("file:")
                .append(settings.path())
                .append("?noop=true&idempotent=false&readLock=none")
                .append("&directoryMustExist=false&autoCreate=false")
                .append("&sendEmptyMessageWhenIdle=true")
                .append("&sortBy=file:modified")
                .append("&recursive=").append(settings.recursive())
                .append("&delay=").append(settings.pollInterval().toMillis())
                .append("&pollStrategy=#").append(LISTING_TIMER)
                .append("&exclude=^\\..*")
                .append("&antInclude=").append(settings.include());
        List<String> excluded = new ArrayList<>();
        excluded.add("**/*" + SourceSettings.REPLACES_SUFFIX);
        if (settings.marker())
        {
            uri.append("&doneFileName=${file:name}")
                    .append(SourceSettings.MARKER_SUFFIX);
            excluded.add("**/*" + SourceSettings.MARKER_SUFFIX);
        }
        if (settings.after() == SourceSettings.After.RENAME)
        {
            excluded.add("**/*" + settings.renameSuffix());
        }
        if (settings.after() == SourceSettings.After.MOVE
                && settings.moveTo().startsWith(settings.path()))
        {
            Path inside = settings.path().relativize(settings.moveTo());
            excluded.add(inside.toString().replace(File.separatorChar, '/')
                    + "/**");
        }
        if (!excluded.isEmpty())
        {
            uri.append("&antExclude=").append(String.join(",", excluded));
        }
        return uri.toString();
    }

    @Override
    public void configure()
    {
        getContext().getRegistry().bind(LISTING_TIMER, new ListingTimer());
        from(uri())
                .routeId(ROUTE_ID)
                .autoStartup(false)
                .process(this::offer);
    }

    /**
     * Passes a listed file, or the end of a listing, to the service.
     *
     * @param exchange a file, or an empty message from an empty listing
     */
    private void offer(Exchange exchange)
    {
        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
        if (timing)
        {
            // Camel lists the whole directory before it hands over the
            // first file, so this is how long the listing took.
            timing = false;
            service.listingTook(System.nanoTime() - listingFrom,
                    file == null ? 0 : exchange.getProperty(
                            Exchange.BATCH_SIZE, 1, Integer.class));
        }
        if (file != null)
        {
            service.onListed(new ListedItem(file.getRelativeFilePath()
                    .replace(File.separatorChar, '/'), file.getFileLength(),
                    file.getLastModified()));
        }
        if (file == null || exchange.getProperty(Exchange.BATCH_COMPLETE,
                false, Boolean.class))
        {
            service.onListingDone();
        }
    }

    /**
     * Times each listing from the start of Camel's poll to the first file
     * it hands over, or to the empty message of an empty listing. A poll
     * that fails is logged by the exception's class only: Camel's default
     * logs its message, which can name a path.
     */
    private final class ListingTimer implements PollingConsumerPollStrategy
    {

        /**
         * Notes when a poll begins.
         *
         * @param consumer the consumer polling
         * @param endpoint its endpoint
         * @return true, so the poll goes ahead
         */
        @Override
        public boolean begin(Consumer consumer, Endpoint endpoint)
        {
            listingFrom = System.nanoTime();
            timing = true;
            return true;
        }

        /**
         * Does nothing: the listing was timed when its first file came.
         *
         * @param consumer       the consumer that polled
         * @param endpoint       its endpoint
         * @param polledMessages how many files the poll handed over
         */
        @Override
        public void commit(Consumer consumer, Endpoint endpoint,
                int polledMessages)
        {
            timing = false;
        }

        /**
         * Logs a failed poll without its message, and lets the next poll
         * try again.
         *
         * @param consumer   the consumer that polled
         * @param endpoint   its endpoint
         * @param retryCounter how many times the poll has been retried
         * @param cause      what went wrong
         * @return false: no immediate retry
         */
        @Override
        public boolean rollback(Consumer consumer, Endpoint endpoint,
                int retryCounter, Exception cause)
        {
            timing = false;
            LOG.warn("intake.list_failed error={}",
                    cause.getClass().getSimpleName());
            return false;
        }
    }
}
