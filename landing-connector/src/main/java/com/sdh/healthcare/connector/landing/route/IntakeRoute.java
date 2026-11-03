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
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;

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

    /** The source's settings. */
    private final SourceSettings settings;

    /** Lands what the route lists. */
    private final IntakeService service;

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
                .append("&exclude=^\\..*")
                .append("&antInclude=").append(settings.include());
        List<String> excluded = new ArrayList<>();
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
}
