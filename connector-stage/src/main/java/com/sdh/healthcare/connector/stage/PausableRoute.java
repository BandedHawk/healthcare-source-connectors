/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.apache.camel.spi.RouteController;

/**
 * A Camel input route that a pause stops taking work
 * (CONTROL-API-DESIGN.md §9). Pausing suspends it, which Camel does as it
 * stops one: the exchange being handled finishes first, and a consumer
 * that can't be suspended is stopped. Resuming starts it again. A route
 * paused before it is started isn't started until it is resumed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PausableRoute implements PauseWatch.Pausable
{

    /** The route's Camel context. */
    private final CamelContext camel;

    /** The route's ID. */
    private final String routeId;

    /** Whether it has been started, and not stopped, as the pod asks. */
    private boolean started;

    /** Whether it is paused. */
    private boolean paused;

    /**
     * Describes a route, added to its context and not started.
     *
     * @param camel   the route's Camel context
     * @param routeId the route's ID
     */
    public PausableRoute(CamelContext camel, String routeId)
    {
        this.camel = camel;
        this.routeId = routeId;
    }

    /**
     * Starts the route, unless it is paused: then it starts once resumed.
     *
     * @throws Exception if it can't be started
     */
    public synchronized void start() throws Exception
    {
        started = true;
        if (!paused)
        {
            camel.getRouteController().startRoute(routeId);
        }
    }

    /**
     * Stops the route for good, as the pod stops: neither a pause nor a
     * resume after touches it.
     *
     * @throws Exception if it can't be stopped
     */
    public synchronized void stop() throws Exception
    {
        started = false;
        camel.getRouteController().stopRoute(routeId);
    }

    /**
     * Restarts the route, for a consumer that gave up, unless it is paused
     * or stopped: a resume starts it again.
     *
     * @throws Exception if it can't be restarted
     */
    public synchronized void restart() throws Exception
    {
        if (!started || paused)
        {
            return;
        }
        RouteController routes = camel.getRouteController();
        routes.stopRoute(routeId);
        routes.startRoute(routeId);
    }

    /**
     * Whether the route is paused.
     *
     * @return true if it is
     */
    public synchronized boolean paused()
    {
        return paused;
    }

    /**
     * Suspends the route, or starts it again. Asking for what it is
     * already changes nothing.
     *
     * @param pause true to suspend it, false to start it again
     * @throws Exception if it can't be suspended or started
     */
    @Override
    public synchronized void paused(boolean pause) throws Exception
    {
        paused = pause;
        if (!started)
        {
            return;
        }
        RouteController routes = camel.getRouteController();
        ServiceStatus status = routes.getRouteStatus(routeId);
        if (pause)
        {
            if (status != null && status.isStarted())
            {
                routes.suspendRoute(routeId);
            }
        }
        else if (status != null && status.isSuspended())
        {
            routes.resumeRoute(routeId);
        }
        else if (status == null || !status.isStarted())
        {
            routes.startRoute(routeId);
        }
    }
}
