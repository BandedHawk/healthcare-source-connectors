/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.broker.receive.ArtemisReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerHealth;
import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.BrokerWatch;
import com.sdh.healthcare.connector.broker.receive.CamelReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.EventHubsSupport;
import com.sdh.healthcare.connector.broker.receive.RabbitmqConnections;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import java.time.Duration;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

/**
 * Receives a stage's input from any of the seven brokers (STAGES.md §7):
 * the Camel route that holds each batch until its handler is done, what
 * the broker needs bound in the Camel registry, and the watch that tells
 * the stage's health whether the broker answers. Artemis has a route of its
 * own, which reports its health itself.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageReceiver implements AutoCloseable
{

    /** The input route's ID. */
    public static final String ROUTE_ID = "stage-input";

    /** Logs starting and stopping, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(StageReceiver.class);

    /** The stage's Camel context. */
    private final CamelContext camel;

    /** The RabbitMQ connections; null for other brokers. */
    private final CachingConnectionFactory rabbitmq;

    /** Watches the broker; null for Artemis. */
    private final BrokerWatch watch;

    /**
     * Adds the input route to a Camel context, stopped.
     *
     * @param camel          the stage's Camel context
     * @param settings       the input's settings
     * @param handler        handles each batch
     * @param health         told whether the broker answers
     * @param labels         the fields Artemis's audit events carry
     * @param initialBackoff the first pause before a batch is tried again
     * @param maxBackoff     the longest pause
     * @throws Exception if the route can't be added, or TLS set up
     */
    public StageReceiver(CamelContext camel, ReceiveSettings settings,
            BatchHandler handler, BrokerHealth health, Fields labels,
            Duration initialBackoff, Duration maxBackoff) throws Exception
    {
        this.camel = camel;
        CachingConnectionFactory factory = null;
        if (settings.kind() == BrokerKind.RABBITMQ)
        {
            factory = RabbitmqConnections.factory(settings.rabbitmq());
            camel.getRegistry().bind(ReceiveSettings.RABBITMQ_BEAN, factory);
        }
        if (settings.kind() == BrokerKind.EVENTHUBS)
        {
            EventHubsSupport.bind(camel, settings.eventhubs());
        }
        this.rabbitmq = factory;
        RouteBuilder route = settings.kind() == BrokerKind.ARTEMIS
                ? new ArtemisReceiveRoute(ROUTE_ID, labels, settings, handler,
                        health, initialBackoff, maxBackoff)
                : new CamelReceiveRoute(ROUTE_ID, settings, handler,
                        initialBackoff, maxBackoff);
        camel.addRoutes(route);
        if (settings.kind() == BrokerKind.ARTEMIS)
        {
            this.watch = null;
        }
        else
        {
            this.watch = new BrokerWatch(ROUTE_ID, settings, health, camel);
            watch.bind(factory);
        }
    }

    /**
     * Starts taking messages.
     *
     * @throws Exception if the route can't start
     */
    public void start() throws Exception
    {
        camel.getRouteController().startRoute(ROUTE_ID);
        if (watch != null)
        {
            watch.start();
        }
        LOG.info("stage.input_started");
    }

    /**
     * Stops taking messages: the batch being handled is finished or handed
     * back (STAGES.md §10), then the connections are closed.
     */
    @Override
    public void close()
    {
        try
        {
            camel.getRouteController().stopRoute(ROUTE_ID);
        }
        catch (Exception failure)
        {
            LOG.warn("stage.input_stop_failed error={}",
                    failure.getClass().getSimpleName());
        }
        if (watch != null)
        {
            watch.close();
        }
        if (rabbitmq != null)
        {
            rabbitmq.destroy();
        }
        LOG.info("stage.input_stopped");
    }
}
