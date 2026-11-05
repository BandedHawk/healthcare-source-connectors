/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeHealth;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.component.azure.eventhubs.EventHubsEndpoint;
import org.apache.camel.component.azure.servicebus.ServiceBusEndpoint;
import org.apache.camel.component.google.pubsub.GooglePubsubEndpoint;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.springrabbit.SpringRabbitMQEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

/**
 * Tells a Camel queue intake's {@link IntakeHealth} when its broker is lost
 * and found again (design §5.3.16). The spike of 2026-11-04 found that most
 * Camel consumers say neither, so each broker is watched its own way:
 * <ul>
 *   <li><b>SQS:</b> by a poll strategy, told of every poll's outcome.</li>
 *   <li><b>RabbitMQ:</b> by a listener on the node's connection factory,
 *       and a passive check that each queue is there.</li>
 *   <li><b>Kafka, Pub/Sub, Service Bus and Event Hubs:</b> by a check
 *       through the broker's own client, every
 *       {@code sdh.landing.broker.check-interval}; the client is made from
 *       the consumer endpoint's own settings and credentials.</li>
 * </ul>
 * Pub/Sub's consumer gives up for good on an error it can't retry, such as
 * a deleted subscription; once the check finds the subscription again, the
 * route is restarted. Artemis reports its own outages.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class BrokerWatch implements AutoCloseable
{

    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(
            BrokerWatch.class);

    /**
     * The longest a check waits for its broker. Checks never overlap: the
     * next starts an interval after the last ends.
     */
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** The Camel component name of Pub/Sub. */
    static final String PUBSUB_COMPONENT = "google-pubsub";

    /** Restarts the intake's route. */
    interface Restarter
    {

        /**
         * Restarts the route.
         *
         * @throws Exception if it can't be restarted
         */
        void restart() throws Exception;
    }

    /** The broker's settings. */
    private final BrokerSettings settings;

    /** The intake's health. */
    private final IntakeHealth health;

    /** The node's Camel context. */
    private final CamelContext camel;

    /** Restarts the route. */
    private final Restarter restarter;

    /** Set when the consumer has given up and needs a restart. */
    private final AtomicBoolean gaveUp = new AtomicBoolean();

    /** The RabbitMQ connection factory, for RabbitMQ; else null. */
    private CachingConnectionFactory rabbitmq;

    /** Runs the checks; null until started, or with no check. */
    private ScheduledExecutorService scheduler;

    /** The check; null until started, or for SQS. */
    private BrokerCheck check;

    /**
     * Watches a broker.
     *
     * @param settings the broker's settings; not Artemis
     * @param health   the intake's health
     * @param camel    the node's Camel context
     */
    public BrokerWatch(BrokerSettings settings, IntakeHealth health,
            CamelContext camel)
    {
        this(settings, health, camel, () ->
        {
            camel.getRouteController().stopRoute(IntakeRoute.ROUTE_ID);
            camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
        });
    }

    /**
     * Watches a broker, restarting the route its own way.
     *
     * @param settings  the broker's settings; not Artemis
     * @param health    the intake's health
     * @param camel     the node's Camel context
     * @param restarter restarts the route
     */
    BrokerWatch(BrokerSettings settings, IntakeHealth health,
            CamelContext camel, Restarter restarter)
    {
        if (settings.kind() == BrokerSettings.Kind.ARTEMIS)
        {
            throw new IllegalArgumentException(
                    "Artemis reports its own outages");
        }
        this.settings = settings;
        this.health = health;
        this.camel = camel;
        this.restarter = restarter;
    }

    /**
     * Puts in place what must be there before the route is made: the SQS
     * poll strategy, the RabbitMQ connection listener, or the Pub/Sub
     * component that watches its subscribers.
     *
     * @param factory the RabbitMQ connection factory, for RabbitMQ; else
     *                null
     */
    public void bind(CachingConnectionFactory factory)
    {
        switch (settings.kind())
        {
            case SQS -> camel.getRegistry().bind(
                    BrokerSettings.POLL_STRATEGY_BEAN,
                    new SqsPollStrategy(health));
            case RABBITMQ ->
            {
                rabbitmq = factory;
                factory.addConnectionListener(
                        RabbitmqCheck.listener(health));
            }
            case PUBSUB ->
            {
                if (camel.hasComponent(PUBSUB_COMPONENT) != null)
                {
                    camel.removeComponent(PUBSUB_COMPONENT);
                }
                camel.addComponent(PUBSUB_COMPONENT,
                        new WatchedPubsubComponent(this::gaveUp));
            }
            default ->
            {
            }
        }
    }

    /**
     * Starts checking, once the route has started. SQS needs no check: its
     * poll strategy reports each poll.
     */
    public void start()
    {
        Endpoint endpoint = camel.getRoute(IntakeRoute.ROUTE_ID)
                .getEndpoint();
        BrokerCheck made;
        try
        {
            made = switch (settings.kind())
            {
                case KAFKA -> new KafkaCheck((KafkaEndpoint) endpoint,
                        TIMEOUT);
                case RABBITMQ -> new RabbitmqCheck(rabbitmq,
                        ((SpringRabbitMQEndpoint) endpoint).getQueues());
                case PUBSUB -> new PubsubCheck(
                        (GooglePubsubEndpoint) endpoint, TIMEOUT);
                case SERVICEBUS -> new ServicebusCheck(
                        (ServiceBusEndpoint) endpoint, TIMEOUT);
                case EVENTHUBS -> new EventhubsCheck(
                        (EventHubsEndpoint) endpoint, settings.eventhubs(),
                        TIMEOUT);
                default -> null;
            };
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        if (made != null)
        {
            start(made);
        }
    }

    /**
     * Runs a check now, then every check interval.
     *
     * @param made the check
     */
    synchronized void start(BrokerCheck made)
    {
        use(made);
        scheduler = Executors.newSingleThreadScheduledExecutor(task ->
        {
            Thread thread = new Thread(task, "sdh-broker-check");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::runCheck, 0,
                settings.checkInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the check, without scheduling it.
     *
     * @param made the check
     */
    synchronized void use(BrokerCheck made)
    {
        check = made;
    }

    /**
     * Checks the broker once, and tells the intake's health. A consumer
     * that gave up is restarted once its broker answers again.
     */
    void runCheck()
    {
        IntakeProblem problem;
        try
        {
            problem = check.check();
        }
        catch (InterruptedException stopping)
        {
            Thread.currentThread().interrupt();
            return;
        }
        catch (RuntimeException failure)
        {
            LOG.warn("intake.check_failed error={}",
                    failure.getClass().getSimpleName());
            problem = IntakeProblem.NETWORK;
        }
        if (problem != null)
        {
            health.unreachable(problem);
            return;
        }
        if (gaveUp.getAndSet(false))
        {
            try
            {
                restarter.restart();
                LOG.info("intake.consumer_restarted");
            }
            catch (Exception failure)
            {
                gaveUp.set(true);
                LOG.warn("intake.restart_failed error={}",
                        failure.getClass().getSimpleName());
                return;
            }
        }
        health.answered();
    }

    /**
     * The consumer has given up for good: reported now, and the route
     * restarted once the broker answers.
     *
     * @param reason why it gave up
     */
    void gaveUp(IntakeProblem reason)
    {
        gaveUp.set(true);
        LOG.warn("intake.consumer_gave_up reason={}", reason);
        health.unreachable(reason);
    }

    /** Stops checking, and closes the check's client. */
    @Override
    public synchronized void close()
    {
        if (scheduler != null)
        {
            scheduler.shutdownNow();
            try
            {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException stopping)
            {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        if (check != null)
        {
            check.close();
            check = null;
        }
    }
}
