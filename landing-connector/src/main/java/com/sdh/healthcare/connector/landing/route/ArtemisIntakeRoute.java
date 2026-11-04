/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.intake.BatchService;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.service.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes an Artemis queue with the JMS client and lands its messages in
 * batches (design §5.3.16). Camel's JMS components can't be used: they
 * acknowledge a message as soon as their listener returns, before its batch
 * could be landed.
 * <p>
 * A Camel timer runs one batch at a time. A session in
 * {@code CLIENT_ACKNOWLEDGE} mode receives up to {@code max-messages} and
 * {@code max-bytes}, for at most {@code max-wait} after the first; once the
 * batch is landed, acknowledging its last message acknowledges them all. If
 * it isn't landed, the session is recovered, so the broker delivers the
 * messages again. A connection that fails is dropped, which also returns
 * any unacknowledged messages, and opened again after a growing pause; the
 * outage is reported once as {@code intake.unreachable}.
 * <p>
 * Text messages are landed as UTF-8, bytes messages as they are. Other
 * kinds have no byte form: they are landed empty and reported, and object
 * messages are never deserialised.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ArtemisIntakeRoute extends RouteBuilder
{

    /** Logs progress and errors, never content. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ArtemisIntakeRoute.class);

    /** How long one receive waits for a first message. */
    static final Duration IDLE_WAIT = Duration.ofSeconds(1);

    /** The intake's name. */
    private final String intake;

    /** The broker's settings. */
    private final BrokerSettings settings;

    /** Lands batches. */
    private final BatchService service;

    /** Makes connections. */
    private final Supplier<ConnectionFactory> factory;

    /** The first pause after a failure. */
    private final Duration initialBackoff;

    /** The longest pause after failures. */
    private final Duration maxBackoff;

    /** The connection; null until opened, or after it failed. */
    private Connection connection;

    /** The session on it. */
    private Session session;

    /** The consumer on the queue. */
    private MessageConsumer consumer;

    /** Failures in a row, for the pause. */
    private int failures;

    /** Whether the outage has been reported. */
    private boolean unreachable;

    /**
     * Creates the route.
     *
     * @param intake         the intake's name
     * @param settings       the broker's settings
     * @param service        lands batches
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    public ArtemisIntakeRoute(String intake, BrokerSettings settings,
            BatchService service, Duration initialBackoff,
            Duration maxBackoff)
    {
        this(intake, settings, service, () -> new ActiveMQConnectionFactory(
                settings.artemis().brokerUrl(), settings.artemis().username(),
                settings.artemis().password()), initialBackoff, maxBackoff);
    }

    /**
     * Creates the route with a connection factory of the caller's.
     *
     * @param intake         the intake's name
     * @param settings       the broker's settings
     * @param service        lands batches
     * @param factory        makes connections
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    ArtemisIntakeRoute(String intake, BrokerSettings settings,
            BatchService service, Supplier<ConnectionFactory> factory,
            Duration initialBackoff, Duration maxBackoff)
    {
        this.intake = intake;
        this.settings = settings;
        this.service = service;
        this.factory = factory;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    @Override
    public void configure() throws Exception
    {
        errorHandler(noErrorHandler());
        from("timer:sdh-artemis-intake?delay=0&period=1")
                .routeId(IntakeRoute.ROUTE_ID)
                .autoStartup(false)
                .process(exchange -> pollOnce());
        getContext().addService(new ServiceSupport()
        {
            @Override
            protected void doStop()
            {
                disconnect();
            }
        });
    }

    /**
     * Receives and lands one batch, if any message comes.
     *
     * @throws Exception if the node is stopping on a fatal error
     */
    synchronized void pollOnce() throws Exception
    {
        try
        {
            connect();
            List<Message> received = receive();
            if (received.isEmpty())
            {
                return;
            }
            List<BatchService.Message> batch = new ArrayList<>();
            for (Message message : received)
            {
                batch.add(message(message));
            }
            BatchService.Result result = service.land(batch);
            switch (result.status())
            {
                case LANDED, SKIPPED ->
                {
                    received.getLast().acknowledge();
                    failures = 0;
                }
                case FATAL ->
                {
                    disconnect();
                    throw new HeldBatches.NotLandedException();
                }
                default ->
                {
                    session.recover();
                    pause();
                }
            }
        }
        catch (JMSException failure)
        {
            markUnreachable(failure instanceof JMSSecurityException
                    ? IntakeProblem.AUTH : IntakeProblem.NETWORK);
            disconnect();
            pause();
        }
    }

    /**
     * Opens the connection, session and consumer, if they aren't open.
     *
     * @throws JMSException if the broker can't be reached
     */
    private void connect() throws JMSException
    {
        if (consumer != null)
        {
            return;
        }
        connection = factory.get().createConnection();
        connection.setExceptionListener(failure -> LOG.warn(
                "intake.connection_failed error={}",
                failure.getClass().getSimpleName()));
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(
                session.createQueue(settings.artemis().queue()));
        connection.start();
        if (unreachable)
        {
            unreachable = false;
            Telemetry.get().audit("intake.reachable",
                    Fields.of(Attr.INTAKE, intake));
        }
    }

    /**
     * Receives up to a batch: waits briefly for a first message, then takes
     * more until a limit is reached or the batch's time is up.
     *
     * @return the messages, oldest first; empty if none came
     * @throws JMSException if the broker fails
     */
    private List<Message> receive() throws JMSException
    {
        List<Message> received = new ArrayList<>();
        Message first = consumer.receive(IDLE_WAIT.toMillis());
        if (first == null)
        {
            return received;
        }
        received.add(first);
        long bytes = size(first);
        long until = System.nanoTime() + settings.maxWait().toNanos();
        while (received.size() < settings.maxMessages()
                && bytes < settings.maxBytes())
        {
            long left = (until - System.nanoTime()) / 1_000_000;
            if (left <= 0)
            {
                break;
            }
            Message next = consumer.receive(left);
            if (next == null)
            {
                break;
            }
            received.add(next);
            bytes += size(next);
        }
        return received;
    }

    /**
     * A message's size, as far as it is known before reading it.
     *
     * @param message the message
     * @return its body's length in bytes; 0 if not known
     * @throws JMSException if the broker fails
     */
    private static long size(Message message) throws JMSException
    {
        if (message instanceof BytesMessage bytes)
        {
            return bytes.getBodyLength();
        }
        if (message instanceof TextMessage text && text.getText() != null)
        {
            return text.getText().length();
        }
        return 0;
    }

    /**
     * A JMS message as a batch message.
     *
     * @param message the message
     * @return its body and ID
     * @throws JMSException if the broker fails
     */
    private BatchService.Message message(Message message) throws JMSException
    {
        byte[] body;
        if (message instanceof BytesMessage bytes)
        {
            body = new byte[(int) bytes.getBodyLength()];
            bytes.readBytes(body);
        }
        else if (message instanceof TextMessage text)
        {
            body = text.getText() == null ? new byte[0]
                    : text.getText().getBytes(StandardCharsets.UTF_8);
        }
        else
        {
            body = new byte[0];
            Telemetry.get().auditWarn("message.unsupported_type",
                    Fields.of(Attr.INTAKE, intake));
        }
        return new BatchService.Message(body, message.getJMSMessageID());
    }

    /**
     * Reports the broker unreachable, once per outage.
     *
     * @param problem why
     */
    private void markUnreachable(IntakeProblem problem)
    {
        if (unreachable)
        {
            return;
        }
        unreachable = true;
        Telemetry.get().auditError("intake.unreachable",
                Fields.of(Attr.INTAKE, intake)
                        .with(Attr.ERROR_CODE, ErrorCode.INTAKE_UNREACHABLE)
                        .with(Attr.REASON, problem.name()));
    }

    /** Closes the connection; unacknowledged messages go back. */
    private synchronized void disconnect()
    {
        Connection open = connection;
        connection = null;
        session = null;
        consumer = null;
        if (open != null)
        {
            try
            {
                open.close();
            }
            catch (JMSException ignored)
            {
                // It is going anyway.
            }
        }
    }

    /**
     * Pauses after a failure, doubling up to the longest pause.
     *
     * @throws InterruptedException if interrupted while pausing
     */
    private void pause() throws InterruptedException
    {
        failures++;
        Thread.sleep(Math.min(maxBackoff.toMillis(), initialBackoff.toMillis()
                << Math.min(failures - 1, 20)));
    }
}
