/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.ShutdownPrepared;
import org.apache.camel.support.service.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes an Artemis queue with the JMS client and hands its messages to a
 * {@link BatchHandler} in batches (design §5.3.16). Camel's JMS components
 * can't be used: they acknowledge a message as soon as their listener
 * returns, before its batch could be handled.
 * <p>
 * A Camel timer runs one batch at a time. A session in
 * {@code CLIENT_ACKNOWLEDGE} mode receives up to {@code max-messages} and
 * {@code max-bytes}, for at most {@code max-wait} after the first; once the
 * batch is handled, acknowledging its last message acknowledges them all. A
 * batch that can't be handled yet is tried again in place, with a growing
 * pause:
 * handing it back would raise its messages' delivery counts, and Artemis
 * moves a message to its dead-letter address after ten deliveries by
 * default. A connection that fails is dropped, which returns any
 * unacknowledged messages, and opened again after a growing pause; the
 * {@link BrokerHealth} is told of the outage, and of its end once a receive
 * works again. After a fatal error nothing more is received.
 * <p>
 * Text messages are handed over as UTF-8, bytes messages as they are. Other
 * kinds have no byte form: they are handed over empty and reported, and
 * object messages are never deserialised. The message properties named in
 * {@link ReceiveSettings#attributes()} are read with each message.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ArtemisReceiveRoute extends RouteBuilder
{

    /** Logs progress and errors, never content. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ArtemisReceiveRoute.class);

    /** How long one receive waits for a first message. */
    static final Duration IDLE_WAIT = Duration.ofSeconds(1);

    /** The route's ID, by which it is started and stopped. */
    private final String routeId;

    /** Labels the audit event for a message with no byte form. */
    private final Fields labels;

    /** The broker's settings. */
    private final ReceiveSettings settings;

    /** Handles batches. */
    private final BatchHandler handler;

    /** Told whether the broker is answering. */
    private final BrokerHealth health;

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

    /** Whether the route is stopping, so a retry should give up. */
    private volatile boolean stopping;

    /** Whether a fatal error stopped receiving. */
    private volatile boolean halted;

    /**
     * Creates the route. It doesn't start with the Camel context: the caller
     * starts it by its ID when ready.
     *
     * @param routeId        the route's ID
     * @param labels         labels the audit event for a message with no
     *                       byte form, such as the intake's name
     * @param settings       the broker's settings
     * @param handler        handles batches
     * @param health         told whether the broker is answering
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    public ArtemisReceiveRoute(String routeId, Fields labels,
            ReceiveSettings settings, BatchHandler handler,
            BrokerHealth health, Duration initialBackoff, Duration maxBackoff)
    {
        this(routeId, labels, settings, handler, health,
                () -> new ActiveMQConnectionFactory(
                        settings.artemis().brokerUrl(),
                        settings.artemis().username(),
                        settings.artemis().password()),
                initialBackoff, maxBackoff);
    }

    /**
     * Creates the route with a connection factory of the caller's.
     *
     * @param routeId        the route's ID
     * @param labels         labels the audit event for a message with no
     *                       byte form
     * @param settings       the broker's settings
     * @param handler        handles batches
     * @param health         told whether the broker is answering
     * @param factory        makes connections
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    public ArtemisReceiveRoute(String routeId, Fields labels,
            ReceiveSettings settings, BatchHandler handler,
            BrokerHealth health, Supplier<ConnectionFactory> factory,
            Duration initialBackoff, Duration maxBackoff)
    {
        this.routeId = routeId;
        this.labels = labels;
        this.settings = settings;
        this.handler = handler;
        this.health = health;
        this.factory = factory;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    @Override
    public void configure() throws Exception
    {
        errorHandler(noErrorHandler());
        from("timer:sdh-artemis-" + routeId + "?delay=0&period=1")
                .routeId(routeId)
                .autoStartup(false)
                .process(new Poller());
    }

    /**
     * The route's processor: polls once per tick. It tells a batch being
     * retried to give up as soon as Camel prepares the route to stop, so
     * the route stops at once rather than when its shutdown times out, and
     * drops the connection when the route stops, which returns whatever
     * wasn't acknowledged.
     */
    private final class Poller extends ServiceSupport
            implements Processor, ShutdownPrepared
    {

        /** Creates the processor. */
        Poller()
        {
        }

        @Override
        public void process(Exchange exchange) throws Exception
        {
            pollOnce();
        }

        @Override
        public void prepareShutdown(boolean suspendOnly, boolean forced)
        {
            stopping = true;
        }

        @Override
        protected void doStart()
        {
            stopping = false;
        }

        @Override
        protected void doStop()
        {
            stopping = true;
            disconnect();
        }
    }

    /**
     * Receives and handles one batch, if any message comes.
     *
     * @throws Exception if the node is stopping on a fatal error
     */
    synchronized void pollOnce() throws Exception
    {
        if (halted || stopping)
        {
            return;
        }
        try
        {
            connect();
            List<Message> received = receive();
            health.answered();
            if (received.isEmpty())
            {
                return;
            }
            List<BrokerMessage> batch = new ArrayList<>();
            for (Message message : received)
            {
                batch.add(message(message));
            }
            while (true)
            {
                switch (handler.handle(batch))
                {
                    case DONE ->
                    {
                        received.getLast().acknowledge();
                        failures = 0;
                        return;
                    }
                    case FATAL ->
                    {
                        halted = true;
                        disconnect();
                        throw new HeldBatches.NotHandledException();
                    }
                    case RETRY ->
                    {
                        // Keep the batch, and try it again after a pause.
                        pause();
                        if (stopping)
                        {
                            return;
                        }
                    }
                }
            }
        }
        catch (JMSException failure)
        {
            health.unreachable(failure instanceof JMSSecurityException
                    ? BrokerProblem.AUTH : BrokerProblem.NETWORK);
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
                "broker.connection_failed error={}",
                failure.getClass().getSimpleName()));
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(
                session.createQueue(settings.artemis().queue()));
        connection.start();
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
     * A JMS message as a broker message.
     *
     * @param message the message
     * @return its body, ID and the properties asked for
     * @throws JMSException if the broker fails
     */
    private BrokerMessage message(Message message) throws JMSException
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
            Telemetry.get().auditWarn("message.unsupported_type", labels);
        }
        Map<String, String> attributes = new HashMap<>();
        for (String name : settings.attributes())
        {
            String value = CamelReceiveRoute.text(
                    message.getObjectProperty(name));
            if (value != null)
            {
                attributes.put(name, value);
            }
        }
        return new BrokerMessage(body, message.getJMSMessageID(), attributes);
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
     * Pauses after a failure, doubling up to the longest pause, and ending
     * early if the route stops.
     *
     * @throws InterruptedException if interrupted while pausing
     */
    private void pause() throws InterruptedException
    {
        failures++;
        long until = System.nanoTime() + 1_000_000L * Math.min(
                maxBackoff.toMillis(), initialBackoff.toMillis()
                        << Math.min(failures - 1, 20));
        while (!stopping && System.nanoTime() < until)
        {
            Thread.sleep(Math.min(50,
                    Math.max(1, (until - System.nanoTime()) / 1_000_000)));
        }
    }
}
