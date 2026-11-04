/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.sdh.healthcare.connector.landing.intake.BatchService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.spi.ShutdownPrepared;
import org.apache.camel.support.AsyncProcessorSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds messages from a broker until the batch holding them is landed
 * (design §5.3.16). For a broker whose consumer settles a message when its
 * exchange completes (SQS, Service Bus, Event Hubs, and RabbitMQ in manual
 * mode), each exchange is kept open, not completed, as it arrives. For one
 * that gives the exchange a way to settle the message later (Pub/Sub), the
 * exchange completes at once, and the message is settled when its batch is
 * landed.
 * <p>
 * A batch closes at {@code max-messages}, {@code max-bytes} or
 * {@code max-wait} after its first message, and a single thread lands it.
 * Only then are its messages settled, so the broker forgets them. A batch
 * that can't be landed is tried again in place, with a growing pause,
 * keeping its messages: handing them back would raise their delivery
 * counts, and a broker with a delivery limit would dead-letter them during
 * a long outage. Event Hubs needs this anyway, since its checkpoint would
 * move past a batch handed back as soon as a later one completed. While a
 * closed batch waits to be landed, a consumer offering more than another
 * batch's worth is held back.
 * <p>
 * Messages are handed back, so the broker delivers them again, only when
 * the route stops: as soon as Camel prepares it for shutdown, so that the
 * shutdown doesn't wait for exchanges that are only being held. After a
 * fatal error nothing more is taken: the consumer is held back, with what
 * it offers unsettled, until the node stops.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class HeldBatches extends AsyncProcessorSupport
        implements ShutdownPrepared
{

    /** Logs progress and errors, never content. */
    private static final Logger LOG =
            LoggerFactory.getLogger(HeldBatches.class);

    /** How a broker's messages are read and settled. */
    interface Broker
    {

        /**
         * A message from its exchange.
         *
         * @param exchange the exchange
         * @return the message
         */
        BatchService.Message message(Exchange exchange);

        /**
         * Settles a message with the broker before its exchange completes,
         * if the broker needs that (RabbitMQ in manual mode, Pub/Sub).
         *
         * @param exchange the exchange
         * @param landed   true to acknowledge it, false to have it delivered
         *                 again
         */
        default void settle(Exchange exchange, boolean landed)
        {
        }

        /**
         * Whether the exchange is held open until its batch is landed. If
         * not, it completes at once, and {@link #settle} alone settles the
         * message.
         *
         * @return true by default
         */
        default boolean holdsExchange()
        {
            return true;
        }
    }

    /** Lands a batch: {@link BatchService#land} in use. */
    @FunctionalInterface
    interface Lander
    {

        /**
         * Lands a batch.
         *
         * @param messages the messages
         * @return what happened
         * @throws InterruptedException if interrupted while waiting
         */
        BatchService.Result land(List<BatchService.Message> messages)
                throws InterruptedException;
    }

    /** Thrown into an exchange whose batch wasn't landed. */
    static final class NotLandedException extends Exception
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        /** Creates the exception, with no detail. */
        NotLandedException()
        {
            super("The batch wasn't landed; the broker delivers it again");
        }
    }

    /**
     * An exchange held open.
     *
     * @param exchange the exchange
     * @param callback completes it
     * @param message  its message
     */
    private record Held(Exchange exchange, AsyncCallback callback,
            BatchService.Message message)
    {
    }

    /** Lands batches. */
    private final Lander service;

    /** Reads and settles messages. */
    private final Broker broker;

    /** The most messages in a batch. */
    private final int maxMessages;

    /** The most bytes in a batch. */
    private final long maxBytes;

    /** The longest a batch stays open. */
    private final long maxWaitNanos;

    /** The first pause after a failure. */
    private final Duration initialBackoff;

    /** The longest pause after failures. */
    private final Duration maxBackoff;

    /** Gives the time, in nanoseconds. */
    private final LongSupplier nanos;

    /** Guards the fields below. */
    private final Object lock = new Object();

    /** The exchanges waiting, oldest first. */
    private final List<Held> waiting = new ArrayList<>();

    /** The batch being landed; empty when none is. */
    private final List<Held> landing = new ArrayList<>();

    /** Their bytes. */
    private long waitingBytes;

    /** When the oldest waiting exchange arrived. */
    private long firstAt;

    /** Whether the processor is running. */
    private boolean running;

    /** Whether a fatal error stopped it taking messages. */
    private boolean halted;

    /** The landing thread. */
    private Thread lander;

    /**
     * Creates the processor.
     *
     * @param service        lands batches
     * @param broker         reads and settles messages
     * @param maxMessages    the most messages in a batch
     * @param maxBytes       the most bytes in a batch
     * @param maxWait        the longest a batch stays open
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     * @param nanos          gives the time, in nanoseconds
     */
    HeldBatches(Lander service, Broker broker, int maxMessages,
            long maxBytes, Duration maxWait, Duration initialBackoff,
            Duration maxBackoff, LongSupplier nanos)
    {
        this.service = service;
        this.broker = broker;
        this.maxMessages = maxMessages;
        this.maxBytes = maxBytes;
        this.maxWaitNanos = maxWait.toNanos();
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.nanos = nanos;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback)
    {
        BatchService.Message message;
        try
        {
            message = broker.message(exchange);
        }
        catch (RuntimeException failure)
        {
            exchange.setException(failure);
            callback.done(true);
            return true;
        }
        synchronized (lock)
        {
            // Hold the consumer back while a whole further batch waits, or
            // for good after a fatal error.
            while (running && (halted || waiting.size() >= 2 * maxMessages))
            {
                try
                {
                    lock.wait();
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (!running)
            {
                refuse(exchange);
                callback.done(true);
                return true;
            }
            if (waiting.isEmpty())
            {
                firstAt = nanos.getAsLong();
            }
            waiting.add(new Held(exchange, callback, message));
            waitingBytes += message.body().length;
            lock.notifyAll();
        }
        if (!broker.holdsExchange())
        {
            callback.done(true);
            return true;
        }
        return false;
    }

    @Override
    protected void doStart()
    {
        synchronized (lock)
        {
            running = true;
            halted = false;
        }
        lander = Thread.ofPlatform().name("sdh-batches").start(this::run);
    }

    @Override
    public void prepareShutdown(boolean suspendOnly, boolean forced)
    {
        try
        {
            stopHolding();
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected void doStop() throws InterruptedException
    {
        stopHolding();
    }

    /**
     * Stops landing, and hands back every message still held. Waits for a
     * batch being landed to finish first, so it is settled either way.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    private void stopHolding() throws InterruptedException
    {
        Thread thread;
        synchronized (lock)
        {
            running = false;
            lock.notifyAll();
            thread = lander;
        }
        if (thread != null)
        {
            thread.join();
        }
        List<Held> left;
        synchronized (lock)
        {
            left = new ArrayList<>(landing);
            left.addAll(waiting);
            landing.clear();
            waiting.clear();
            waitingBytes = 0;
        }
        release(left);
    }

    /** The landing thread's loop: lands batches until stopped. */
    private void run()
    {
        int failures = 0;
        while (true)
        {
            List<BatchService.Message> messages;
            try
            {
                messages = nextBatch();
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            if (messages == null)
            {
                return;
            }
            BatchService.Result result;
            try
            {
                result = service.land(messages);
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            switch (result.status())
            {
                case LANDED, SKIPPED ->
                {
                    failures = 0;
                    acknowledge();
                }
                case FATAL ->
                {
                    synchronized (lock)
                    {
                        halted = true;
                        lock.notifyAll();
                    }
                    return;
                }
                default ->
                {
                    // Keep the batch, and try it again after a pause.
                    failures++;
                    if (!pause(failures))
                    {
                        return;
                    }
                }
            }
        }
    }

    /**
     * The batch to land: the one being landed if it failed, or else the
     * next once it closes.
     *
     * @return its messages; null once stopped
     * @throws InterruptedException if interrupted while waiting
     */
    private List<BatchService.Message> nextBatch() throws InterruptedException
    {
        synchronized (lock)
        {
            while (true)
            {
                if (!running)
                {
                    return null;
                }
                if (!landing.isEmpty())
                {
                    return landing.stream().map(Held::message).toList();
                }
                if (!waiting.isEmpty())
                {
                    long open = nanos.getAsLong() - firstAt;
                    if (waiting.size() >= maxMessages
                            || waitingBytes >= maxBytes
                            || open >= maxWaitNanos)
                    {
                        take();
                        return landing.stream().map(Held::message).toList();
                    }
                    long left = Math.max(1,
                            (maxWaitNanos - open) / 1_000_000);
                    lock.wait(left);
                }
                else
                {
                    lock.wait();
                }
            }
        }
    }

    /**
     * Moves the oldest exchanges, up to a batch's limits, to the batch being
     * landed. A single message over {@code max-bytes} is a batch of its own.
     * The caller holds the lock.
     */
    private void take()
    {
        long bytes = 0;
        while (!waiting.isEmpty() && landing.size() < maxMessages)
        {
            long next = waiting.getFirst().message().body().length;
            if (!landing.isEmpty() && bytes + next > maxBytes)
            {
                break;
            }
            landing.add(waiting.removeFirst());
            bytes += next;
            waitingBytes -= next;
        }
        firstAt = nanos.getAsLong();
        lock.notifyAll();
    }

    /** Settles the batch just landed, so the broker forgets it. */
    private void acknowledge()
    {
        List<Held> batch;
        synchronized (lock)
        {
            batch = new ArrayList<>(landing);
            landing.clear();
            lock.notifyAll();
        }
        for (Held held : batch)
        {
            try
            {
                broker.settle(held.exchange(), true);
            }
            catch (RuntimeException failure)
            {
                // The broker delivers it again; the ID recognises it.
                LOG.warn("batch.settle_failed error={}",
                        failure.getClass().getSimpleName());
            }
            if (broker.holdsExchange())
            {
                held.callback().done(false);
            }
        }
    }

    /**
     * Hands messages back, so the broker delivers them again.
     *
     * @param batch the exchanges
     */
    private void release(List<Held> batch)
    {
        for (Held held : batch)
        {
            refuse(held.exchange());
            if (broker.holdsExchange())
            {
                held.callback().done(false);
            }
        }
    }

    /**
     * Hands one message back, without completing its exchange.
     *
     * @param exchange the exchange
     */
    private void refuse(Exchange exchange)
    {
        try
        {
            broker.settle(exchange, false);
        }
        catch (RuntimeException failure)
        {
            // Unsettled, it comes again once its lock or lease ends.
            LOG.warn("batch.settle_failed error={}",
                    failure.getClass().getSimpleName());
        }
        if (broker.holdsExchange())
        {
            exchange.setException(new NotLandedException());
        }
    }

    /**
     * Pauses after failures, doubling up to the longest pause.
     *
     * @param failures failures in a row
     * @return false if stopped while pausing
     */
    private boolean pause(int failures)
    {
        long pause = Math.min(maxBackoff.toMillis(),
                initialBackoff.toMillis() << Math.min(failures - 1, 20));
        synchronized (lock)
        {
            long until = System.currentTimeMillis() + pause;
            while (running)
            {
                long left = until - System.currentTimeMillis();
                if (left <= 0)
                {
                    return true;
                }
                try
                {
                    lock.wait(left);
                }
                catch (InterruptedException interrupted)
                {
                    return false;
                }
            }
            return false;
        }
    }
}
