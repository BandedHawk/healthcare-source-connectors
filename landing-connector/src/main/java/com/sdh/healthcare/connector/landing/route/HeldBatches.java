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
import org.apache.camel.support.AsyncProcessorSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds messages from a broker whose consumer acknowledges a message when
 * its exchange completes (SQS, and RabbitMQ in manual mode), until the batch
 * holding them is landed (design §5.3.16).
 * <p>
 * Each exchange is kept open, not completed, as it arrives. A batch closes
 * at {@code max-messages}, {@code max-bytes} or {@code max-wait} after its
 * first message, and a single thread lands it. Only then are its exchanges
 * completed: normally once landed, so the broker forgets the messages; with
 * an error if not, so the broker delivers them again. After a failure the
 * next batch waits, with a growing pause. While a closed batch waits to be
 * landed, a consumer offering more than another batch's worth is held back.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class HeldBatches extends AsyncProcessorSupport
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
         * if the broker needs that (RabbitMQ in manual mode).
         *
         * @param exchange the exchange
         * @param landed   true to acknowledge it, false to have it delivered
         *                 again
         */
        default void settle(Exchange exchange, boolean landed)
        {
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

    /** Their bytes. */
    private long waitingBytes;

    /** When the oldest waiting exchange arrived. */
    private long firstAt;

    /** Whether the processor is running. */
    private boolean running;

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
            // Hold the consumer back while a whole further batch waits.
            while (running && waiting.size() >= 2 * maxMessages)
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
                broker.settle(exchange, false);
                exchange.setException(new NotLandedException());
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
        return false;
    }

    @Override
    protected void doStart()
    {
        synchronized (lock)
        {
            running = true;
        }
        lander = Thread.ofPlatform().name("sdh-batches").start(this::run);
    }

    @Override
    protected void doStop() throws InterruptedException
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
            left = new ArrayList<>(waiting);
            waiting.clear();
            waitingBytes = 0;
        }
        complete(left, false);
    }

    /** The landing thread's loop: lands batches until stopped. */
    private void run()
    {
        int failures = 0;
        while (true)
        {
            List<Held> batch;
            try
            {
                batch = nextBatch();
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            if (batch == null)
            {
                return;
            }
            BatchService.Result result;
            try
            {
                result = service.land(batch.stream().map(Held::message)
                        .toList());
            }
            catch (InterruptedException interrupted)
            {
                complete(batch, false);
                return;
            }
            boolean landed = result.status() == BatchService.Status.LANDED
                    || result.status() == BatchService.Status.SKIPPED;
            complete(batch, landed);
            if (result.status() == BatchService.Status.FATAL)
            {
                stopTaking();
                return;
            }
            failures = landed ? 0 : failures + 1;
            if (failures > 0 && !pause(failures))
            {
                return;
            }
        }
    }

    /**
     * Waits for a batch to close, and takes it.
     *
     * @return the batch; null once stopped
     * @throws InterruptedException if interrupted while waiting
     */
    private List<Held> nextBatch() throws InterruptedException
    {
        synchronized (lock)
        {
            while (true)
            {
                if (!running)
                {
                    return null;
                }
                if (!waiting.isEmpty())
                {
                    long open = nanos.getAsLong() - firstAt;
                    if (waiting.size() >= maxMessages
                            || waitingBytes >= maxBytes
                            || open >= maxWaitNanos)
                    {
                        return take();
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
     * Takes the oldest exchanges, up to a batch's limits. A single message
     * over {@code max-bytes} is a batch of its own. The caller holds the
     * lock.
     *
     * @return the batch
     */
    private List<Held> take()
    {
        List<Held> batch = new ArrayList<>();
        long bytes = 0;
        while (!waiting.isEmpty() && batch.size() < maxMessages)
        {
            long next = waiting.getFirst().message().body().length;
            if (!batch.isEmpty() && bytes + next > maxBytes)
            {
                break;
            }
            Held held = waiting.removeFirst();
            batch.add(held);
            bytes += next;
            waitingBytes -= next;
        }
        firstAt = nanos.getAsLong();
        lock.notifyAll();
        return batch;
    }

    /**
     * Completes a batch's exchanges.
     *
     * @param batch  the exchanges
     * @param landed true if the batch was landed, so the broker can forget
     *               them; false to have them delivered again
     */
    private void complete(List<Held> batch, boolean landed)
    {
        for (Held held : batch)
        {
            try
            {
                broker.settle(held.exchange(), landed);
            }
            catch (RuntimeException failure)
            {
                // The broker delivers it again; the ID recognises it.
                LOG.warn("batch.settle_failed error={}",
                        failure.getClass().getSimpleName());
            }
            if (!landed)
            {
                held.exchange().setException(new NotLandedException());
            }
            held.callback().done(false);
        }
    }

    /** Stops taking messages after a fatal error, releasing those held. */
    private void stopTaking()
    {
        List<Held> left;
        synchronized (lock)
        {
            running = false;
            left = new ArrayList<>(waiting);
            waiting.clear();
            waitingBytes = 0;
            lock.notifyAll();
        }
        complete(left, false);
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
