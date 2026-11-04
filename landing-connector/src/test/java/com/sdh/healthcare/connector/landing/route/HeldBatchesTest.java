/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.landing.intake.BatchService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HeldBatches}, with a stub lander and broker.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class HeldBatchesTest
{

    /** A long time, so only the other limits close a batch. */
    static final Duration LONG = Duration.ofMinutes(10);

    /** The context the exchanges belong to. */
    DefaultCamelContext camel;

    /** The batches given to the lander, as their bodies. */
    final List<List<String>> landed = new CopyOnWriteArrayList<>();

    /** How each exchange was settled, by body. */
    final Map<String, Boolean> settled = new ConcurrentHashMap<>();

    /** The processor under test. */
    HeldBatches held;

    /** Creates the context. */
    @BeforeEach
    void setUp()
    {
        camel = new DefaultCamelContext();
        camel.start();
    }

    /** Stops the processor and context. */
    @AfterEach
    void tearDown()
    {
        if (held != null)
        {
            held.stop();
        }
        camel.stop();
    }

    /**
     * Starts a processor whose lander answers with the given status.
     *
     * @param maxMessages the most messages in a batch
     * @param maxBytes    the most bytes in a batch
     * @param maxWait     the longest a batch stays open
     * @param status      what the lander says, for a batch
     * @return the processor
     */
    HeldBatches start(int maxMessages, long maxBytes, Duration maxWait,
            Function<List<String>, BatchService.Status> status)
    {
        return start(maxMessages, maxBytes, maxWait, status, true);
    }

    /**
     * Starts a processor for a broker of the given kind.
     *
     * @param maxMessages the most messages in a batch
     * @param maxBytes    the most bytes in a batch
     * @param maxWait     the longest a batch stays open
     * @param status      what the lander says, for a batch
     * @param holds       whether the broker holds exchanges open
     * @return the processor
     */
    HeldBatches start(int maxMessages, long maxBytes, Duration maxWait,
            Function<List<String>, BatchService.Status> status,
            boolean holds)
    {
        held = new HeldBatches(messages ->
        {
            List<String> bodies = new ArrayList<>();
            for (BatchService.Message message : messages)
            {
                bodies.add(new String(message.body(),
                        StandardCharsets.UTF_8));
            }
            landed.add(bodies);
            return new BatchService.Result(status.apply(bodies),
                    bodies.size(), 0);
        }, new HeldBatches.Broker()
        {
            @Override
            public BatchService.Message message(Exchange exchange)
            {
                String body = exchange.getMessage().getBody(String.class);
                return new BatchService.Message(
                        body.getBytes(StandardCharsets.UTF_8), body);
            }

            @Override
            public void settle(Exchange exchange, boolean ok)
            {
                settled.put(exchange.getMessage().getBody(String.class), ok);
            }

            @Override
            public boolean holdsExchange()
            {
                return holds;
            }
        }, maxMessages, maxBytes, maxWait, Duration.ofMillis(1),
                Duration.ofMillis(5), System::nanoTime);
        held.start();
        return held;
    }

    /**
     * Offers messages, and waits for their exchanges to complete.
     *
     * @param bodies the bodies
     * @return the completed exchanges
     * @throws InterruptedException if interrupted while waiting
     */
    List<Exchange> offer(String... bodies) throws InterruptedException
    {
        CountDownLatch done = new CountDownLatch(bodies.length);
        List<Exchange> exchanges = new ArrayList<>();
        for (String body : bodies)
        {
            Exchange exchange = new DefaultExchange(camel);
            exchange.getMessage().setBody(body);
            exchanges.add(exchange);
            held.process(exchange, sync -> done.countDown());
        }
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        return exchanges;
    }

    /**
     * A batch closes at max-messages; its exchanges complete only once it
     * is landed, acknowledged.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsABatchWhenItIsFull() throws Exception
    {
        start(3, 1000, LONG, bodies -> BatchService.Status.LANDED);
        List<Exchange> exchanges = offer("a", "b", "c");
        assertThat(landed).containsExactly(List.of("a", "b", "c"));
        assertThat(exchanges).allSatisfy(
                exchange -> assertThat(exchange.getException()).isNull());
        assertThat(settled).containsOnly(Map.entry("a", true),
                Map.entry("b", true), Map.entry("c", true));
    }

    /**
     * A batch closes at max-wait, however few it holds.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsAPartBatchWhenItsTimeIsUp() throws Exception
    {
        start(100, 1000, Duration.ofMillis(50),
                bodies -> BatchService.Status.LANDED);
        offer("a");
        assertThat(landed).containsExactly(List.of("a"));
    }

    /**
     * A batch closes at max-bytes; a message over the limit is a batch of
     * its own.
     *
     * @throws Exception if the test fails
     */
    @Test
    void splitsBatchesAtMaxBytes() throws Exception
    {
        start(100, 4, Duration.ofMillis(50),
                bodies -> BatchService.Status.LANDED);
        offer("aa", "bb", "cccccc", "d");
        assertThat(landed).containsExactly(List.of("aa", "bb"),
                List.of("cccccc"), List.of("d"));
    }

    /**
     * A batch that isn't landed is tried again in place, keeping its
     * messages, and its exchanges complete only once it lands.
     *
     * @throws Exception if the test fails
     */
    @Test
    void triesAFailedBatchAgainInPlace() throws Exception
    {
        AtomicInteger tries = new AtomicInteger();
        start(1, 1000, LONG, bodies -> tries.incrementAndGet() < 3
                ? BatchService.Status.FAILED : BatchService.Status.LANDED);
        Exchange exchange = offer("a").getFirst();
        assertThat(exchange.getException()).isNull();
        assertThat(landed).containsExactly(List.of("a"), List.of("a"),
                List.of("a"));
        assertThat(settled).containsOnly(Map.entry("a", true));
        assertThat(offer("b").getFirst().getException()).isNull();
    }

    /**
     * After a fatal error nothing more is taken or handed back: the batch
     * stays held, and the consumer is held back, until the stop releases
     * both.
     *
     * @throws Exception if the test fails
     */
    @Test
    void holdsEverythingAfterAFatalErrorUntilStopped() throws Exception
    {
        start(1, 1000, LONG, bodies -> BatchService.Status.FATAL);
        CountDownLatch done = new CountDownLatch(2);
        Exchange first = new DefaultExchange(camel);
        first.getMessage().setBody("a");
        held.process(first, sync -> done.countDown());
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (landed.isEmpty() && System.nanoTime() < deadline)
        {
            Thread.sleep(10);
        }
        Exchange second = new DefaultExchange(camel);
        second.getMessage().setBody("b");
        Thread offering = Thread.ofPlatform().start(
                () -> held.process(second, sync -> done.countDown()));
        Thread.sleep(200);
        assertThat(offering.isAlive()).isTrue();
        assertThat(done.getCount()).isEqualTo(2);
        assertThat(settled).isEmpty();

        held.stop();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        offering.join();
        assertThat(landed).containsExactly(List.of("a"));
        assertThat(first.getException())
                .isInstanceOf(HeldBatches.NotLandedException.class);
        assertThat(second.getException())
                .isInstanceOf(HeldBatches.NotLandedException.class);
        assertThat(settled).containsOnly(Map.entry("a", false),
                Map.entry("b", false));
    }

    /**
     * For a broker that settles later, the exchange completes at once, and
     * the message is settled once its batch is landed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void settlesLaterWhenTheBrokerDoesntHoldExchanges() throws Exception
    {
        start(2, 1000, LONG, bodies -> BatchService.Status.LANDED, false);
        List<Exchange> exchanges = offer("a");
        assertThat(exchanges.getFirst().getException()).isNull();
        assertThat(landed).isEmpty();
        assertThat(settled).isEmpty();
        offer("b");
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (settled.size() < 2 && System.nanoTime() < deadline)
        {
            Thread.sleep(10);
        }
        assertThat(landed).containsExactly(List.of("a", "b"));
        assertThat(settled).containsOnly(Map.entry("a", true),
                Map.entry("b", true));
    }

    /**
     * Stopping completes the exchanges still held with an error.
     *
     * @throws Exception if the test fails
     */
    @Test
    void releasesHeldExchangesWhenStopped() throws Exception
    {
        start(100, 1000, LONG, bodies -> BatchService.Status.LANDED);
        CountDownLatch done = new CountDownLatch(1);
        Exchange exchange = new DefaultExchange(camel);
        exchange.getMessage().setBody("a");
        assertThat(held.process(exchange, sync -> done.countDown()))
                .isFalse();
        held.stop();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(exchange.getException())
                .isInstanceOf(HeldBatches.NotLandedException.class);
        assertThat(settled).containsEntry("a", false);
        assertThat(landed).isEmpty();
    }
}
