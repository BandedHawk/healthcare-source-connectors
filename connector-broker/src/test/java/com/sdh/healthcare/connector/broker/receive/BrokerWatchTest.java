/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link BrokerWatch}: what each check tells the broker's
 * health, and the restart of a consumer that gave up.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerWatchTest
{

    /** Small batches. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            10, DataSize.ofKilobytes(64), Duration.ofMillis(500));

    /** The route the watch restarts. */
    static final String ROUTE = "receive";

    /** The broker's health. */
    final RecordingHealth health = new RecordingHealth();

    /** How many times the route was restarted. */
    final AtomicInteger restarts = new AtomicInteger();

    /** Whether the next restart fails. */
    boolean restartFails;

    /**
     * Settings for a broker.
     *
     * @param type the broker
     * @param uri  its URI
     * @return the settings
     */
    static ReceiveSettings settings(String type, String uri)
    {
        return ReceiveSettings.of(new ReceiveProperties(type, uri, BATCH,
                Duration.ofSeconds(30),
                new ReceiveProperties.Rabbitmq("r:5672", "/", null, null,
                        false),
                new ReceiveProperties.Artemis("tcp://a:61616", null, null,
                        "q"),
                new ReceiveProperties.Eventhubs(null, null, null)),
                "sdh.test.broker", Set.of());
    }

    /**
     * A watch on Kafka, its check answering as given, one answer a run.
     *
     * @param answers the answers in turn; null for answered
     * @return the watch
     */
    BrokerWatch watch(BrokerProblem... answers)
    {
        List<BrokerProblem> left = new ArrayList<>(Arrays.asList(answers));
        return watch(() -> left.remove(0));
    }

    /**
     * A watch on Kafka with a given check, never scheduled: each test runs
     * the check itself.
     *
     * @param answer gives each check's answer
     * @return the watch
     */
    BrokerWatch watch(Supplier<BrokerProblem> answer)
    {
        BrokerWatch watch = new BrokerWatch(ROUTE,
                settings("kafka", "kafka:t?groupId=g"), health, null, () ->
                {
                    if (restartFails)
                    {
                        throw new IllegalStateException("not now");
                    }
                    restarts.incrementAndGet();
                });
        watch.use(new BrokerCheck()
        {
            @Override
            public BrokerProblem check()
            {
                return answer.get();
            }

            @Override
            public void close()
            {
            }
        });
        return watch;
    }

    /** A failed check reports its reason; one that answers ends it. */
    @Test
    void reportsEachCheckToTheHealth()
    {
        BrokerWatch watch = watch(BrokerProblem.AUTH, null);
        watch.runCheck();
        assertThat(health.reachable()).isFalse();
        assertThat(health.problem()).isEqualTo(BrokerProblem.AUTH);
        watch.runCheck();
        assertThat(health.reachable()).isTrue();
        assertThat(health.answers()).isEqualTo(1);
        assertThat(restarts).hasValue(0);
    }

    /** A check that throws counts as a network problem. */
    @Test
    void countsABrokenCheckAsANetworkProblem()
    {
        BrokerWatch watch = watch(() ->
        {
            throw new IllegalStateException("broken client");
        });
        watch.runCheck();
        assertThat(health.problem()).isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * A consumer that gave up is reported at once, and restarted once its
     * broker answers again, only once.
     */
    @Test
    void restartsAConsumerThatGaveUpOnceTheBrokerAnswers()
    {
        BrokerWatch watch = watch(BrokerProblem.NOT_FOUND, null, null);
        watch.gaveUp(BrokerProblem.NOT_FOUND);
        assertThat(health.problem()).isEqualTo(BrokerProblem.NOT_FOUND);
        watch.runCheck();
        assertThat(restarts).hasValue(0);
        watch.runCheck();
        assertThat(restarts).hasValue(1);
        assertThat(health.reachable()).isTrue();
        watch.runCheck();
        assertThat(restarts).hasValue(1);
    }

    /** A restart that fails is tried again at the next check. */
    @Test
    void triesAFailedRestartAgain()
    {
        BrokerWatch watch = watch(null, null);
        watch.gaveUp(BrokerProblem.PERMISSION);
        restartFails = true;
        watch.runCheck();
        assertThat(health.reachable()).isFalse();
        restartFails = false;
        watch.runCheck();
        assertThat(restarts).hasValue(1);
        assertThat(health.reachable()).isTrue();
    }

    /**
     * SQS gets the poll strategy, and Pub/Sub the component that watches
     * its subscribers, in place of any already there.
     *
     * @throws Exception if Camel fails
     */
    @Test
    void bindsWhatEachBrokerNeeds() throws Exception
    {
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            new BrokerWatch(ROUTE, settings("sqs", "aws2-sqs:q"), health,
                    camel).bind(null);
            assertThat(camel.getRegistry().lookupByNameAndType(
                    ReceiveSettings.POLL_STRATEGY_BEAN,
                    PollingConsumerPollStrategy.class))
                    .isInstanceOf(SqsPollStrategy.class);

            camel.getComponent(BrokerWatch.PUBSUB_COMPONENT);
            new BrokerWatch(ROUTE, settings("pubsub", "google-pubsub:p:s"),
                    health, camel).bind(null);
            assertThat(camel.getComponent(BrokerWatch.PUBSUB_COMPONENT))
                    .isInstanceOf(WatchedPubsubComponent.class);
        }
        CachingConnectionFactory factory = new CachingConnectionFactory();
        try
        {
            new BrokerWatch(ROUTE,
                    settings("rabbitmq", "spring-rabbitmq:x?queues=q"),
                    health, null).bind(factory);
        }
        finally
        {
            factory.destroy();
        }
    }

    /** Artemis reports its own outages. */
    @Test
    void refusesArtemis()
    {
        assertThatThrownBy(() -> new BrokerWatch(ROUTE,
                settings("artemis", null), health, null))
                .hasMessageContaining("Artemis");
    }
}
