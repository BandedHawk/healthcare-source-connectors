/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.IntakeHealth;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link BrokerWatch}: what each check tells the intake's
 * health, and the restart of a consumer that gave up.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerWatchTest
{

    /** Small batches. */
    static final LandingProperties.Batch BATCH = new LandingProperties.Batch(
            10, DataSize.ofKilobytes(64), Duration.ofMillis(500));

    /** The fake clock, in nanoseconds. */
    final AtomicLong now = new AtomicLong();

    /** The intake's health. */
    final IntakeHealth health = new IntakeHealth("claims", now::get);

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
    static BrokerSettings settings(String type, String uri)
    {
        return BrokerSettings.of(new LandingProperties.Broker(type, uri,
                BATCH, Duration.ofHours(1), Duration.ofSeconds(30),
                new LandingProperties.Rabbitmq("r:5672", "/", null, null,
                        false),
                new LandingProperties.Artemis("tcp://a:61616", null, null,
                        "q"),
                new LandingProperties.Eventhubs(null, null, null)));
    }

    /**
     * A watch on Kafka, its check answering as given, one answer a run.
     *
     * @param answers the answers in turn; null for answered
     * @return the watch
     */
    BrokerWatch watch(IntakeProblem... answers)
    {
        List<IntakeProblem> left = new ArrayList<>(Arrays.asList(answers));
        return watch(() -> left.remove(0));
    }

    /**
     * A watch on Kafka with a given check, never scheduled: each test runs
     * the check itself.
     *
     * @param answer gives each check's answer
     * @return the watch
     */
    BrokerWatch watch(Supplier<IntakeProblem> answer)
    {
        BrokerWatch watch = new BrokerWatch(
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
            public IntakeProblem check()
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
        BrokerWatch watch = watch(IntakeProblem.AUTH, null);
        watch.runCheck();
        assertThat(health.reachable()).isFalse();
        assertThat(health.problem()).isEqualTo(IntakeProblem.AUTH);
        now.addAndGet(Duration.ofSeconds(40).toNanos());
        watch.runCheck();
        assertThat(health.reachable()).isTrue();
        assertThat(health.secondsSinceAnswered()).isZero();
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
        assertThat(health.problem()).isEqualTo(IntakeProblem.NETWORK);
    }

    /**
     * A consumer that gave up is reported at once, and restarted once its
     * broker answers again, only once.
     */
    @Test
    void restartsAConsumerThatGaveUpOnceTheBrokerAnswers()
    {
        BrokerWatch watch = watch(IntakeProblem.NOT_FOUND, null, null);
        watch.gaveUp(IntakeProblem.NOT_FOUND);
        assertThat(health.problem()).isEqualTo(IntakeProblem.NOT_FOUND);
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
        watch.gaveUp(IntakeProblem.PERMISSION);
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
            new BrokerWatch(settings("sqs", "aws2-sqs:q"), health, camel)
                    .bind(null);
            assertThat(camel.getRegistry().lookupByNameAndType(
                    BrokerSettings.POLL_STRATEGY_BEAN,
                    PollingConsumerPollStrategy.class))
                    .isInstanceOf(SqsPollStrategy.class);

            camel.getComponent(BrokerWatch.PUBSUB_COMPONENT);
            new BrokerWatch(settings("pubsub", "google-pubsub:p:s"), health,
                    camel).bind(null);
            assertThat(camel.getComponent(BrokerWatch.PUBSUB_COMPONENT))
                    .isInstanceOf(WatchedPubsubComponent.class);
        }
        CachingConnectionFactory factory = new CachingConnectionFactory();
        try
        {
            new BrokerWatch(settings("rabbitmq", "spring-rabbitmq:x?queues=q"),
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
        assertThatThrownBy(() -> new BrokerWatch(settings("artemis", null),
                health, null)).hasMessageContaining("Artemis");
    }
}
