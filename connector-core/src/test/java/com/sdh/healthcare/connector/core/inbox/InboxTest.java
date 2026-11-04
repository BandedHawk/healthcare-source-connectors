/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Inbox}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class InboxTest
{

    /** A quick retry policy. */
    static final RetryPolicy RETRY =
            new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1));

    /** The inbox table. */
    Table table;

    /** The last lease epoch handed out. */
    long epoch;

    /** MACs the position. */
    final SectionIds ids = new SectionIds(new byte[32]);

    /** Creates the table. */
    @BeforeEach
    void setUp()
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        table = InboxTables.inbox(catalog, "sdh_ingest");
    }

    /**
     * An inbox as a fresh start of the splitter would load it.
     *
     * @return the inbox
     * @throws Exception if it can't be loaded
     */
    Inbox inbox() throws Exception
    {
        long ours = ++epoch;
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return ours;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(table);
        Inbox inbox = new Inbox(table, committer, ids, "splitter", RETRY,
                Clock.systemUTC());
        inbox.load();
        return inbox;
    }

    /**
     * A message for a new item.
     *
     * @return the message
     */
    static ConnectorMessage.ItemLanded landed()
    {
        Instant now = Instant.now();
        return new ConnectorMessage.ItemLanded(UUID.randomUUID(), "claims",
                "file", 42, "abcdefghijklmnop", now,
                new ConnectorMessage.StorageRef("sdh", "sdh_landing",
                        "landed_items", LocalDate.now(), 1, "kek/1"));
    }

    /**
     * Items come out in the order they went in, once each, and the
     * position survives a restart.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keepsOrderDropsRepeatsAndRemembersThePosition() throws Exception
    {
        Inbox inbox = inbox();
        ConnectorMessage.ItemLanded first = landed();
        ConnectorMessage.ItemLanded second = landed();
        assertThat(inbox.add(first)).isTrue();
        assertThat(inbox.add(second)).isTrue();
        assertThat(inbox.add(first)).as("a redelivery").isFalse();
        assertThat(inbox.backlog()).isEqualTo(2);

        Inbox.Entry next = inbox.next(Duration.ZERO).orElseThrow();
        assertThat(next.itemId()).isEqualTo(first.fileId());
        assertThat(next.seq()).isEqualTo(1);
        assertThat(next.size()).isEqualTo(42);
        assertThat(next.intake()).isEqualTo("claims");
        inbox.done(next.seq());
        assertThat(inbox.next(Duration.ZERO).orElseThrow().itemId())
                .isEqualTo(second.fileId());

        Inbox restarted = inbox();
        assertThat(restarted.backlog()).isEqualTo(1);
        assertThat(restarted.add(first)).as("still known").isFalse();
        Inbox.Entry resumed = restarted.next(Duration.ZERO).orElseThrow();
        assertThat(resumed.itemId()).isEqualTo(second.fileId());
        restarted.done(resumed.seq());
        assertThat(restarted.next(Duration.ZERO)).isEmpty();
        assertThat(restarted.add(landed())).isTrue();
        assertThat(restarted.next(Duration.ZERO).orElseThrow().seq())
                .isEqualTo(3);
    }

    /**
     * A waiting reader wakes when an item arrives.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wakesAWaitingReader() throws Exception
    {
        Inbox inbox = inbox();
        ConnectorMessage.ItemLanded item = landed();
        Thread.ofVirtual().start(() ->
        {
            try
            {
                Thread.sleep(100);
                inbox.add(item);
            }
            catch (Exception failure)
            {
                throw new IllegalStateException(failure);
            }
        });
        long started = System.nanoTime();
        assertThat(inbox.next(Duration.ofSeconds(10))).hasValueSatisfying(
                entry -> assertThat(entry.itemId()).isEqualTo(item.fileId()));
        assertThat(System.nanoTime() - started)
                .isLessThan(Duration.ofSeconds(5).toNanos());
    }

    /**
     * A position that was changed outside the splitter is refused.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesATamperedPosition() throws Exception
    {
        Inbox inbox = inbox();
        inbox.add(landed());
        inbox.done(1);
        String stored = table.properties().get(Inbox.PROPERTY_PREFIX
                + "splitter");
        table.updateProperties().set(Inbox.PROPERTY_PREFIX + "splitter",
                stored.replace("1", "7")).commit();
        assertThatThrownBy(this::inbox)
                .isInstanceOf(CheckpointTamperedException.class);
    }
}
