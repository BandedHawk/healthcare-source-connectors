/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Retention of {@value LandingTables#LANDED_ITEMS} (design §5.3.13): whole
 * days are purged, but never part of an item. An item landed across
 * midnight has rows in two day partitions, so the cutoff moves back to keep
 * all of it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LandingRetentionTest
{

    /** A small chunk, so a few kilobytes make several batches. */
    static final int CHUNK = 1024;

    /** One chunk to a digest block, and a commit after each. */
    static final int BLOCK = CHUNK;

    /** The intake under test. */
    static final String INTAKE = "claims";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** The first day retention keeps, in the tests. */
    static final Instant CUTOFF = Instant.parse("2026-06-10T00:00:00Z");

    /** The landed items table. */
    Table table;

    /** Wraps and unwraps data keys. */
    AesKeyWrapProvider keys;

    /** Makes the digests and MACs, with an all-zero ID key. */
    final SectionIds ids = new SectionIds(new byte[32]);

    /** Commits under epoch 1. */
    FencedCommitter committer;

    /**
     * Creates the catalog, the table and the KEK.
     *
     * @throws Exception if the KEK can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        table = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(table);
    }

    /**
     * An item landed across midnight keeps every row, and the cutoff moves
     * back to its start; an older item is purged whole.
     *
     * @throws Exception if landing or maintenance fails
     */
    @Test
    void purgesWholeDaysButNeverPartOfAnItem() throws Exception
    {
        byte[] old = data(3 * CHUNK);
        byte[] straddling = data(4 * CHUNK);
        LandedItem gone = land(old, CUTOFF.minus(Duration.ofDays(3)));
        // Starts a minute before the cutoff; each batch is 40 s later.
        Instant started = CUTOFF.minusSeconds(60);
        LandedItem kept = land(straddling, started);
        assertThat(days(kept.itemId())).as("rows on both sides of midnight")
                .hasSize(2);

        assertThat(LandedItems.keepFrom(table, CUTOFF, Optional.empty()))
                .isEqualTo(started);

        Clock now = Clock.fixed(CUTOFF.plus(Duration.ofDays(2))
                .plus(Duration.ofHours(12)), ZoneOffset.UTC);
        TableMaintenance.Report report = new TableMaintenance(committer,
                MaintenanceSettings.DEFAULT, now)
                .run(table, LandingTables.LANDED_AT,
                        Optional.of(Duration.ofDays(2)), false,
                        cutoff -> keepFrom(cutoff));
        assertThat(report.retentionDeletedFiles()).isPositive();

        assertThat(LandedItems.summary(table, gone.itemId())).isEmpty();
        assertThat(days(gone.itemId())).isEmpty();
        assertThat(read(kept.itemId())).isEqualTo(straddling);
    }

    /**
     * Without the guard, the same purge takes the first rows of the item
     * landed across midnight: the case the guard is for.
     *
     * @throws Exception if landing or maintenance fails
     */
    @Test
    void plainDayRetentionWouldSplitAnItem() throws Exception
    {
        LandedItem split = land(data(4 * CHUNK), CUTOFF.minusSeconds(60));
        Clock now = Clock.fixed(CUTOFF.plus(Duration.ofDays(2))
                .plus(Duration.ofHours(12)), ZoneOffset.UTC);
        new TableMaintenance(committer, MaintenanceSettings.DEFAULT, now)
                .run(table, LandingTables.LANDED_AT,
                        Optional.of(Duration.ofDays(2)), false);
        assertThat(LandedItems.summary(table, split.itemId())).isPresent();
        assertThat(days(split.itemId())).hasSize(1);
    }

    /**
     * An item still being landed is kept from when it started, even if
     * none of its rows reaches the cutoff yet.
     *
     * @throws Exception if the table can't be read
     */
    @Test
    void keepsTheItemStillBeingLanded() throws Exception
    {
        Instant started = CUTOFF.minus(Duration.ofDays(5));
        LandingCheckpoint unfinished = new LandingCheckpoint(UUID.randomUUID(),
                ItemKind.FILE, null, started, 2, "", false, 0, null, null);
        assertThat(LandedItems.keepFrom(table, CUTOFF,
                Optional.of(unfinished))).isEqualTo(started);
        LandingCheckpoint finished = new LandingCheckpoint(UUID.randomUUID(),
                ItemKind.FILE, null, started, 2, "", true, 10, "d", started);
        assertThat(LandedItems.keepFrom(table, CUTOFF,
                Optional.of(finished))).isEqualTo(CUTOFF);
    }

    /**
     * The guard, as the landing node applies it.
     *
     * @param cutoff the first instant retention would keep
     * @return where to keep rows from
     */
    Instant keepFrom(Instant cutoff)
    {
        try
        {
            return LandedItems.keepFrom(table, cutoff, Optional.empty());
        }
        catch (Exception failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Lands an item, one chunk per batch, with each batch 40 s after the
     * last.
     *
     * @param bytes   the item
     * @param startAt when landing starts
     * @return the landed item
     * @throws Exception if landing fails
     */
    LandedItem land(byte[] bytes, Instant startAt) throws Exception
    {
        ItemLander lander = new ItemLander(table, committer, keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK)
                        .withSizes(CHUNK, BLOCK, BLOCK)
                        .withRetry(new RetryPolicy(2, Duration.ofMillis(1),
                                Duration.ofMillis(1))),
                new SteppingClock(startAt, Duration.ofSeconds(40)));
        String name = "item-" + startAt.toEpochMilli();
        return lander.land(new NewItem(UUID.randomUUID(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, name, bytes.length, "1700000000000"),
                name, null),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length - (int) offset));
    }

    /**
     * The UTC days an item has rows in.
     *
     * @param itemId the item
     * @return the days
     * @throws Exception if the table can't be read
     */
    Set<String> days(UUID itemId) throws Exception
    {
        table.refresh();
        Set<String> days = new HashSet<>();
        if (table.currentSnapshot() == null)
        {
            return days;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(LandingTables.ITEM_ID, itemId))
                .select(LandingTables.LANDED_AT).build())
        {
            for (Record row : rows)
            {
                days.add(row.getField(LandingTables.LANDED_AT).toString()
                        .substring(0, 10));
            }
        }
        return days;
    }

    /**
     * Reads an item back, checking every chunk and its digest.
     *
     * @param itemId the item
     * @return its bytes
     * @throws Exception if it is missing or damaged
     */
    byte[] read(UUID itemId) throws Exception
    {
        table.refresh();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        LandedItems.read(table, itemId, keys, ids, output);
        return output.toByteArray();
    }

    /**
     * Random bytes, the same each time for the same length.
     *
     * @param length how many
     * @return the bytes
     */
    static byte[] data(int length)
    {
        byte[] bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }

    /** A clock that moves on by a fixed step each time it is read. */
    static final class SteppingClock extends Clock
    {

        /** The next instant it gives. */
        private Instant next;

        /** How far it moves each time. */
        private final Duration step;

        /**
         * Creates the clock.
         *
         * @param start the first instant it gives
         * @param step  how far it moves each time
         */
        SteppingClock(Instant start, Duration step)
        {
            this.next = start;
            this.step = step;
        }

        @Override
        public ZoneId getZone()
        {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return this;
        }

        @Override
        public synchronized Instant instant()
        {
            Instant now = next;
            next = next.plus(step);
            return now;
        }
    }
}
