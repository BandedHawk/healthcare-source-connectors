/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GivenUpItems}, with {@link ItemLanderTest}'s fixtures.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class GivenUpItemsTest
{

    /** The grace period. */
    static final Duration GRACE = Duration.ofDays(1);

    /** A small digest block, so an item spans several commits. */
    static final int BLOCK = ItemLanderTest.BLOCK;

    /** The intake under test. */
    static final String INTAKE = ItemLanderTest.INTAKE;

    /** The tables, keys and landers. */
    final ItemLanderTest fixtures = new ItemLanderTest();

    /** The landed items table. */
    Table table;

    /** Makes the digests and MACs. */
    RecordIds ids;

    /**
     * Creates the catalog, the table and the KEK.
     *
     * @throws Exception if the KEK can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        fixtures.setUp();
        table = fixtures.table;
        ids = fixtures.ids;
    }

    /**
     * A committer under a new lease epoch.
     *
     * @return the committer
     */
    FencedCommitter committer()
    {
        return fixtures.committer();
    }

    /**
     * A lander under a new lease epoch.
     *
     * @return the lander
     */
    ItemLander lander()
    {
        return fixtures.lander();
    }

    /**
     * A file item for some bytes, with a UUIDv7 ID as landing gives it.
     * Random IDs either side of the sign bit in one Parquet file can make
     * filtered reads miss rows; UUIDv7 IDs all share their first byte.
     *
     * @param bytes the item
     * @param name  its path
     * @return the item
     */
    NewItem fileItem(byte[] bytes, String name)
    {
        NewItem item = fixtures.fileItem(bytes, name);
        return new NewItem(UuidV7.next(System.currentTimeMillis()),
                item.kind(), item.source(), item.originalName(), null);
    }

    /**
     * How many chunk rows an item has.
     *
     * @param itemId the item
     * @return the count
     * @throws Exception if the table can't be read
     */
    long chunkRows(UUID itemId) throws Exception
    {
        return fixtures.chunkRows(itemId);
    }

    /**
     * Reads a landed item back.
     *
     * @param itemId the item
     * @return its bytes
     * @throws Exception if it can't be read
     */
    byte[] readBack(UUID itemId) throws Exception
    {
        return fixtures.readBack(itemId);
    }

    /**
     * Random bytes.
     *
     * @param length how many
     * @return the bytes
     */
    static byte[] data(int length)
    {
        return ItemLanderTest.data(length);
    }

    /**
     * Opens bytes from any position.
     *
     * @param bytes the item
     * @return the opener
     */
    static ItemLander.ItemOpener opener(byte[] bytes)
    {
        return ItemLanderTest.opener(bytes);
    }

    /**
     * Lands part of an item, then gives it up.
     *
     * @return the item given up
     * @throws Exception if the test fails to run
     */
    UUID giveUp() throws Exception
    {
        byte[] bytes = data(3 * BLOCK);
        NewItem item = fileItem(bytes, "vanished-" + UUID.randomUUID());
        ItemLander first = lander();
        first.afterCommit(nextBlock ->
        {
            throw new ItemLanderTest.Crash();
        });
        assertThatThrownBy(() -> first.land(item, opener(bytes)))
                .isInstanceOf(ItemLanderTest.Crash.class);
        lander().clear(item.itemId());
        assertThat(chunkRows(item.itemId())).isPositive();
        return item.itemId();
    }

    /**
     * The data files holding an item's rows.
     *
     * @param itemId the item
     * @return their locations
     * @throws IOException if the table can't be read
     */
    List<String> filesOf(UUID itemId) throws IOException
    {
        table.refresh();
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.equal(LandingTables.ITEM_ID, itemId))
                .planFiles())
        {
            for (FileScanTask task : tasks)
            {
                files.add(task.file().location());
            }
        }
        return files;
    }

    /**
     * Whether a file is still in storage.
     *
     * @param location the file
     * @return true if it is
     */
    boolean stored(String location)
    {
        try
        {
            return table.io().newInputFile(location).exists();
        }
        catch (NotFoundException gone)
        {
            return false;
        }
    }

    /**
     * Runs maintenance with the purge, as of a time.
     *
     * @param at      the time of the run
     * @param compact whether small files are compacted
     * @return what it did
     */
    TableMaintenance.Report maintain(Instant at, boolean compact)
    {
        FencedCommitter committer = committer();
        return new TableMaintenance(committer, new MaintenanceSettings(
                Duration.ofDays(7), 1, compact, 0.75, 2, 8L << 30,
                Duration.ofDays(3)), Clock.fixed(at, ZoneOffset.UTC))
                .run(table, LandingTables.LANDED_AT, Optional.empty(), false,
                        UnaryOperator.identity(), new GivenUpItems(
                                committer, ids, INTAKE, GRACE));
    }

    /**
     * An item given up is recorded as the checkpoint is cleared; its
     * chunks are kept through the grace period, then removed with their
     * files and taken off the record, and the files leave storage in the
     * same run. A landed item beside it is untouched.
     *
     * @throws Exception if the test fails
     */
    @Test
    void removesAGivenUpItemAfterTheGracePeriod() throws Exception
    {
        Instant before = Instant.now();
        UUID givenUp = giveUp();
        assertThat(GivenUpItems.entries(table, ids, INTAKE))
                .singleElement().satisfies(entry ->
                {
                    assertThat(entry.itemId()).isEqualTo(givenUp);
                    assertThat(entry.givenUpAt()).isAfterOrEqualTo(before);
                });
        assertThat(lander().checkpoint()).isEmpty();
        byte[] bytes = data(2 * BLOCK + 5);
        NewItem kept = fileItem(bytes, "kept");
        lander().land(kept, opener(bytes));
        List<String> files = filesOf(givenUp);

        assertThat(maintain(Instant.now(), false).purgedFiles()).isZero();
        assertThat(chunkRows(givenUp)).isPositive();

        TableMaintenance.Report report =
                maintain(Instant.now().plus(GRACE).plusSeconds(60), false);
        assertThat(report.purgedFiles()).isEqualTo(files.size());
        assertThat(chunkRows(givenUp)).isZero();
        table.refresh();
        assertThat(GivenUpItems.entries(table, ids, INTAKE)).isEmpty();
        assertThat(table.properties())
                .doesNotContainKey(GivenUpItems.propertyKey(INTAKE));
        assertThat(files).noneMatch(this::stored);
        assertThat(readBack(kept.itemId())).isEqualTo(bytes);
    }

    /**
     * After compaction has merged a given-up item's chunks with another
     * item's, the shared file is rewritten without them.
     *
     * @throws Exception if the test fails
     */
    @Test
    void rewritesAFileItSharesWithAnotherItem() throws Exception
    {
        UUID givenUp = giveUp();
        byte[] bytes = data(BLOCK + 3);
        NewItem kept = fileItem(bytes, "kept");
        lander().land(kept, opener(bytes));
        assertThat(maintain(Instant.now(), true).compactedFiles())
                .isPositive();
        assertThat(filesOf(givenUp)).isNotEmpty()
                .containsAnyElementsOf(filesOf(kept.itemId()));

        maintain(Instant.now().plus(GRACE).plusSeconds(60), false);
        assertThat(chunkRows(givenUp)).isZero();
        assertThat(readBack(kept.itemId())).isEqualTo(bytes);
    }

    /**
     * An item landed in full is never removed, even if the record names
     * it; it is taken off the record.
     *
     * @throws Exception if the test fails
     */
    @Test
    void neverRemovesALandedItem() throws Exception
    {
        byte[] bytes = data(BLOCK);
        NewItem landed = fileItem(bytes, "landed");
        lander().land(landed, opener(bytes));
        committer().commit(table, transaction -> GivenUpItems.record(
                transaction, ids, INTAKE, new GivenUpItems.Entry(
                        landed.itemId(), Instant.EPOCH)));

        maintain(Instant.now(), false);
        assertThat(readBack(landed.itemId())).isEqualTo(bytes);
        table.refresh();
        assertThat(GivenUpItems.entries(table, ids, INTAKE)).isEmpty();
    }

    /**
     * A record changed without the ID key, or copied from another intake,
     * is refused.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesATamperedRecord() throws Exception
    {
        giveUp();
        table.refresh();
        String stored = table.properties().get(
                GivenUpItems.propertyKey(INTAKE));
        committer().commit(table, transaction -> transaction
                .updateProperties().set(GivenUpItems.propertyKey("other"),
                        stored).set(GivenUpItems.propertyKey(INTAKE),
                        stored.replace("\"items\":\"",
                                "\"items\":\"" + UUID.randomUUID() + "@0,"))
                .commit());
        table.refresh();
        assertThatThrownBy(() -> GivenUpItems.entries(table, ids, INTAKE))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> GivenUpItems.entries(table, ids, "other"))
                .isInstanceOf(CheckpointTamperedException.class);
    }
}
