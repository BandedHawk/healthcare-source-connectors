/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
 * Tests for {@link ItemLander} and {@link LandedItems}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ItemLanderTest
{

    /** A small chunk, so items of a few kilobytes span many chunks. */
    static final int CHUNK = 1024;

    /** Four chunks to a digest block. */
    static final int BLOCK = 4 * CHUNK;

    /** The intake under test. */
    static final String INTAKE = "claims-sftp";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** The catalog the landing tables are in. */
    InMemoryCatalog catalog;

    /** The landed items table. */
    Table table;

    /** Wraps and unwraps data keys. */
    AesKeyWrapProvider keys;

    /** Makes the digests and MACs, with an all-zero ID key. */
    final SectionIds ids = new SectionIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /**
     * Creates the catalog, the table and the KEK.
     *
     * @throws Exception if the KEK can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        table = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
    }

    /**
     * A committer under a new lease epoch, as after a restart.
     *
     * @return the committer, established on the table
     */
    FencedCommitter committer()
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
        return committer;
    }

    /**
     * A lander that commits after every block, as after a restart.
     *
     * @return the lander
     */
    ItemLander lander()
    {
        return new ItemLander(table, committer(), keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK)
                        .withSizes(CHUNK, BLOCK, BLOCK)
                        .withRetry(new RetryPolicy(2, Duration.ofMillis(1),
                                Duration.ofMillis(1))),
                Clock.systemUTC());
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

    /**
     * Opens bytes from any position, as a file or object source can.
     *
     * @param bytes the item
     * @return the opener
     */
    static ItemLander.ItemOpener opener(byte[] bytes)
    {
        return offset -> new ByteArrayInputStream(bytes, (int) offset,
                bytes.length - (int) offset);
    }

    /**
     * A file item with a source reference for its bytes.
     *
     * @param bytes the item
     * @param name  its path
     * @return the item
     */
    NewItem fileItem(byte[] bytes, String name)
    {
        return new NewItem(UUID.randomUUID(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, name, bytes.length, "1700000000000"),
                name, null);
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
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        LandedItems.read(table, itemId, keys, ids, output);
        return output.toByteArray();
    }

    /**
     * How many chunk rows an item has.
     *
     * @param itemId the item
     * @return the number of rows with a chunk index of 0 or more
     * @throws Exception if the table can't be read
     */
    long chunkRows(UUID itemId) throws Exception
    {
        table.refresh();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(LandingTables.ITEM_ID, itemId),
                        Expressions.greaterThanOrEqual(
                                LandingTables.CHUNK_INDEX, 0)))
                .build())
        {
            long count = 0;
            for (Record ignored : rows)
            {
                count++;
            }
            return count;
        }
    }

    /**
     * How many snapshots the table has.
     *
     * @return the count
     */
    long snapshotCount()
    {
        table.refresh();
        long count = 0;
        for (Object ignored : table.snapshots())
        {
            count++;
        }
        return count;
    }

    /**
     * An item spanning several blocks is stored with its summary row, and
     * reads back whole, with its name, and the same digest as the archive
     * of the same bytes.
     */
    @Test
    void landsAndReadsBackWithSummaryNameAndDigest() throws Exception
    {
        byte[] bytes = data(3 * BLOCK + 100);
        NewItem item = fileItem(bytes, "inbound/jones-mrn-123.xml");
        LandedItem landed = lander().land(item, opener(bytes));

        assertThat(landed.size()).isEqualTo(bytes.length);
        assertThat(landed.intake()).isEqualTo(INTAKE);
        assertThat(landed.snapshotId()).isPositive();
        assertThat(readBack(item.itemId())).isEqualTo(bytes);
        assertThat(chunkRows(item.itemId())).isEqualTo(13);

        Record summary =
                LandedItems.summary(table, item.itemId()).orElseThrow();
        assertThat(summary.getField(LandingTables.ITEM_DIGEST))
                .isEqualTo(landed.digest());
        assertThat(summary.getField(LandingTables.CHUNK_COUNT)).isEqualTo(13);
        assertThat(summary.getField(LandingTables.KIND)).isEqualTo("file");
        assertThat(summary.getField(LandingTables.SOURCE_REF_HASH))
                .isEqualTo(item.source().hash());
        assertThat(LandedItems.originalName(summary, keys))
                .contains("inbound/jones-mrn-123.xml");
        String sha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(bytes));
        assertThat(landed.sha256()).isEqualTo(sha256);
        assertThat(summary.getField(LandingTables.ITEM_SHA256))
                .isEqualTo(sha256);
        assertThat(lander().land(item, opener(bytes)).sha256())
                .isEqualTo(sha256);
    }

    /** No name or path appears in the table's clear columns. */
    @Test
    void theNameIsStoredOnlyEncrypted() throws Exception
    {
        byte[] bytes = data(100);
        NewItem item = fileItem(bytes, "smith-john-19700101.hl7");
        lander().land(item, opener(bytes));
        table.refresh();
        try (CloseableIterable<Record> rows =
                IcebergGenerics.read(table).build())
        {
            for (Record row : rows)
            {
                for (int field = 0; field < row.size(); field++)
                {
                    Object value = row.get(field);
                    String text = value instanceof ByteBuffer buffer
                            ? new String(LandedItems.bytes(buffer),
                                    java.nio.charset.StandardCharsets.UTF_8)
                            : String.valueOf(value);
                    assertThat(text).doesNotContain("smith");
                }
            }
        }
    }

    /** Empty items and items of exactly whole chunks land and read back. */
    @Test
    void landsEmptyAndWholeChunkItems() throws Exception
    {
        for (int length : new int[] {0, CHUNK, BLOCK, 2 * BLOCK})
        {
            byte[] bytes = data(length);
            NewItem item = fileItem(bytes, "f" + length);
            LandedItem landed = lander().land(item, opener(bytes));
            assertThat(landed.size()).isEqualTo(length);
            assertThat(readBack(item.itemId())).isEqualTo(bytes);
            assertThat(chunkRows(item.itemId()))
                    .isEqualTo(Math.max(1, length / CHUNK));
        }
    }

    /** A push has no source reference and no known size. */
    @Test
    void landsAStreamOfUnknownSize() throws Exception
    {
        byte[] bytes = data(2 * BLOCK + 7);
        NewItem item = new NewItem(UUID.randomUUID(), ItemKind.PUSH, null,
                null, null);
        LandedItem landed = lander().land(item, offset ->
        {
            assertThat(offset).isZero();
            return new ByteArrayInputStream(bytes);
        });
        assertThat(landed.source()).isNull();
        assertThat(readBack(item.itemId())).isEqualTo(bytes);
        Record summary =
                LandedItems.summary(table, item.itemId()).orElseThrow();
        assertThat(summary.getField(LandingTables.SOURCE_REF_HASH)).isNull();
        assertThat(LandedItems.originalName(summary, keys)).isEmpty();
    }

    /**
     * After a crash part-way, landing the same item resumes at the next
     * block: no chunk is written twice, and the digest is the same as
     * landing it in one go.
     */
    @Test
    void resumesAfterACrashWithTheSameDigest() throws Exception
    {
        byte[] bytes = data(3 * BLOCK + 100);
        String expected = lander().land(fileItem(bytes, "reference"),
                opener(bytes)).digest();

        NewItem item = fileItem(bytes, "crashes");
        ItemLander first = lander();
        first.afterCommit(nextBlock ->
        {
            if (nextBlock == 2)
            {
                throw new Crash();
            }
        });
        assertThatThrownBy(() -> first.land(item, opener(bytes)))
                .isInstanceOf(Crash.class);
        LandingCheckpoint stored = lander().checkpoint().orElseThrow();
        assertThat(stored.done()).isFalse();
        assertThat(stored.nextBlock()).isEqualTo(2);

        List<Long> offsets = new ArrayList<>();
        LandedItem landed = lander().land(item, offset ->
        {
            offsets.add(offset);
            return opener(bytes).open(offset);
        });
        assertThat(offsets).containsExactly(2L * BLOCK);
        assertThat(landed.digest()).isEqualTo(expected);
        assertThat(readBack(item.itemId())).isEqualTo(bytes);
        assertThat(chunkRows(item.itemId())).isEqualTo(13);
        // The plain hash can't be carried over a restart.
        assertThat(landed.sha256()).isNull();
    }

    /** Landing a finished item again changes nothing. */
    @Test
    void landingAFinishedItemAgainReturnsIt() throws Exception
    {
        byte[] bytes = data(BLOCK + 1);
        NewItem item = fileItem(bytes, "twice");
        LandedItem landed = lander().land(item, opener(bytes));
        long snapshots = snapshotCount();

        LandedItem again = lander().land(item, offset ->
        {
            throw new AssertionError("must not read the source");
        });
        assertThat(again.digest()).isEqualTo(landed.digest());
        assertThat(again.kekRef()).isEqualTo(KEK);
        table.refresh();
        // Only the second lander's fencing commit was added.
        assertThat(snapshotCount()).isEqualTo(snapshots + 1);
        assertThat(lander().lastLanded()).hasValueSatisfying(
                last -> assertThat(last.itemId()).isEqualTo(item.itemId()));
    }

    /** A source whose size or version changed can't be resumed. */
    @Test
    void refusesToResumeAChangedSource() throws Exception
    {
        byte[] bytes = data(3 * BLOCK);
        NewItem item = fileItem(bytes, "changes");
        ItemLander first = lander();
        first.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        assertThatThrownBy(() -> first.land(item, opener(bytes)))
                .isInstanceOf(Crash.class);

        NewItem rewritten = new NewItem(item.itemId(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, "changes", bytes.length,
                        "1700000099999"), "changes", null);
        assertThatThrownBy(() -> lander().land(rewritten, opener(bytes)))
                .isInstanceOf(ItemLander.ItemChangedException.class);

        // Another item can't start until the unfinished one is cleared.
        byte[] other = data(10);
        assertThatThrownBy(() -> lander().land(fileItem(other, "o"),
                opener(other))).isInstanceOf(IllegalStateException.class);
        lander().clear(item.itemId());
        assertThat(lander().checkpoint()).isEmpty();
        lander().land(fileItem(other, "o"), opener(other));
    }

    /**
     * Bytes that don't match the size the source gave mean the file was
     * changed while it was read: nothing is finished.
     */
    @Test
    void refusesBytesThatDoNotMatchTheSourceSize() throws Exception
    {
        byte[] bytes = data(BLOCK + 10);
        NewItem shorter = new NewItem(UUID.randomUUID(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, "grew", bytes.length - 1, "1"),
                null, null);
        assertThatThrownBy(() -> lander().land(shorter, opener(bytes)))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        assertThat(LandedItems.summary(table, shorter.itemId())).isEmpty();

        lander().clear(shorter.itemId());
        NewItem longer = new NewItem(UUID.randomUUID(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, "shrank", bytes.length + 1, "1"),
                null, null);
        assertThatThrownBy(() -> lander().land(longer, opener(bytes)))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        assertThat(LandedItems.summary(table, longer.itemId())).isEmpty();
    }

    /** An item that isn't fully landed can't be read. */
    @Test
    void anUnfinishedItemCannotBeRead() throws Exception
    {
        byte[] bytes = data(3 * BLOCK);
        NewItem item = fileItem(bytes, "unfinished");
        ItemLander first = lander();
        first.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        assertThatThrownBy(() -> first.land(item, opener(bytes)))
                .isInstanceOf(Crash.class);
        assertThat(chunkRows(item.itemId())).isEqualTo(4);
        assertThatThrownBy(() -> readBack(item.itemId()))
                .hasMessageContaining("isn't fully landed");
    }

    /** A checkpoint changed outside the connector stops it. */
    @Test
    void aTamperedCheckpointIsRefused() throws Exception
    {
        byte[] bytes = data(10);
        lander().land(fileItem(bytes, "x"), opener(bytes));
        String key = LandingCheckpoint.propertyKey(INTAKE);
        String stored = table.properties().get(key);
        table.updateProperties()
                .set(key, stored.replace("\"done\":true", "\"done\":false"))
                .commit();
        assertThatThrownBy(() -> lander().checkpoint())
                .isInstanceOf(CheckpointTamperedException.class);
    }

    /** The item.landed message says where the item is. */
    @Test
    void theMessageNamesTheItemAndWhereItIs() throws Exception
    {
        byte[] bytes = data(10);
        NewItem item = fileItem(bytes, "m");
        LandedItem landed = lander().land(item, opener(bytes));
        ConnectorMessage.ItemLanded message =
                landed.message("prod", LandingTables.DEFAULT_NAMESPACE);
        assertThat(message.messageId()).isEqualTo("il-" + item.itemId());
        assertThat(message.kind()).isEqualTo("file");
        assertThat(message.storage().table())
                .isEqualTo(LandingTables.LANDED_ITEMS);
        assertThat(message.storage().snapshotId())
                .isEqualTo(landed.snapshotId());
        assertThat(message.storage().kekRef()).isEqualTo(KEK);
    }

    /** Thrown by a test hook to simulate a crash. */
    static final class Crash extends RuntimeException
    {
    }
}
