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
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
    final RecordIds ids = new RecordIds(new byte[32]);

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
     * A file item with a source reference for its bytes, and a
     * time-ordered ID, as an intake gives it.
     *
     * @param bytes the item
     * @param name  its path
     * @return the item
     */
    NewItem fileItem(byte[] bytes, String name)
    {
        return new NewItem(UuidV7.next(System.currentTimeMillis()),
                ItemKind.FILE,
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
     *
     * @throws Exception if the test fails to run
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
    /**
     * A message batch keeps its message index on the summary row; the keys
     * landed from a time on can be read back for the intake, and an index
     * that doesn't fit the item is refused.
     *
     * @throws Exception if the test fails
     */
    @Test
    void keepsAMessageBatchsMessageIndex() throws Exception
    {
        byte[] bytes = data(30);
        MessageIndex messageIndex = new MessageIndex(List.of(0L, 10L, 25L),
                Arrays.asList("k-0", null, "k-2"));
        NewItem item = new NewItem(UUID.randomUUID(), ItemKind.MESSAGE_BATCH,
                null, null, null, messageIndex);
        Instant before = Instant.now().minusSeconds(1);
        lander().land(item, opener(bytes));

        Record summary =
                LandedItems.summary(table, item.itemId()).orElseThrow();
        assertThat(LandedItems.messageIndex(summary)).contains(messageIndex);
        assertThat(LandedItems.messageKeys(table, INTAKE, before))
                .containsExactlyInAnyOrder("k-0", "k-2");
        assertThat(LandedItems.messageKeys(table, INTAKE,
                Instant.now().plusSeconds(86_400))).isEmpty();
        assertThat(LandedItems.messageKeys(table, "other", before)).isEmpty();
        assertThat(LandedItems.messageIndex(LandedItems.summary(table,
                lander().land(fileItem(bytes, "f"), opener(bytes)).itemId())
                .orElseThrow())).isEmpty();

        NewItem tooShort = new NewItem(UUID.randomUUID(),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(List.of(0L, 40L), Arrays.asList(null, null)));
        assertThatThrownBy(() -> lander().land(tooShort, opener(bytes)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NewItem(UUID.randomUUID(),
                ItemKind.MESSAGE_BATCH, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(LandedItems.messageRefs(summary, keys)).isEmpty();
        assertThat(LandedItems.messageReplaces(summary, keys)).isEmpty();
    }

    /**
     * A batch's broker IDs, and the messages they replace, are kept
     * encrypted and read back in order, nulls included; the item a push
     * replaces is kept and comes back with it after a restart.
     *
     * @throws Exception if the test fails
     */
    @Test
    void keepsBrokerIdsEncryptedAndTheReplacedItem() throws Exception
    {
        byte[] bytes = data(30);
        List<String> refs = Arrays.asList("smith-1", null, "smith-\"\u00e9");
        List<String> links = Arrays.asList(null, null,
                UUID.randomUUID() + "/smith-0");
        NewItem batch = new NewItem(UUID.randomUUID(),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(List.of(0L, 10L, 25L),
                        Arrays.asList("k-0", null, "k-2")), refs, links);
        lander().land(batch, opener(bytes));
        Record summary =
                LandedItems.summary(table, batch.itemId()).orElseThrow();
        assertThat(LandedItems.messageRefs(summary, keys)).contains(refs);
        assertThat(new String(LandedItems.bytes(
                summary.getField(LandingTables.MESSAGE_REFS_ENC)),
                StandardCharsets.ISO_8859_1))
                .doesNotContain("smith");
        assertThat(LandedItems.messageReplaces(summary, keys))
                .contains(links);
        assertThat(new String(LandedItems.bytes(
                summary.getField(LandingTables.MESSAGE_REPLACES_ENC)),
                StandardCharsets.ISO_8859_1))
                .doesNotContain("smith");
        assertThatThrownBy(() -> new NewItem(UUID.randomUUID(),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(List.of(0L), Arrays.asList("k")),
                List.of("a", "b"))).isInstanceOf(
                        IllegalArgumentException.class);
        assertThatThrownBy(() -> new NewItem(UUID.randomUUID(),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(List.of(0L), Arrays.asList("k")),
                List.of("a"), List.of("x", "y"))).isInstanceOf(
                        IllegalArgumentException.class);

        UUID earlier = UUID.randomUUID();
        NewItem push = new NewItem(UUID.randomUUID(), ItemKind.PUSH, null,
                "corr-1", earlier);
        LandedItem landed = lander().land(push, opener(bytes));
        assertThat(landed.replaces()).isEqualTo(earlier);
        assertThat(landed.message("cat", "ns").replaces()).isEqualTo(earlier);
        assertThat(LandedItems.replaces(LandedItems.summary(table,
                push.itemId()).orElseThrow())).contains(earlier);
        assertThat(lander().lastLanded()).hasValueSatisfying(
                last -> assertThat(last.replaces()).isEqualTo(earlier));
    }

    /**
     * The item's name appears in no column in clear, only encrypted.
     *
     * @throws Exception if the test fails to run
     */
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
                                    StandardCharsets.UTF_8)
                            : String.valueOf(value);
                    assertThat(text).doesNotContain("smith");
                }
            }
        }
    }

    /**
     * Empty items and items of exactly whole chunks land and read back.
     *
     * @throws Exception if the test fails to run
     */
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

    /**
     * A push has no source reference and no known size.
     *
     * @throws Exception if the test fails to run
     */
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
     *
     * @throws Exception if the test fails to run
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

    /**
     * Landing a finished item again changes nothing.
     *
     * @throws Exception if the test fails to run
     */
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

    /**
     * A source whose size or version changed can't be resumed.
     *
     * @throws Exception if the test fails to run
     */
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
     *
     * @throws Exception if the test fails to run
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

    /**
     * An item that isn't fully landed can't be read.
     *
     * @throws Exception if the test fails to run
     */
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

    /**
     * A checkpoint changed outside the connector stops it.
     *
     * @throws Exception if the test fails to run
     */
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

    /**
     * The item.landed message says where the item is.
     *
     * @throws Exception if the test fails to run
     */
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

    /**
     * Small items landed together share one data file and one commit; each
     * reads back whole, with the digest and hash it would have alone, and
     * recovery finds them all, by position in the file.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void landsSmallItemsTogetherInOneCommit() throws Exception
    {
        byte[] empty = data(0);
        byte[] small = data(10);
        byte[] blocks = data(2 * BLOCK + 7);
        List<NewItem> items = List.of(fileItem(empty, "g/empty"),
                fileItem(small, "g/small"), fileItem(blocks, "g/blocks"));
        List<byte[]> contents = List.of(empty, small, blocks);
        ItemLander lander = lander();
        long snapshots = snapshotCount();

        ItemLander.GroupLanded result = lander.landAll(List.of(
                new ItemLander.Grouped(items.get(0), opener(empty)),
                new ItemLander.Grouped(items.get(1), opener(small)),
                new ItemLander.Grouped(items.get(2), opener(blocks))),
                3 * BLOCK);

        assertThat(snapshotCount()).isEqualTo(snapshots + 1);
        assertThat(result.changed()).isEmpty();
        assertThat(result.landed()).extracting(LandedItem::itemId)
                .containsExactlyElementsOf(
                        items.stream().map(NewItem::itemId).toList());
        assertThat(result.landed()).extracting(LandedItem::file)
                .containsOnly(result.landed().get(0).file());
        for (int index = 0; index < items.size(); index++)
        {
            byte[] bytes = contents.get(index);
            LandedItem landed = result.landed().get(index);
            assertThat(readBack(landed.itemId())).isEqualTo(bytes);
            assertThat(landed.size()).isEqualTo(bytes.length);
            assertThat(landed.sha256()).isEqualTo(HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes)));
            assertThat(landed.digest()).isEqualTo(lander().land(
                    fileItem(bytes, "alone/" + index), opener(bytes))
                    .digest());
        }
        assertThat(chunkRows(items.get(2).itemId())).isEqualTo(9);
        assertThat(lander().checkpoint()).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.done()).isTrue());
    }

    /**
     * Recovery finds every item of the last group commit, as it was
     * landed, from where its summary row is.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recoveryFindsEveryItemOfTheLastGroup() throws Exception
    {
        byte[] first = data(20);
        byte[] second = data(BLOCK + 3);
        NewItem one = fileItem(first, "r/one");
        NewItem two = fileItem(second, "r/two");
        List<LandedItem> landed = lander().landAll(List.of(
                new ItemLander.Grouped(one, opener(first)),
                new ItemLander.Grouped(two, opener(second))), BLOCK * 2)
                .landed();

        List<LandedItem> found = lander().lastLandedAll();
        assertThat(found).hasSize(2);
        for (int index = 0; index < 2; index++)
        {
            LandedItem expected = landed.get(index);
            LandedItem actual = found.get(index);
            assertThat(actual.itemId()).isEqualTo(expected.itemId());
            assertThat(actual.source()).isEqualTo(expected.source());
            assertThat(actual.digest()).isEqualTo(expected.digest());
            assertThat(actual.size()).isEqualTo(expected.size());
            assertThat(actual.sha256()).isEqualTo(expected.sha256());
            assertThat(actual.landedAt()).isEqualTo(expected.landedAt());
            assertThat(actual.kekRef()).isEqualTo(KEK);
            assertThat(actual.file()).isEqualTo(expected.file());
            assertThat(actual.fileSize()).isEqualTo(expected.fileSize());
            assertThat(actual.summaryRow())
                    .isEqualTo(expected.summaryRow());
        }
        assertThat(lander().lastLanded()).hasValueSatisfying(
                last -> assertThat(last.itemId()).isEqualTo(two.itemId()));
    }

    /**
     * An item whose source changed is left out of its group, and nothing
     * of it is committed; the rest are landed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void leavesOutAnItemWhoseSourceChanged() throws Exception
    {
        byte[] bytes = data(30);
        NewItem kept = fileItem(bytes, "c/kept");
        NewItem shrunk = fileItem(bytes, "c/shrunk");
        ItemLander.GroupLanded result = lander().landAll(List.of(
                new ItemLander.Grouped(shrunk,
                        opener(Arrays.copyOf(bytes, 20))),
                new ItemLander.Grouped(kept, opener(bytes))), BLOCK);

        assertThat(result.changed()).containsExactly(shrunk);
        assertThat(result.landed()).extracting(LandedItem::itemId)
                .containsExactly(kept.itemId());
        assertThat(chunkRows(shrunk.itemId())).isZero();
        assertThat(readBack(kept.itemId())).isEqualTo(bytes);
    }

    /**
     * A group tried again after its commit went through, as after an error
     * that followed it, isn't landed twice: its items are returned again
     * without being read, and an item that joined it since is landed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aCommittedGroupTriedAgainLandsOnlyTheRest() throws Exception
    {
        byte[] bytes = data(40);
        NewItem one = fileItem(bytes, "t/one");
        NewItem two = fileItem(bytes, "t/two");
        NewItem three = fileItem(bytes, "t/three");
        ItemLander crashing = lander();
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        assertThatThrownBy(() -> crashing.landAll(List.of(
                new ItemLander.Grouped(one, opener(bytes)),
                new ItemLander.Grouped(two, opener(bytes))), BLOCK))
                .isInstanceOf(Crash.class);

        ItemLander.ItemOpener unread = offset ->
        {
            throw new AssertionError("must not read the source");
        };
        ItemLander.GroupLanded again = lander().landAll(List.of(
                new ItemLander.Grouped(one, unread),
                new ItemLander.Grouped(two, unread),
                new ItemLander.Grouped(three, opener(bytes))), BLOCK);

        assertThat(again.landed()).extracting(LandedItem::itemId)
                .containsExactly(one.itemId(), two.itemId(),
                        three.itemId());
        assertThat(chunkRows(one.itemId())).isEqualTo(1);
        assertThat(readBack(three.itemId())).isEqualTo(bytes);
        assertThat(lander().lastLandedAll()).extracting(LandedItem::itemId)
                .containsExactly(three.itemId());
    }

    /**
     * An item landed alone after a group replaces it as the last commit.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void anItemLandedAloneEndsTheLastGroup() throws Exception
    {
        byte[] bytes = data(15);
        lander().landAll(List.of(
                new ItemLander.Grouped(fileItem(bytes, "a/1"), opener(bytes)),
                new ItemLander.Grouped(fileItem(bytes, "a/2"),
                        opener(bytes))), BLOCK);
        NewItem alone = fileItem(bytes, "a/alone");
        lander().land(alone, opener(bytes));

        assertThat(lander().lastLandedAll()).extracting(LandedItem::itemId)
                .containsExactly(alone.itemId());
        table.refresh();
        assertThat(table.properties())
                .doesNotContainKey(LandingBatch.propertyKey(INTAKE));
    }

    /**
     * An item larger than a group allows, a group with an item twice, or
     * an item whose ID isn't time-ordered, is refused before anything is
     * written.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesAGroupItCannotLand() throws Exception
    {
        byte[] bytes = data(BLOCK + 1);
        NewItem large = fileItem(bytes, "l/large");
        assertThatThrownBy(() -> lander().landAll(List.of(
                new ItemLander.Grouped(large, opener(bytes))), BLOCK))
                .isInstanceOf(IllegalArgumentException.class);
        NewItem twice = fileItem(data(5), "l/twice");
        assertThatThrownBy(() -> lander().landAll(List.of(
                new ItemLander.Grouped(twice, opener(data(5))),
                new ItemLander.Grouped(twice, opener(data(5)))), BLOCK))
                .isInstanceOf(IllegalArgumentException.class);
        NewItem unordered = new NewItem(new UUID(-1L, -1L), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, "l/unordered", 5, "1"), null, null);
        assertThatThrownBy(() -> lander().landAll(List.of(
                new ItemLander.Grouped(unordered, opener(data(5)))), BLOCK))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(chunkRows(large.itemId())).isZero();
        assertThat(chunkRows(twice.itemId())).isZero();
    }

    /**
     * A tampered record of the last group commit is refused.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aTamperedGroupIsRefused() throws Exception
    {
        byte[] bytes = data(10);
        lander().landAll(List.of(
                new ItemLander.Grouped(fileItem(bytes, "x/1"), opener(bytes)),
                new ItemLander.Grouped(fileItem(bytes, "x/2"),
                        opener(bytes))), BLOCK);
        String key = LandingBatch.propertyKey(INTAKE);
        String stored = table.properties().get(key);
        table.updateProperties()
                .set(key, stored.replace("\"rows\":4", "\"rows\":5"))
                .commit();
        assertThatThrownBy(() -> lander().lastLandedAll())
                .isInstanceOf(CheckpointTamperedException.class);
    }

    /**
     * A group recorded as announced is known to be so until a later commit;
     * a tampered record only means not known.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void knowsWhenTheLastGroupWasAnnounced() throws Exception
    {
        byte[] bytes = data(12);
        List<LandedItem> group = lander().landAll(List.of(
                new ItemLander.Grouped(fileItem(bytes, "m/1"), opener(bytes)),
                new ItemLander.Grouped(fileItem(bytes, "m/2"),
                        opener(bytes))), BLOCK).landed();
        assertThat(lander().lastAnnounced()).isFalse();
        lander().markAnnounced(group.getLast().itemId());
        assertThat(lander().lastAnnounced()).isTrue();

        String key = LandingBatch.announcedKey(INTAKE);
        String stored = table.properties().get(key);
        table.updateProperties().set(key, stored.replace(
                group.getLast().itemId().toString(),
                group.getFirst().itemId().toString())).commit();
        assertThat(lander().lastAnnounced()).isFalse();
        table.updateProperties().set(key, stored).commit();
        assertThat(lander().lastAnnounced()).isTrue();

        lander().land(fileItem(bytes, "m/alone"), opener(bytes));
        assertThat(lander().lastAnnounced()).isFalse();
    }

    /** Thrown by a test hook to simulate a crash. */
    static final class Crash extends RuntimeException
    {
    }
}
