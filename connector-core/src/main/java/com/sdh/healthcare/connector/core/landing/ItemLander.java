/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static com.sdh.healthcare.connector.core.landing.LandingTables.*;

import com.nimbusds.jose.util.JSONArrayUtils;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.table.DataFileRows;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongConsumer;
import javax.crypto.Mac;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/**
 * Lands items in {@value LandingTables#LANDED_ITEMS} (design §5.3.2):
 * stores each one exactly as received, encrypted in chunks, resumably.
 * <ul>
 *   <li>The item is read as a stream, so its size needn't be known in
 *       advance, as for an HTTP push. Each chunk (8 MiB) has the record
 *       envelope: one batch data key per commit, chunk key
 *       HKDF(BDK, itemId, seq 0), and AAD itemId ‖ 0 ‖ chunkIndex ‖ 0. The
 *       chunk count in the AAD is 0, because it isn't known when a chunk is
 *       written; the summary row's count and the digest catch a missing or
 *       extra chunk instead.</li>
 *   <li>The digest is the chained one of design §5.1, over fixed blocks
 *       (64 MiB), so a file's item digest equals the file digest the
 *       splitter reports for it. The chain can resume from its last
 *       link.</li>
 *   <li>Progress is committed every {@code commitBytes}, in whole blocks,
 *       with the landing checkpoint. The last commit adds the summary row
 *       and marks the checkpoint done.</li>
 * </ul>
 * One item is landed at a time: an unfinished item must be finished or
 * {@linkplain #clear cleared} before another starts. Small items can
 * instead be landed together, in one data file and one commit
 * ({@link #landAll}, docs/PERFORMANCE.md §3.1); the commit records them
 * all ({@link LandingBatch}), so recovery can find each.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemLander
{

    /** The label of the digest chain's MAC, as for archived files. */
    static final String DIGEST_LABEL = "sdh-file-digest";

    /** How many bytes each link of the digest chain covers: 64 MiB. */
    public static final int DEFAULT_BLOCK = 64 << 20;

    /** How many bytes a commit holds at least, by default: 512 MiB. */
    public static final long DEFAULT_COMMIT_BYTES = 512L << 20;

    /** How long the digest chain's links are: 32 bytes. */
    private static final int CHAIN_BYTES = 32;

    /** How long a batch data key is: 32 bytes. */
    private static final int KEY_BYTES = 32;

    /** Makes batch data keys. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * How an intake lands its items.
     *
     * @param intake      the intake's name
     * @param kekRef      the KEK that wraps each batch data key
     * @param commitBytes commit after at least this many bytes, rounded up
     *                    to whole blocks
     * @param retry       how storage commits are retried
     * @param chunkSize   the plaintext size of a chunk
     * @param blockSize   how many bytes each digest link covers; a whole
     *                    number of chunks
     */
    public record Settings(String intake, String kekRef, long commitBytes,
            RetryPolicy retry, int chunkSize, int blockSize)
    {

        /**
         * Creates settings, checking the sizes fit together.
         *
         * @param intake      the intake's name
         * @param kekRef      the KEK that wraps each batch data key
         * @param commitBytes commit after at least this many bytes
         * @param retry       how storage commits are retried
         * @param chunkSize   the plaintext size of a chunk
         * @param blockSize   how many bytes each digest link covers
         * @throws IllegalArgumentException if a size isn't positive, or the
         *                                  block isn't a whole number of
         *                                  chunks
         */
        public Settings
        {
            if (chunkSize < 1 || blockSize < chunkSize
                    || blockSize % chunkSize != 0 || commitBytes < 1)
            {
                throw new IllegalArgumentException(
                        "blockSize must be a positive multiple of chunkSize");
            }
        }

        /**
         * The usual settings: 8 MiB chunks, 64 MiB blocks, a commit every
         * 512 MiB.
         *
         * @param intake the intake's name
         * @param kekRef the KEK that wraps each batch data key
         * @return the settings
         */
        public static Settings defaults(String intake, String kekRef)
        {
            return new Settings(intake, kekRef, DEFAULT_COMMIT_BYTES,
                    RetryPolicy.DEFAULT, RecordCrypto.CHUNK_SIZE,
                    DEFAULT_BLOCK);
        }

        /**
         * The same settings with another retry policy.
         *
         * @param otherRetry how storage commits are retried
         * @return the new settings
         */
        public Settings withRetry(RetryPolicy otherRetry)
        {
            return new Settings(intake, kekRef, commitBytes, otherRetry,
                    chunkSize, blockSize);
        }

        /**
         * The same settings with smaller sizes, for tests.
         *
         * @param chunk  the plaintext size of a chunk
         * @param block  how many bytes each digest link covers
         * @param commit commit after at least this many bytes
         * @return the new settings
         */
        public Settings withSizes(int chunk, int block, long commit)
        {
            return new Settings(intake, kekRef, commit, retry, chunk, block);
        }
    }

    /**
     * Opens an item's bytes from a position, so an unfinished item can be
     * resumed. A source that can't start part-way through, such as an HTTP
     * push, throws for any position but 0.
     */
    @FunctionalInterface
    public interface ItemOpener
    {

        /**
         * Opens the item's bytes.
         *
         * @param offset where to start, in bytes
         * @return the bytes from there to the end
         * @throws IOException if the source can't be read
         */
        InputStream open(long offset) throws IOException;
    }

    /**
     * The source of an item changed while it was being landed, or since an
     * unfinished landing started: its size or version isn't what it was.
     * Nothing more of the item is committed; the intake decides whether to
     * land it again as a new item.
     */
    public static final class ItemChangedException extends IOException
    {

        /** Creates the exception, with a fixed message without PHI. */
        public ItemChangedException()
        {
            super("Item changed while it was being landed");
        }
    }

    /** The table items are landed in. */
    private final Table table;

    /** Makes the fenced commits. */
    private final FencedCommitter committer;

    /** Wraps the batch data keys. */
    private final KeyProvider keys;

    /** Makes the digests and the checkpoint MACs, with the ID key. */
    private final RecordIds ids;

    /** How the intake lands its items. */
    private final Settings settings;

    /** Gives the times recorded with items. */
    private final Clock clock;

    /** Writes and reads the checkpoint with its MAC. */
    private final MacedJson<LandingCheckpoint> codec;

    /** The table property the checkpoint is kept in. */
    private final String checkpointKey;

    /** Writes and reads the last group commit's items with their MAC. */
    private final MacedJson<LandingBatch> batchCodec;

    /** The table property the last group commit's items are kept in. */
    private final String batchKey;

    /** Runs after each commit; see {@link #afterCommit}. */
    private LongConsumer afterCommit = nextBlock ->
    {
    };

    /**
     * Creates a lander for one intake.
     *
     * @param table     the {@value LandingTables#LANDED_ITEMS} table
     * @param committer makes fenced commits under the intake's lease
     * @param keys      wraps the batch data keys
     * @param ids       makes the digests and MACs, with the ID key
     * @param settings  how the intake lands its items
     * @param clock     gives the times recorded with items
     */
    public ItemLander(Table table, FencedCommitter committer,
            KeyProvider keys, RecordIds ids, Settings settings, Clock clock)
    {
        this.table = table;
        this.committer = committer;
        this.keys = keys;
        this.ids = ids;
        this.settings = settings;
        this.clock = clock;
        this.codec = LandingCheckpoint.codec(ids);
        this.checkpointKey = LandingCheckpoint.propertyKey(settings.intake());
        this.batchCodec = LandingBatch.codec(ids);
        this.batchKey = LandingBatch.propertyKey(settings.intake());
    }

    /**
     * A small item to land with others, and how to open it.
     *
     * @param item   the item
     * @param opener opens its bytes, from the start
     */
    public record Grouped(NewItem item, ItemOpener opener)
    {
    }

    /**
     * What a group commit did.
     *
     * @param landed  the items landed, in order
     * @param changed the items left out because their source changed;
     *                nothing of them was committed
     */
    public record GroupLanded(List<LandedItem> landed, List<NewItem> changed)
    {
    }

    /**
     * A test hook that runs after each commit, with the next block's
     * number. Throwing from it simulates a crash.
     *
     * @param hook the hook
     */
    public void afterCommit(LongConsumer hook)
    {
        this.afterCommit = hook;
    }

    /**
     * The intake's checkpoint, read afresh from the table.
     *
     * @return the checkpoint; empty if the intake has landed nothing yet
     * @throws CheckpointTamperedException if its MAC doesn't verify
     */
    public Optional<LandingCheckpoint> checkpoint()
    {
        table.refresh();
        String stored = table.properties().get(checkpointKey);
        if (stored == null)
        {
            return Optional.empty();
        }
        try
        {
            return Optional.of(codec.decode(stored));
        }
        catch (MacedJson.Tampered tampered)
        {
            throw new CheckpointTamperedException();
        }
    }

    /**
     * The item the checkpoint says was landed last, once it is done: for
     * recovery, to make sure its ledger row and {@code item.landed} message
     * were sent. Needs no source.
     *
     * @return the item; empty if there is none, or it isn't finished
     * @throws IOException if its summary row can't be read
     */
    public Optional<LandedItem> lastLanded() throws IOException
    {
        List<LandedItem> last = lastLandedAll();
        return last.isEmpty() ? Optional.empty()
                : Optional.of(last.getLast());
    }

    /**
     * The items the last commit landed, once the checkpoint is done: every
     * item of a group commit, or the one item landed alone. For recovery,
     * to make sure each has its ledger row and {@code item.landed}
     * message. Needs no source.
     *
     * @return the items, in the order they were landed; empty if there are
     *         none, or the last item isn't finished
     * @throws CheckpointTamperedException if a MAC doesn't verify
     * @throws IOException                 if a summary row can't be read
     */
    public List<LandedItem> lastLandedAll() throws IOException
    {
        Optional<LandingCheckpoint> stored = checkpoint();
        if (stored.isEmpty() || !stored.get().done())
        {
            return List.of();
        }
        String encoded = table.properties().get(batchKey);
        if (encoded == null)
        {
            return List.of(finished(stored.get()));
        }
        LandingBatch batch;
        try
        {
            batch = batchCodec.decode(encoded);
        }
        catch (MacedJson.Tampered tampered)
        {
            throw new CheckpointTamperedException();
        }
        if (!batch.last().equals(stored.get().itemId()))
        {
            throw new IllegalStateException(
                    "The last group commit isn't the checkpoint's");
        }
        return batchItems(batch);
    }

    /**
     * The items of a group commit, from their summary rows, read from its
     * data file by position without the chunks' ciphertext.
     *
     * @param batch the group commit's items
     * @return the items, in its order
     * @throws IOException if the data file can't be read
     */
    private List<LandedItem> batchItems(LandingBatch batch)
            throws IOException
    {
        List<String> columns = table.schema().columns().stream()
                .map(Types.NestedField::name)
                .filter(name -> !name.equals(CIPHERTEXT)).toList();
        List<Record> rows = DataFileRows.read(table.io(), table.schema(),
                batch.file(), batch.fileSize(), 0, batch.rows(), columns);
        Map<UUID, Integer> summaries = new HashMap<>();
        for (int position = 0; position < rows.size(); position++)
        {
            Record row = rows.get(position);
            if ((Integer) row.getField(CHUNK_INDEX) == SUMMARY_ROW)
            {
                summaries.put((UUID) row.getField(ITEM_ID), position);
            }
        }
        long snapshotId = snapshotId(table.currentSnapshot());
        List<LandedItem> items = new ArrayList<>();
        for (UUID itemId : batch.itemIds())
        {
            Integer position = summaries.get(itemId);
            if (position == null)
            {
                throw new IllegalStateException(
                        "A grouped item has no summary");
            }
            LandedItem item = LandedItems.landedItem(rows.get(position),
                    snapshotId);
            items.add(new LandedItem(item.itemId(), item.intake(),
                    item.kind(), item.source(), item.size(), item.digest(),
                    item.landedAt(), item.snapshotId(), item.kekRef(),
                    item.sha256(), item.replaces(), batch.file(),
                    batch.fileSize(), position));
        }
        return items;
    }

    /**
     * Lands small items together: one data file, under one data key, and
     * one commit, which names them all ({@link LandingBatch}) and leaves
     * the checkpoint done at the last (docs/PERFORMANCE.md §3.1). Each item
     * is read whole into memory before any of it is written, so an item
     * whose source changed is left out, and the rest are landed. Landing a
     * group whose commit had gone through changes nothing and returns its
     * items again.
     *
     * @param items        the items, with their own IDs; none unfinished
     * @param maxItemBytes the most bytes an item may have
     * @return the items landed, and those left out
     * @throws IllegalArgumentException if an item is larger than
     *                                  {@code maxItemBytes}, two have the
     *                                  same ID, or an ID has its top bit
     *                                  set, as no time-ordered one has
     * @throws IllegalStateException    if an item is still unfinished
     * @throws Exception                if reading a source or storage
     *                                  fails; then nothing is committed
     */
    public GroupLanded landAll(List<Grouped> items, int maxItemBytes)
            throws Exception
    {
        Span span = Telemetry.get().span("land.group",
                Fields.of(Attr.INTAKE, settings.intake())
                        .with(Attr.COUNT, items.size()));
        try (Scope ignored = span.makeCurrent())
        {
            return landAllTraced(items, maxItemBytes);
        }
        catch (Exception | Error failure)
        {
            Telemetry.fail(span, failure);
            throw failure;
        }
        finally
        {
            span.end();
        }
    }

    /**
     * Lands small items together, inside their span.
     *
     * @param items        the items
     * @param maxItemBytes the most bytes an item may have
     * @return the items landed, and those left out
     * @throws Exception as for {@link #landAll}
     */
    private GroupLanded landAllTraced(List<Grouped> items, int maxItemBytes)
            throws Exception
    {
        Set<UUID> seen = new HashSet<>();
        for (Grouped grouped : items)
        {
            UUID itemId = grouped.item().itemId();
            if (!seen.add(itemId))
            {
                throw new IllegalArgumentException(
                        "Two items have the same ID");
            }
            // Parquet keeps a file's UUID bounds unsigned and Iceberg
            // compares them signed: a file holding IDs with the top bit both
            // set and clear is skipped by lookups by item ID. Time-ordered
            // IDs (UUIDv7) never set it.
            if (itemId.getMostSignificantBits() < 0)
            {
                throw new IllegalArgumentException(
                        "Grouped items need time-ordered IDs");
            }
        }
        Optional<LandingCheckpoint> stored = checkpoint();
        if (stored.isPresent() && !stored.get().done())
        {
            throw new IllegalStateException(
                    "Another item is still being landed");
        }
        if (stored.isPresent() && seen.contains(stored.get().itemId()))
        {
            return alreadyLanded(items, maxItemBytes);
        }
        List<NewItem> changed = new ArrayList<>();
        List<Written> written = new ArrayList<>();
        Batch batch = null;
        try
        {
            for (Grouped grouped : items)
            {
                byte[] bytes;
                try
                {
                    bytes = readWhole(grouped, maxItemBytes);
                }
                catch (ItemChangedException itemChanged)
                {
                    changed.add(grouped.item());
                    continue;
                }
                Instant startedAt = clock.instant();
                if (batch == null)
                {
                    batch = new Batch();
                }
                try
                {
                    written.add(write(batch, grouped.item(), bytes,
                            startedAt));
                }
                finally
                {
                    Arrays.fill(bytes, (byte) 0);
                }
            }
            if (batch == null)
            {
                return new GroupLanded(List.of(), changed);
            }
            Written last = written.getLast();
            Instant landedAt = batch.landedAt.toInstant();
            LandingCheckpoint done = new LandingCheckpoint(
                    last.item().itemId(), last.item().kind(),
                    last.item().source(), last.startedAt(), last.blocks(),
                    last.chain(), true, last.size(), last.digest(),
                    landedAt);
            int rows = (int) batch.rows;
            DataFile file = batch.finish();
            batch = null;
            LandingBatch landedBatch = new LandingBatch(written.stream()
                    .map(each -> each.item().itemId()).toList(),
                    file.location(), file.fileSizeInBytes(), rows);
            Snapshot snapshot = commit(file, done, landedBatch);
            afterCommit.accept(done.nextBlock());
            List<LandedItem> landed = new ArrayList<>();
            for (Written each : written)
            {
                NewItem item = each.item();
                Telemetry.get().audit("item.landed",
                        Fields.of(Attr.ITEM_ID, item.itemId())
                                .with(Attr.INTAKE, settings.intake())
                                .with(Attr.KIND, item.kind().wireName())
                                .with(Attr.BYTES, each.size()));
                landed.add(new LandedItem(item.itemId(), settings.intake(),
                        item.kind(), item.source(), each.size(),
                        each.digest(), landedAt, snapshotId(snapshot),
                        settings.kekRef(), each.sha256(), item.replaces(),
                        file.location(), file.fileSizeInBytes(),
                        each.summaryRow()));
            }
            return new GroupLanded(landed, changed);
        }
        finally
        {
            if (batch != null)
            {
                batch.abandon();
            }
        }
    }

    /**
     * Lands a group whose last commit had already gone through, as when a
     * group is tried again after an error that followed its commit: the
     * items that commit landed are returned again, and the rest, which it
     * left out or which joined the group since, are landed now.
     *
     * @param items        the group's items
     * @param maxItemBytes the most bytes an item may have
     * @return the items landed, and those left out
     * @throws Exception as for {@link #landAll}
     */
    private GroupLanded alreadyLanded(List<Grouped> items, int maxItemBytes)
            throws Exception
    {
        Set<UUID> wanted = new HashSet<>();
        items.forEach(grouped -> wanted.add(grouped.item().itemId()));
        List<LandedItem> landed = new ArrayList<>(lastLandedAll().stream()
                .filter(item -> wanted.contains(item.itemId())).toList());
        Set<UUID> landedIds = new HashSet<>();
        landed.forEach(item -> landedIds.add(item.itemId()));
        List<Grouped> rest = items.stream().filter(
                grouped -> !landedIds.contains(grouped.item().itemId()))
                .toList();
        if (rest.isEmpty())
        {
            return new GroupLanded(landed, List.of());
        }
        GroupLanded more = landAllTraced(rest, maxItemBytes);
        landed.addAll(more.landed());
        return new GroupLanded(landed, more.changed());
    }

    /**
     * An item of a group, written to the batch but not yet committed.
     *
     * @param item       the item
     * @param size       its size, in bytes
     * @param digest     its digest
     * @param sha256     its plain SHA-256, in hex
     * @param startedAt  when landing it started
     * @param blocks     how many digest blocks it has
     * @param chain      the digest chain after its last block, in base64
     * @param summaryRow its summary row's position in the data file
     */
    private record Written(NewItem item, long size, String digest,
            String sha256, Instant startedAt, long blocks, String chain,
            long summaryRow)
    {
    }

    /**
     * Reads a whole item of a group, checking its size against the one
     * its source gave.
     *
     * @param grouped      the item and its opener
     * @param maxItemBytes the most bytes it may have
     * @return its bytes
     * @throws ItemChangedException     if its size isn't the one its source
     *                                  gave, or the source says it changed
     * @throws IllegalArgumentException if it is larger than
     *                                  {@code maxItemBytes}
     * @throws IOException              if the source can't be read
     */
    private static byte[] readWhole(Grouped grouped, int maxItemBytes)
            throws IOException
    {
        SourceRef source = grouped.item().source();
        long expected = source == null ? -1 : source.size();
        if (expected > maxItemBytes || maxItemBytes == Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException(
                    "Item is too large to land in a group");
        }
        byte[] bytes;
        try (InputStream input = grouped.opener().open(0))
        {
            bytes = input.readNBytes(
                    (expected < 0 ? maxItemBytes : (int) expected) + 1);
        }
        Telemetry.get().bytesRead(bytes.length, "land");
        if (expected >= 0 && bytes.length != expected)
        {
            Arrays.fill(bytes, (byte) 0);
            throw new ItemChangedException();
        }
        if (bytes.length > maxItemBytes)
        {
            Arrays.fill(bytes, (byte) 0);
            throw new IllegalArgumentException(
                    "Item is too large to land in a group");
        }
        return bytes;
    }

    /**
     * Writes a whole item to a batch: its chunks, then its summary row.
     * The digest is the same chained one {@link #land} makes.
     *
     * @param batch     the batch
     * @param item      the item
     * @param bytes     its bytes
     * @param startedAt when landing it started
     * @return what was written
     */
    private Written write(Batch batch, NewItem item, byte[] bytes,
            Instant startedAt)
    {
        int chunkSize = settings.chunkSize();
        int blockSize = settings.blockSize();
        int chunkCount = Math.max(1, Math.ceilDiv(bytes.length, chunkSize));
        int blocks = Math.max(1, Math.ceilDiv(bytes.length, blockSize));
        byte[] chain = new byte[CHAIN_BYTES];
        for (int block = 0; block < blocks; block++)
        {
            int from = block * blockSize;
            Mac link = ids.newLabelledMac(DIGEST_LABEL);
            link.update(chain);
            link.update(bytes, from, Math.min(blockSize, bytes.length - from));
            chain = link.doFinal();
        }
        for (int chunk = 0; chunk < chunkCount; chunk++)
        {
            int from = chunk * chunkSize;
            byte[] plaintext = Arrays.copyOfRange(bytes, from,
                    Math.min(bytes.length, from + chunkSize));
            batch.chunk(item.itemId(), chunk, plaintext);
            Arrays.fill(plaintext, (byte) 0);
        }
        MessageDigest sha256 = newSha256();
        sha256.update(bytes);
        String digest = RecordIds.encode(chain);
        String plainHash = HexFormat.of().formatHex(sha256.digest());
        batch.summary(item, chunkCount, bytes.length, digest, plainHash,
                startedAt);
        return new Written(item, bytes.length, digest, plainHash, startedAt,
                blocks, Base64.getEncoder().encodeToString(chain),
                batch.summaryRow);
    }

    /**
     * Lands an item, or continues landing it from the checkpoint if it is
     * the unfinished item with the same ID. Landing an item that is already
     * done changes nothing and returns it again.
     *
     * @param item   the item
     * @param opener opens its bytes; from the checkpoint's position when
     *               resuming
     * @return the landed item
     * @throws ItemChangedException  if the source's size or version isn't
     *                               what it was, or the bytes read don't
     *                               match the size the source gave
     * @throws IllegalStateException if another item is still unfinished
     * @throws Exception             if reading the source or storage fails
     */
    public LandedItem land(NewItem item, ItemOpener opener) throws Exception
    {
        Span span = Telemetry.get().span("land.item",
                Fields.of(Attr.ITEM_ID, item.itemId())
                        .with(Attr.INTAKE, settings.intake())
                        .with(Attr.KIND, item.kind().wireName()));
        try (Scope ignored = span.makeCurrent())
        {
            return landTraced(item, opener);
        }
        catch (Exception | Error failure)
        {
            Telemetry.fail(span, failure);
            throw failure;
        }
        finally
        {
            span.end();
        }
    }

    /**
     * Removes the checkpoint of an unfinished item, so the intake can land
     * others. In the same commit the item is recorded as given up
     * ({@link GivenUpItems}): its committed chunks stay, without a summary
     * row, until maintenance removes them after the grace period.
     *
     * @param itemId the unfinished item
     * @throws IllegalStateException if that item isn't the unfinished one
     * @throws Exception             if storage fails
     */
    public void clear(UUID itemId) throws Exception
    {
        Optional<LandingCheckpoint> stored = checkpoint();
        if (stored.isEmpty() || stored.get().done()
                || !stored.get().itemId().equals(itemId))
        {
            throw new IllegalStateException("Not the unfinished item");
        }
        settings.retry().call("landing.clear",
                RetryPolicy.TRANSIENT_STORAGE, attempt -> committer.commit(
                        table, transaction ->
                        {
                            transaction.updateProperties()
                                    .remove(checkpointKey).commit();
                            GivenUpItems.record(transaction, ids,
                                    settings.intake(), new GivenUpItems.Entry(
                                            itemId, clock.instant()));
                        }));
        Telemetry.get().audit("item.cleared",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.INTAKE, settings.intake()));
    }

    /**
     * Lands an item, inside its span.
     *
     * @param item   the item
     * @param opener opens its bytes
     * @return the landed item
     * @throws Exception as for {@link #land}
     */
    private LandedItem landTraced(NewItem item, ItemOpener opener)
            throws Exception
    {
        Optional<LandingCheckpoint> stored = checkpoint();
        LandingCheckpoint resumeFrom = null;
        if (stored.isPresent())
        {
            LandingCheckpoint checkpoint = stored.get();
            if (checkpoint.itemId().equals(item.itemId()))
            {
                if (checkpoint.done())
                {
                    return finished(checkpoint);
                }
                if (!Objects.equals(checkpoint.source(), item.source()))
                {
                    throw new ItemChangedException();
                }
                resumeFrom = checkpoint;
            }
            else if (!checkpoint.done())
            {
                throw new IllegalStateException(
                        "Another item is still being landed");
            }
        }
        int chunkSize = settings.chunkSize();
        int chunksPerBlock = settings.blockSize() / chunkSize;
        long block = resumeFrom == null ? 0 : resumeFrom.nextBlock();
        byte[] chain = resumeFrom == null ? new byte[CHAIN_BYTES]
                : Base64.getDecoder().decode(resumeFrom.chainHash());
        Instant startedAt = resumeFrom == null ? clock.instant()
                : resumeFrom.startedAt();
        long offset = block * settings.blockSize();
        if (resumeFrom != null)
        {
            Telemetry.get().audit("item.resumed",
                    Fields.of(Attr.ITEM_ID, item.itemId())
                            .with(Attr.INTAKE, settings.intake())
                            .with(Attr.OFFSET, offset));
        }
        long expectedSize = item.source() == null ? -1 : item.source().size();
        // The plain SHA-256 can't be carried across a restart, so a resumed
        // item has none.
        MessageDigest sha256 = resumeFrom == null ? newSha256() : null;

        Batch batch = null;
        try (InputStream input = opener.open(offset))
        {
            long chunkIndex = block * chunksPerBlock;
            long bytesRead = offset;
            Mac link = null;
            byte[] current = input.readNBytes(chunkSize);
            while (true)
            {
                // Reading one chunk ahead tells whether this one is last.
                byte[] next = current.length < chunkSize ? new byte[0]
                        : input.readNBytes(chunkSize);
                boolean last = next.length == 0;
                bytesRead += current.length;
                Telemetry.get().bytesRead(current.length, "land");
                if (expectedSize >= 0 && (bytesRead > expectedSize
                        || last && bytesRead != expectedSize))
                {
                    throw new ItemChangedException();
                }
                if (chunkIndex >= Integer.MAX_VALUE)
                {
                    throw new IllegalArgumentException(
                            "Item has too many chunks");
                }
                if (batch == null)
                {
                    batch = new Batch();
                }
                if (chunkIndex % chunksPerBlock == 0)
                {
                    link = ids.newLabelledMac(DIGEST_LABEL);
                    link.update(chain);
                }
                link.update(current);
                if (sha256 != null)
                {
                    sha256.update(current);
                }
                batch.chunk(item.itemId(), (int) chunkIndex, current);
                Arrays.fill(current, (byte) 0);
                boolean blockEnds = last
                        || (chunkIndex + 1) % chunksPerBlock == 0;
                if (blockEnds)
                {
                    chain = link.doFinal();
                }
                if (last)
                {
                    String digest = RecordIds.encode(chain);
                    int chunkCount = (int) (chunkIndex + 1);
                    String plainHash = sha256 == null ? null
                            : HexFormat.of().formatHex(sha256.digest());
                    batch.summary(item, chunkCount, bytesRead, digest,
                            plainHash, startedAt);
                    Instant landedAt = batch.landedAt.toInstant();
                    LandingCheckpoint done = new LandingCheckpoint(
                            item.itemId(), item.kind(), item.source(),
                            startedAt, (chunkIndex / chunksPerBlock) + 1,
                            Base64.getEncoder().encodeToString(chain), true,
                            bytesRead, digest, landedAt);
                    long summaryRow = batch.summaryRow;
                    DataFile file = batch.finish();
                    Snapshot snapshot = commit(file, done, null);
                    batch = null;
                    afterCommit.accept(done.nextBlock());
                    Telemetry.get().audit("item.landed",
                            Fields.of(Attr.ITEM_ID, item.itemId())
                                    .with(Attr.INTAKE, settings.intake())
                                    .with(Attr.KIND, item.kind().wireName())
                                    .with(Attr.BYTES, bytesRead));
                    return new LandedItem(item.itemId(), settings.intake(),
                            item.kind(), item.source(), bytesRead, digest,
                            landedAt, snapshotId(snapshot),
                            settings.kekRef(), plainHash, item.replaces(),
                            file.location(), file.fileSizeInBytes(),
                            summaryRow);
                }
                if (blockEnds && batch.bytes >= settings.commitBytes())
                {
                    long nextBlock = (chunkIndex + 1) / chunksPerBlock;
                    LandingCheckpoint progress = new LandingCheckpoint(
                            item.itemId(), item.kind(), item.source(),
                            startedAt, nextBlock,
                            Base64.getEncoder().encodeToString(chain), false,
                            -1, null, null);
                    commit(batch.finish(), progress, null);
                    batch = null;
                    afterCommit.accept(nextBlock);
                }
                chunkIndex++;
                current = next;
            }
        }
        finally
        {
            if (batch != null)
            {
                batch.abandon();
            }
        }
    }

    /**
     * The landed item a done checkpoint describes, with the KEK from its
     * summary row and the table's current snapshot, which holds it.
     *
     * @param checkpoint the done checkpoint
     * @return the item
     * @throws IOException if its summary row can't be read
     */
    private LandedItem finished(LandingCheckpoint checkpoint)
            throws IOException
    {
        Record summary = LandedItems.summary(table, checkpoint.itemId())
                .orElseThrow(() -> new IllegalStateException(
                        "Checkpoint is done but the item has no summary"));
        return new LandedItem(checkpoint.itemId(), settings.intake(),
                checkpoint.kind(), checkpoint.source(), checkpoint.size(),
                checkpoint.digest(), checkpoint.landedAt(),
                snapshotId(table.currentSnapshot()),
                (String) summary.getField(KEK_REF),
                (String) summary.getField(ITEM_SHA256),
                (UUID) summary.getField(REPLACES_ITEM_ID));
    }

    /**
     * The item a fully landed item replaces, from its summary row.
     *
     * @param itemId the item
     * @return the item it replaces; empty if none, or if it isn't fully
     *         landed
     * @throws IOException if the table can't be read
     */
    public Optional<UUID> replacesOf(UUID itemId) throws IOException
    {
        return LandedItems.summary(table, itemId)
                .flatMap(LandedItems::replaces);
    }

    /**
     * A new SHA-256 digest.
     *
     * @return the digest
     */
    private static MessageDigest newSha256()
    {
        try
        {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException unavailable)
        {
            throw new IllegalStateException(unavailable);
        }
    }

    /**
     * A snapshot's ID.
     *
     * @param snapshot the snapshot; null if the table has none
     * @return its ID; -1 for none
     */
    private static long snapshotId(Snapshot snapshot)
    {
        return snapshot == null ? -1 : snapshot.snapshotId();
    }

    /**
     * Commits a batch's data file with the next checkpoint, retrying on
     * transient storage errors. A retry first checks whether an earlier
     * attempt had in fact committed, so a batch is never appended twice.
     * The commit records a group's items, or, for an item landed alone,
     * removes the last group's.
     *
     * @param file  the batch's data file
     * @param next  the checkpoint after the batch
     * @param group the items of a group commit; null for one item alone
     * @return the snapshot the commit made; the current one if an earlier
     *         attempt had committed
     * @throws Exception if storage still fails after the retries
     */
    private Snapshot commit(DataFile file, LandingCheckpoint next,
            LandingBatch group) throws Exception
    {
        String encoded = codec.encode(next);
        String encodedGroup = group == null ? null : batchCodec.encode(group);
        long started = System.nanoTime();
        Snapshot snapshot = settings.retry().call("landing.commit",
                RetryPolicy.TRANSIENT_STORAGE, attempt ->
                {
                    if (attempt > 1 && alreadyCommitted(next))
                    {
                        return table.currentSnapshot();
                    }
                    return committer.commit(table, transaction ->
                    {
                        transaction.newFastAppend().appendFile(file).commit();
                        UpdateProperties properties =
                                transaction.updateProperties()
                                        .set(checkpointKey, encoded);
                        if (encodedGroup != null)
                        {
                            properties.set(batchKey, encodedGroup);
                        }
                        else
                        {
                            properties.remove(batchKey);
                        }
                        properties.commit();
                    });
                });
        Telemetry.get().commitDuration(System.nanoTime() - started,
                table.name());
        return snapshot;
    }

    /**
     * Tells whether the stored checkpoint is already at or past one, so a
     * commit that seemed to fail had in fact gone through.
     *
     * @param next the checkpoint being committed
     * @return true if it, or a later one for the same item, is stored
     */
    private boolean alreadyCommitted(LandingCheckpoint next)
    {
        Optional<LandingCheckpoint> current = checkpoint();
        return current.isPresent()
                && current.get().itemId().equals(next.itemId())
                && (current.get().done()
                        || !next.done()
                                && current.get().nextBlock()
                                        >= next.nextBlock());
    }

    /** One Parquet data file for one commit, with its own data key. */
    private final class Batch
    {

        /** The batch's ID, recorded on every row. */
        private final UUID batchId = UUID.randomUUID();

        /** The batch data key; zeroed when the batch is finished. */
        private final byte[] dataKey = new byte[KEY_BYTES];

        /** The batch data key, wrapped by the KEK. */
        private final WrappedKey wrappedKey;

        /**
         * When the batch was started; every row's partition time. To the
         * microsecond, as the table keeps it, so that a message rebuilt from
         * the summary row (a reprocess, STAGES.md §15.8) is the one first
         * sent.
         */
        private final OffsetDateTime landedAt = OffsetDateTime.ofInstant(
                clock.instant().truncatedTo(ChronoUnit.MICROS),
                ZoneOffset.UTC);

        /** Writes the batch's rows. */
        private final DataWriter<Record> writer;

        /** How many plaintext bytes the batch holds. */
        private long bytes;

        /** How many rows have been written to the batch's file. */
        private long rows;

        /** The summary row's position in the file; -1 until written. */
        private long summaryRow = -1;

        /** Starts a batch: makes its key and opens its data file. */
        Batch()
        {
            RANDOM.nextBytes(dataKey);
            wrappedKey = keys.wrap(settings.kekRef(), dataKey);
            GenericRecord probe = GenericRecord.create(table.schema());
            probe.setField(LANDED_AT, landedAt);
            PartitionKey partition =
                    new PartitionKey(table.spec(), table.schema());
            partition.partition(new InternalRecordWrapper(
                    table.schema().asStruct()).wrap(probe));
            String location = table.locationProvider().newDataLocation(
                    table.spec(), partition,
                    FileFormat.PARQUET.addExtension(batchId.toString()));
            try
            {
                writer = Parquet.writeData(table.io().newOutputFile(location))
                        .schema(table.schema())
                        .withSpec(table.spec())
                        .withPartition(partition)
                        .withSortOrder(table.sortOrder())
                        .setAll(table.properties())
                        .setAll(IngestTables.WRITE_OVERRIDES)
                        .createWriterFunc(GenericParquetWriter::create)
                        .overwrite()
                        .build();
            }
            catch (IOException unwritable)
            {
                throw new UncheckedIOException(unwritable);
            }
        }

        /**
         * A row with the columns every row has.
         *
         * @param itemId     the item's ID
         * @param chunkIndex the chunk's position; -1 for the summary
         * @param ciphertext the encrypted chunk; empty for the summary
         * @return the row
         */
        private GenericRecord row(UUID itemId, int chunkIndex,
                byte[] ciphertext)
        {
            GenericRecord row = GenericRecord.create(table.schema());
            row.setField(ITEM_ID, itemId);
            row.setField(CHUNK_INDEX, chunkIndex);
            row.setField(CIPHERTEXT, ByteBuffer.wrap(ciphertext));
            row.setField(BDK_WRAPPED, ByteBuffer.wrap(wrappedKey.wrapped()));
            row.setField(KEK_REF, wrappedKey.keyRef());
            row.setField(BATCH_ID, batchId);
            row.setField(LANDED_AT, landedAt);
            return row;
        }

        /**
         * Encrypts and writes a chunk.
         *
         * @param itemId     the item's ID
         * @param chunkIndex the chunk's position in the item
         * @param plaintext  the chunk
         */
        void chunk(UUID itemId, int chunkIndex, byte[] plaintext)
        {
            byte[] ciphertext = RecordCrypto.encryptChunk(
                    RecordCrypto.recordKey(dataKey, itemId, 0), itemId, 0,
                    chunkIndex, 0, plaintext);
            writer.write(row(itemId, chunkIndex, ciphertext));
            rows++;
            bytes += plaintext.length;
        }

        /**
         * Writes the item's summary row.
         *
         * @param item       the item
         * @param chunkCount how many chunks it is stored in
         * @param size       its size, in bytes
         * @param digest     its digest
         * @param sha256     its plain SHA-256, in hex; null if it was resumed
         * @param startedAt  when landing it started
         */
        void summary(NewItem item, int chunkCount, long size, String digest,
                String sha256, Instant startedAt)
        {
            GenericRecord row = row(item.itemId(), SUMMARY_ROW, new byte[0]);
            row.setField(INTAKE, settings.intake());
            row.setField(KIND, item.kind().wireName());
            row.setField(ITEM_SIZE, size);
            row.setField(CHUNK_COUNT, chunkCount);
            row.setField(CHUNK_SIZE, settings.chunkSize());
            row.setField(ITEM_DIGEST, digest);
            row.setField(ITEM_SHA256, sha256);
            row.setField(DIGEST_BLOCK_SIZE, settings.blockSize());
            row.setField(STARTED_AT,
                    OffsetDateTime.ofInstant(startedAt, ZoneOffset.UTC));
            if (item.source() != null)
            {
                row.setField(SOURCE_REF_HASH, item.source().hash());
                row.setField(SOURCE_SIZE, item.source().size());
                row.setField(SOURCE_VERSION, item.source().version());
            }
            if (item.originalName() != null)
            {
                row.setField(ORIGINAL_NAME_ENC, ByteBuffer.wrap(
                        RecordCrypto.encryptChunk(RecordCrypto.recordKey(
                                dataKey, item.itemId(), -1), item.itemId(),
                                -1, 0, 1, item.originalName().getBytes(
                                        StandardCharsets.UTF_8))));
            }
            row.setField(REPLACES_ITEM_ID, item.replaces());
            if (item.messageIndex() != null)
            {
                MessageIndex messageIndex = item.messageIndex();
                if (messageIndex.offsets().get(messageIndex.count() - 1) > size)
                {
                    throw new IllegalArgumentException(
                            "A message starts after the end of the item");
                }
                row.setField(MESSAGE_OFFSETS, messageIndex.offsets());
                row.setField(MESSAGE_KEYS, messageIndex.keys());
            }
            if (item.messageRefs() != null)
            {
                row.setField(MESSAGE_REFS_ENC, ByteBuffer.wrap(
                        RecordCrypto.encryptChunk(RecordCrypto.recordKey(
                                dataKey, item.itemId(), MESSAGE_REFS_SEQ),
                                item.itemId(), MESSAGE_REFS_SEQ, 0, 1,
                                JSONArrayUtils.toJSONString(
                                        item.messageRefs()).getBytes(
                                                StandardCharsets.UTF_8))));
            }
            if (item.messageReplaces() != null)
            {
                row.setField(MESSAGE_REPLACES_ENC, ByteBuffer.wrap(
                        RecordCrypto.encryptChunk(RecordCrypto.recordKey(
                                dataKey, item.itemId(), MESSAGE_REPLACES_SEQ),
                                item.itemId(), MESSAGE_REPLACES_SEQ, 0, 1,
                                JSONArrayUtils.toJSONString(
                                        item.messageReplaces()).getBytes(
                                                StandardCharsets.UTF_8))));
            }
            summaryRow = rows;
            writer.write(row);
            rows++;
        }

        /**
         * Closes the data file and forgets the data key.
         *
         * @return the data file, ready to commit
         */
        DataFile finish()
        {
            try
            {
                writer.close();
            }
            catch (IOException unwritable)
            {
                throw new UncheckedIOException(unwritable);
            }
            finally
            {
                Arrays.fill(dataKey, (byte) 0);
            }
            return writer.toDataFile();
        }

        /**
         * Forgets the data key and closes the data file without committing
         * it, on the failure path only. It never throws, so the error that
         * stopped the landing is the one reported; the file is left for
         * orphan cleanup.
         */
        void abandon()
        {
            Arrays.fill(dataKey, (byte) 0);
            try
            {
                writer.close();
            }
            catch (IOException | RuntimeException ignored)
            {
                // An orphan file; orphan cleanup removes it.
            }
        }
    }
}
