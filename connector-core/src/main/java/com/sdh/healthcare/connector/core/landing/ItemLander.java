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
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.LongConsumer;
import javax.crypto.Mac;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

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
 * {@linkplain #clear cleared} before another starts.
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
        Optional<LandingCheckpoint> stored = checkpoint();
        if (stored.isEmpty() || !stored.get().done())
        {
            return Optional.empty();
        }
        return Optional.of(finished(stored.get()));
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
                    Snapshot snapshot = commit(batch.finish(), done);
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
                            settings.kekRef(), plainHash, item.replaces());
                }
                if (blockEnds && batch.bytes >= settings.commitBytes())
                {
                    long nextBlock = (chunkIndex + 1) / chunksPerBlock;
                    LandingCheckpoint progress = new LandingCheckpoint(
                            item.itemId(), item.kind(), item.source(),
                            startedAt, nextBlock,
                            Base64.getEncoder().encodeToString(chain), false,
                            -1, null, null);
                    commit(batch.finish(), progress);
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
     *
     * @param file the batch's data file
     * @param next the checkpoint after the batch
     * @return the snapshot the commit made; null if an earlier attempt had
     *         committed
     * @throws Exception if storage still fails after the retries
     */
    private Snapshot commit(DataFile file, LandingCheckpoint next)
            throws Exception
    {
        String encoded = codec.encode(next);
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
                        transaction.updateProperties()
                                .set(checkpointKey, encoded).commit();
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
            writer.write(row);
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
