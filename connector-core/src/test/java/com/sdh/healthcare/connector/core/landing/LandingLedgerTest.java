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
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LandingLedger}, {@link LedgerIndex} and
 * {@link SourceRef}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LandingLedgerTest
{

    /** The intake under test. */
    static final String INTAKE = "claims-sftp";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** A quick retry policy. */
    static final RetryPolicy RETRY =
            new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1));

    /** The landed items table. */
    Table items;

    /** The ledger table. */
    Table ledgerTable;

    /** Wraps and unwraps data keys. */
    AesKeyWrapProvider keys;

    /** Makes the hashes and MACs, with an all-zero ID key. */
    final RecordIds ids = new RecordIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /**
     * Creates the catalog, both tables and the KEK.
     *
     * @throws Exception if the KEK can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        items = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        ledgerTable = LandingTables.ledger(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
    }

    /**
     * A committer under a new lease epoch, established on both tables.
     *
     * @return the committer
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
        committer.establish(items);
        committer.establish(ledgerTable);
        return committer;
    }

    /**
     * Lands bytes as a file item, without writing its ledger entry.
     *
     * @param committer makes the commits
     * @param path      the file's path
     * @param bytes     its content
     * @return the landed item
     * @throws Exception if landing fails
     */
    LandedItem land(FencedCommitter committer, String path, byte[] bytes)
            throws Exception
    {
        ItemLander lander = new ItemLander(items, committer, keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK).withRetry(RETRY),
                Clock.systemUTC());
        return lander.land(new NewItem(UUID.randomUUID(), ItemKind.FILE,
                SourceRef.of(ids, INTAKE, path, bytes.length, "1"), path,
                null), offset -> new ByteArrayInputStream(bytes));
    }

    /**
     * Entries load back into an index that knows each sighting, and finds
     * items with the same content.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recordsLoadsAndFindsDuplicateContent() throws Exception
    {
        FencedCommitter committer = committer();
        LandingLedger ledger = new LandingLedger(ledgerTable, committer, RETRY);
        LandedItem first = land(committer, "a/one.csv", new byte[] {1, 2, 3});
        LandedItem copy = land(committer, "b/copy.csv", new byte[] {1, 2, 3});
        LandedItem other = land(committer, "a/two.csv", new byte[] {4});
        ledger.record(List.of(LandingLedger.Entry.of(first),
                LandingLedger.Entry.of(copy)));
        ledger.record(List.of(LandingLedger.Entry.of(other)));

        LedgerIndex index = ledger.load(INTAKE);
        assertThat(index.size()).isEqualTo(3);
        assertThat(index.contains(first.source().key())).isTrue();
        assertThat(index.contains(SourceRef.of(ids, INTAKE, "a/one.csv", 3,
                "2").key())).as("a new version is a new item").isFalse();
        assertThat(index.contains(SourceRef.of(ids, INTAKE, "a/one.csv", 4,
                "1").key())).as("a new size is a new item").isFalse();
        assertThat(index.mayHaveContent(first.digest())).isTrue();
        assertThat(first.digest()).isEqualTo(copy.digest());
        assertThat(ledger.itemsWithDigest(first.digest()))
                .containsExactlyInAnyOrder(first.itemId(), copy.itemId());
        assertThat(ledger.load("another-intake").size()).isZero();
    }

    /**
     * An item without a source reference is recorded by its digest only.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recordsItemsWithoutASource() throws Exception
    {
        LandingLedger ledger =
                new LandingLedger(ledgerTable, committer(), RETRY);
        UUID itemId = UUID.randomUUID();
        ledger.record(List.of(new LandingLedger.Entry(INTAKE, null, itemId,
                "abcdefghijkl234567", Instant.now(), 1, null)));
        LedgerIndex index = ledger.load(INTAKE);
        assertThat(index.size()).isZero();
        assertThat(index.mayHaveContent("abcdefghijkl234567")).isTrue();
        assertThat(ledger.contains(itemId)).isTrue();
    }

    /**
     * A crash between an item's commit and its ledger entry is repaired at
     * startup, once.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void repairsTheEntryOfTheLastLandedItem() throws Exception
    {
        FencedCommitter committer = committer();
        LandedItem item = land(committer, "crash/before-ledger.xml",
                new byte[] {9});

        FencedCommitter restarted = committer();
        LandingLedger ledger =
                new LandingLedger(ledgerTable, restarted, RETRY);
        ItemLander lander = new ItemLander(items, restarted, keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK).withRetry(RETRY),
                Clock.systemUTC());
        LedgerIndex index = ledger.load(INTAKE);
        assertThat(index.contains(item.source().key())).isFalse();

        assertThat(ledger.repair(lander, index)).isTrue();
        assertThat(index.contains(item.source().key())).isTrue();
        assertThat(ledger.load(INTAKE).contains(item.source().key())).isTrue();
        assertThat(ledger.repair(lander, index)).isFalse();
    }

    /**
     * An entry is found by its source reference, with the item's size and
     * SHA-256, as an HTTP push's receipt needs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsAnEntryByItsSourceReference() throws Exception
    {
        LandingLedger ledger =
                new LandingLedger(ledgerTable, committer(), RETRY);
        SourceRef ref = SourceRef.of(ids, INTAKE, "push key", -1, "");
        UUID itemId = UUID.randomUUID();
        Instant landedAt = Instant.parse("2026-11-04T10:15:30Z");
        ledger.record(List.of(new LandingLedger.Entry(INTAKE, ref, itemId,
                "abcdefghijkl234567", landedAt, 42, "00ff")));

        assertThat(ledger.find(INTAKE, ref)).hasValueSatisfying(entry ->
        {
            assertThat(entry.itemId()).isEqualTo(itemId);
            assertThat(entry.size()).isEqualTo(42);
            assertThat(entry.sha256()).isEqualTo("00ff");
            assertThat(entry.landedAt()).isEqualTo(landedAt);
        });
        assertThat(ledger.find(INTAKE,
                SourceRef.of(ids, INTAKE, "other key", -1, ""))).isEmpty();
        assertThat(ledger.find("other", ref)).isEmpty();
    }

    /** The reference hash depends on the intake and the reference only. */
    @Test
    void sourceReferencesAreKeyedAndCarryNoPath()
    {
        SourceRef ref = SourceRef.of(ids, INTAKE, "jones/mrn-42.xml", 10, "1");
        assertThat(ref.hash()).doesNotContain("jones").hasSize(52);
        assertThat(SourceRef.of(ids, INTAKE, "jones/mrn-42.xml", 99, "7")
                .hash()).isEqualTo(ref.hash());
        assertThat(SourceRef.of(ids, "other", "jones/mrn-42.xml", 10, "1")
                .hash()).isNotEqualTo(ref.hash());
        assertThat(SourceRef.of(new RecordIds(new byte[] {
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                INTAKE, "jones/mrn-42.xml", 10, "1").hash())
                .as("another ID key gives another hash")
                .isNotEqualTo(ref.hash());
    }

    /** The index holds many keys, the zero key, and repeats only once. */
    @Test
    void theIndexGrowsAndKeepsEveryKey()
    {
        LedgerIndex index = new LedgerIndex(0, Long.MAX_VALUE, INTAKE);
        int count = 100_000;
        for (long value = 0; value < count; value++)
        {
            index.add(new LedgerIndex.Key(value, value * 31), "aaaaaaaaaaaa");
        }
        index.add(new LedgerIndex.Key(5, 155), "aaaaaaaaaaaa");
        assertThat(index.size()).isEqualTo(count);
        for (long value = 0; value < count; value++)
        {
            assertThat(index.contains(new LedgerIndex.Key(value, value * 31)))
                    .isTrue();
        }
        assertThat(index.contains(new LedgerIndex.Key(1, 1))).isFalse();
        assertThat(index.contains(new LedgerIndex.Key(0, 0))).isTrue();
    }

    /**
     * The index is sized for the ledger as it loads, from the table's
     * metadata, so loading doesn't grow it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sizesTheIndexForTheLedger() throws Exception
    {
        LandingLedger ledger =
                new LandingLedger(ledgerTable, committer(), RETRY);
        ledger.record(entries(2_000));
        ledger.record(entries(1_000));
        assertThat(ledger.count(INTAKE)).isEqualTo(3_000);
        assertThat(ledger.count("another-intake")).isZero();

        LedgerIndex index = ledger.load(INTAKE);
        assertThat(index.size()).isEqualTo(3_000);
        assertThat(index.bytes()).isEqualTo(LedgerIndex.bytesFor(3_000))
                .as("8,192 slots of 24 bytes").isEqualTo(8_192 * 24);
    }

    /**
     * When its next growth would take the index past its share of the
     * heap, it warns, once.
     */
    @Test
    void warnsOnceBeforeTheIndexOutgrowsItsShare()
    {
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            long smallest = LedgerIndex.bytesFor(0);
            LedgerIndex index = new LedgerIndex(0, 3 * smallest, INTAKE);
            index.checkBudget();
            assertThat(lowMemoryWarnings(events)).isEmpty();
            for (long value = 1; value <= 2_000; value++)
            {
                index.add(new LedgerIndex.Key(value, value),
                        RecordIds.encode(ByteBuffer.allocate(Long.BYTES)
                                .putLong(value << 4).array()));
            }
            assertThat(index.bytes()).isEqualTo(4 * smallest);
            assertThat(lowMemoryWarnings(events)).singleElement()
                    .satisfies(warning ->
                    {
                        assertThat(warning.getAttributes().get(
                                AttributeKey.longKey(Attr.COUNT.key())))
                                .isEqualTo(683);
                        assertThat(warning.getAttributes().get(
                                AttributeKey.longKey(Attr.BYTES.key())))
                                .isEqualTo(4 * smallest);
                    });
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
    }

    /**
     * The {@code ledger.memory_low} events recorded.
     *
     * @param events the exporter
     * @return the events
     */
    static List<LogRecordData> lowMemoryWarnings(
            InMemoryLogRecordExporter events)
    {
        return events.getFinishedLogRecordItems().stream()
                .filter(record -> "sdh.audit.ledger.memory_low"
                        .equals(record.getEventName()))
                .toList();
    }

    /**
     * Ledger entries with random references and digests.
     *
     * @param count how many
     * @return the entries
     */
    List<LandingLedger.Entry> entries(int count)
    {
        List<LandingLedger.Entry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++)
        {
            String path = UUID.randomUUID().toString();
            entries.add(new LandingLedger.Entry(INTAKE,
                    SourceRef.of(ids, INTAKE, path, 1, "1"),
                    UUID.randomUUID(), RecordIds.encode(
                            ids.newLabelledMac("test").doFinal(
                                    path.getBytes())),
                    Instant.now(), 1, null));
        }
        return entries;
    }

    /** A digest prefix is 60 bits of the base32 digest. */
    @Test
    void digestPrefixesDecodeBase32()
    {
        assertThat(LedgerIndex.digestPrefix("aaaaaaaaaaaa")).isZero();
        assertThat(LedgerIndex.digestPrefix("aaaaaaaaaaab")).isEqualTo(1);
        assertThat(LedgerIndex.digestPrefix("777777777777xyz"))
                .isEqualTo((1L << 60) - 1);
    }

    /**
     * The item a copy is the same as is the earliest earlier one, by
     * UUIDv7 order compared unsigned, leaving out the item it replaces and
     * any landed after it.
     */
    @Test
    void choosesTheEarliestEarlierItemAsTheOriginal()
    {
        UUID low = new UUID(0x0000_0000_0001_7000L, 1);
        UUID mid = new UUID(0x0000_0000_0002_7000L, 1);
        UUID copy = new UUID(0x0000_0000_0003_7000L, 1);
        UUID later = new UUID(0x0000_0000_0004_7000L, 1);
        UUID highBit = new UUID(0x8000_0000_0000_7000L, 1);

        assertThat(LandingLedger.earliest(List.of(later, mid, low, copy),
                copy, null)).contains(low);
        assertThat(LandingLedger.earliest(List.of(mid, low), copy, low))
                .contains(mid);
        assertThat(LandingLedger.earliest(List.of(low), copy, low)).isEmpty();
        assertThat(LandingLedger.earliest(List.of(later, copy), copy, null))
                .isEmpty();
        // Unsigned: a top bit set is later, not earlier.
        assertThat(LandingLedger.earliest(List.of(highBit, copy), later,
                null)).contains(copy);
    }
}
