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
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
    final SectionIds ids = new SectionIds(new byte[32]);

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

    /** An item without a source reference is recorded by its digest only. */
    @Test
    void recordsItemsWithoutASource() throws Exception
    {
        LandingLedger ledger =
                new LandingLedger(ledgerTable, committer(), RETRY);
        UUID itemId = UUID.randomUUID();
        ledger.record(List.of(new LandingLedger.Entry(INTAKE, null, itemId,
                "abcdefghijkl234567", Instant.now())));
        LedgerIndex index = ledger.load(INTAKE);
        assertThat(index.size()).isZero();
        assertThat(index.mayHaveContent("abcdefghijkl234567")).isTrue();
        assertThat(ledger.contains(itemId)).isTrue();
    }

    /**
     * A crash between an item's commit and its ledger entry is repaired at
     * startup, once.
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
        assertThat(SourceRef.of(new SectionIds(new byte[] {
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
        LedgerIndex index = new LedgerIndex();
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

    /** A digest prefix is 60 bits of the base32 digest. */
    @Test
    void digestPrefixesDecodeBase32()
    {
        assertThat(LedgerIndex.digestPrefix("aaaaaaaaaaaa")).isZero();
        assertThat(LedgerIndex.digestPrefix("aaaaaaaaaaab")).isEqualTo(1);
        assertThat(LedgerIndex.digestPrefix("777777777777xyz"))
                .isEqualTo((1L << 60) - 1);
    }
}
