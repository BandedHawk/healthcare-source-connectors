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
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LandedItemSource}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LandedItemSourceTest
{

    /** The KEK. */
    static final String KEK = "landed-items-kek/1";

    /** The landed items table. */
    Table table;

    /** Wraps and unwraps the data keys. */
    AesKeyWrapProvider keys;

    /**
     * Creates the table and key.
     *
     * @throws Exception if the key can't be made
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
    }

    /**
     * Lands bytes in 16-byte chunks, so a few bytes span many windows.
     *
     * @param bytes the content
     * @return the landed item
     * @throws Exception if landing fails
     */
    LandedItem land(byte[] bytes) throws Exception
    {
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
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
        return new ItemLander(table, committer, keys,
                new RecordIds(new byte[32]),
                ItemLander.Settings.defaults("claims", KEK)
                        .withSizes(16, 64, 64), Clock.systemUTC())
                .land(new NewItem(UUID.randomUUID(), ItemKind.FILE, null,
                        null, null), offset -> new ByteArrayInputStream(bytes,
                                (int) offset, bytes.length));
    }

    /**
     * Reads at any position, across chunks and windows, in any order.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void readsAnywhereInTheItem() throws Exception
    {
        byte[] bytes = new byte[1000];
        new Random(7).nextBytes(bytes);
        LandedItem item = land(bytes);
        try (LandedItemSource source =
                LandedItemSource.open(table, item.itemId(), keys))
        {
            assertThat(source.size()).isEqualTo(1000);
            assertThat(source.digest()).isEqualTo(item.digest());
            assertThat(source.readFully(0, 1000)).isEqualTo(bytes);
            Random random = new Random(11);
            for (int read = 0; read < 50; read++)
            {
                int position = random.nextInt(1000);
                int length = 1 + random.nextInt(1000 - position);
                byte[] expected = new byte[length];
                System.arraycopy(bytes, position, expected, 0, length);
                assertThat(source.readFully(position, length))
                        .isEqualTo(expected);
            }
            assertThat(source.read(1000, ByteBuffer.allocate(4)))
                    .isEqualTo(-1);
        }
    }

    /**
     * An empty item has no chunks to read.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void readsAnEmptyItem() throws Exception
    {
        LandedItem item = land(new byte[0]);
        try (LandedItemSource source =
                LandedItemSource.open(table, item.itemId(), keys))
        {
            assertThat(source.size()).isZero();
            assertThat(source.read(0, ByteBuffer.allocate(4))).isEqualTo(-1);
        }
    }

    /** An item that isn't landed can't be opened. */
    @Test
    void refusesAnItemThatIsntLanded()
    {
        assertThatThrownBy(() -> LandedItemSource.open(table,
                UUID.randomUUID(), keys))
                .isInstanceOf(LandedItemSource.NotLandedException.class);
    }
}
