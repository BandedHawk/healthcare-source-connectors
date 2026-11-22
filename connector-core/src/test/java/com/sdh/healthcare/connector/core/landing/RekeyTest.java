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
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Rekey} on {@code landed_items} (CONTROL-API-DESIGN.md
 * §11), with {@link ItemLanderTest}'s fixtures: a rotation takes effect at
 * the next batch, and a re-key moves every row and the ID key to the new
 * KEK, file by file within its budget, with every item reading back the
 * same and the old files gone.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RekeyTest
{

    /** The KEK rotated to. */
    static final String NEW_KEK = "landed-items-kek/2";

    /** The fixtures' KEK, rotated from. */
    static final String OLD_KEK = ItemLanderTest.KEK;

    /** The tables, keys and landers. */
    final ItemLanderTest fixtures = new ItemLanderTest();

    /** The landed items table. */
    Table table;

    /** Creates the tests; JUnit runs each test on a new instance. */
    RekeyTest()
    {
    }

    /**
     * Creates the catalog and the table, with both KEKs.
     *
     * @throws Exception if the KEKs can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        fixtures.setUp();
        table = fixtures.table;
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        fixtures.keys = new AesKeyWrapProvider(Map.of(
                OLD_KEK, generator.generateKey(),
                NEW_KEK, generator.generateKey()));
    }

    /**
     * Lands an item.
     *
     * @param bytes  the item
     * @param name   its path
     * @param lander lands it
     * @return its ID
     * @throws Exception if it can't be landed
     */
    UUID land(byte[] bytes, String name, ItemLander lander) throws Exception
    {
        return lander.land(fixtures.fileItem(bytes, name),
                ItemLanderTest.opener(bytes)).itemId();
    }

    /**
     * Runs maintenance with a re-key, as of now.
     *
     * @param budget the most bytes rewritten
     * @return the re-key's run
     */
    Rekey maintain(long budget)
    {
        FencedCommitter committer = fixtures.committer();
        Rekey rekey = new Rekey(committer, fixtures.keys, OLD_KEK, NEW_KEK,
                budget);
        new TableMaintenance(committer, new MaintenanceSettings(
                Duration.ofDays(7), 1, false, 0.75, 2, 8L << 30,
                Duration.ofDays(3)), Clock.fixed(Instant.now(),
                        ZoneOffset.UTC))
                .run(table, LandingTables.LANDED_AT, Optional.empty(), false,
                        UnaryOperator.identity(), rekey);
        return rekey;
    }

    /**
     * The table's data files now.
     *
     * @return their locations
     * @throws Exception if the table can't be read
     */
    List<String> files() throws Exception
    {
        table.refresh();
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
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
     * A lander rotated to the new KEK wraps its next batch with it, and
     * items under either read back. A re-key with a one-byte budget takes
     * a file each run, until a run is quiet; then nothing is under the old
     * KEK, the old files are gone, and every item reads back the same.
     *
     * @throws Exception if the test fails
     */
    @Test
    void movesEveryRowToTheNewKekFileByFile() throws Exception
    {
        byte[] first = ItemLanderTest.data(2 * ItemLanderTest.BLOCK + 5);
        byte[] second = ItemLanderTest.data(ItemLanderTest.BLOCK + 9);
        byte[] third = ItemLanderTest.data(ItemLanderTest.BLOCK + 17);
        UUID one = land(first, "one", fixtures.lander());
        UUID two = land(second, "two", fixtures.lander());
        String[] active = {OLD_KEK};
        UUID three = land(third, "three",
                fixtures.lander().activeKek(() -> active[0]));
        active[0] = NEW_KEK;
        byte[] fourth = ItemLanderTest.data(ItemLanderTest.BLOCK + 3);
        UUID four = land(fourth, "four",
                fixtures.lander().activeKek(() -> active[0]));
        table.refresh();
        Map<String, Long> before = Rekey.byKek(table);
        assertThat(before).containsKeys(OLD_KEK, NEW_KEK);
        long oldRows = before.get(OLD_KEK);
        assertThat(Rekey.left(table, OLD_KEK)).isEqualTo(oldRows);
        List<String> oldFiles = files();

        long rewritten = 0;
        long rows = 0;
        int runs = 0;
        while (true)
        {
            Rekey run = maintain(1);
            runs++;
            assertThat(runs).isLessThan(50);
            Rekey.Done done = run.done().get(table.name());
            assertThat(done.files()).isLessThanOrEqualTo(1);
            rewritten += done.files();
            rows += done.rows();
            if (run.quiet())
            {
                break;
            }
        }
        assertThat(rewritten).isGreaterThan(1);
        assertThat(rows).isEqualTo(oldRows);
        table.refresh();
        assertThat(Rekey.left(table, OLD_KEK)).isZero();
        assertThat(Rekey.byKek(table)).containsOnlyKeys(NEW_KEK);
        List<String> now = files();
        assertThat(oldFiles).anyMatch(location -> !now.contains(location));
        assertThat(oldFiles.stream().filter(location -> !now.contains(
                location)).filter(this::stored).toList()).isEmpty();
        assertThat(fixtures.readBack(one)).isEqualTo(first);
        assertThat(fixtures.readBack(two)).isEqualTo(second);
        assertThat(fixtures.readBack(three)).isEqualTo(third);
        assertThat(fixtures.readBack(four)).isEqualTo(fourth);
    }

    /**
     * The ID key moves to the new KEK and stays the same key; asked again,
     * or for another KEK, it is left as it is.
     *
     * @throws Exception if the test fails
     */
    @Test
    void movesTheIdKeyToTheNewKek() throws Exception
    {
        FencedCommitter committer = fixtures.committer();
        byte[] key = IdKeys.loadOrCreate(table, committer, fixtures.keys,
                OLD_KEK);
        assertThat(IdKeys.kek(table)).contains(OLD_KEK);

        assertThat(IdKeys.rekey(table, committer, fixtures.keys, OLD_KEK,
                NEW_KEK)).isTrue();
        assertThat(IdKeys.kek(table)).contains(NEW_KEK);
        assertThat(IdKeys.load(table, fixtures.keys)).isEqualTo(key);
        assertThat(IdKeys.rekey(table, committer, fixtures.keys, OLD_KEK,
                NEW_KEK)).isFalse();
    }

    /** A re-key needs two different KEKs and a budget. */
    @Test
    void refusesTheSameKekOrNoBudget()
    {
        FencedCommitter committer = fixtures.committer();
        assertThatThrownBy(() -> new Rekey(committer, fixtures.keys, OLD_KEK,
                OLD_KEK, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Rekey(committer, fixtures.keys, OLD_KEK,
                NEW_KEK, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
