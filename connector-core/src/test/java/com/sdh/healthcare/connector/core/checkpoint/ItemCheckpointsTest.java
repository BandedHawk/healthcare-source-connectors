/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ItemCheckpoints}, on a real Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ItemCheckpointsTest
{

    /** How many schemas have been handed out. */
    static final AtomicInteger SCHEMAS = new AtomicInteger();

    /** The database. */
    final DataSource database = TestPostgres.get().getPostgresDatabase();

    /** This test's schema. */
    final String schema = "item_checkpoints_" + SCHEMAS.incrementAndGet();

    /** The store. */
    ItemCheckpoints checkpoints;

    /** The item. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ItemCheckpointsTest()
    {
    }

    /** Makes the table in the test's schema. */
    @BeforeEach
    void table()
    {
        checkpoints = new ItemCheckpoints(database, schema,
                new CheckpointCodec(new RecordIds(new byte[32])),
                Duration.ofSeconds(10));
        checkpoints.createTableIfMissing();
    }

    /**
     * A lease that is always held.
     *
     * @param epoch its epoch
     */
    record Held(long epoch) implements LeaseToken
    {

        /** Never finds the lease lost. */
        @Override
        public void checkHeld()
        {
        }
    }

    /**
     * A new checkpoint for an item.
     *
     * @param itemId the item
     * @return the checkpoint
     */
    static Checkpoint start(UUID itemId)
    {
        return Checkpoint.start(itemId, "item:" + itemId, "csv", 100,
                Instant.parse("2026-11-05T10:00:00Z"));
    }

    /**
     * A checkpoint is written and read back; one after a batch is kept as
     * pending until it is saved.
     */
    @Test
    void keepsACheckpointAndItsPendingBatch()
    {
        assertThat(checkpoints.load(item)).isEmpty();
        Checkpoint first = start(item);
        checkpoints.save(first, new Held(1));
        Checkpoint next = first.committed(ResumePoint.at(40), 3, -1);
        checkpoints.prepare(next, new Held(1));
        assertThat(checkpoints.load(item)).hasValue(
                new ItemCheckpoints.Stored(first, next, null));
        checkpoints.save(next, new Held(1));
        assertThat(checkpoints.load(item)).hasValue(
                new ItemCheckpoints.Stored(next, null, null));
        assertThat(checkpoints.known(List.of(item, UUID.randomUUID())))
                .containsExactly(item);
    }

    /**
     * The report counts the items by state and lists those not finished,
     * from the plain columns, without the codec.
     */
    @Test
    void reportsTheItemsByStateAndThoseNotFinished()
    {
        UUID done = UUID.randomUUID();
        checkpoints.save(start(item), new Held(2));
        checkpoints.save(start(done).withState(Checkpoint.State.COMPLETED),
                new Held(1));
        SplitItemReport report = new SplitItemReport(database, schema);
        assertThat(report.countsByState()).containsExactly(
                Map.entry("COMPLETED", 1L), Map.entry("SPLITTING", 1L));
        assertThat(report.unfinished(10)).singleElement().satisfies(row ->
        {
            assertThat(row.itemId()).isEqualTo(item);
            assertThat(row.state()).isEqualTo("SPLITTING");
            assertThat(row.epoch()).isEqualTo(2);
            assertThat(row.updatedAt()).isNotNull();
        });
        assertThat(report.unfinished(0)).isEmpty();
        assertThatThrownBy(() -> new SplitItemReport(database, "x; y"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Items in progress whose checkpoint hasn't been written since a time
     * are listed as stalled; finished ones, and those written since, are
     * not.
     */
    @Test
    void listsItemsLeftPartWay()
    {
        UUID done = UUID.randomUUID();
        UUID archiving = UUID.randomUUID();
        checkpoints.save(start(item), new Held(1));
        checkpoints.save(start(done).withState(Checkpoint.State.COMPLETED),
                new Held(1));
        checkpoints.save(start(archiving).withState(Checkpoint.State.ARCHIVING),
                new Held(1));
        Instant later = Instant.now().plus(Duration.ofMinutes(1));
        assertThat(checkpoints.stalled(later))
                .containsExactly(item, archiving);
        assertThat(checkpoints.stalled(
                Instant.now().minus(Duration.ofMinutes(1)))).isEmpty();
    }

    /**
     * Items being split, archived or failed are part way under the version
     * they were begun under; held ones are listed apart, and finished ones
     * and those of no known version not at all (STAGES.md §14.4a).
     */
    @Test
    void countsTheItemsPartWayAndHeldByVersion()
    {
        UUID archiving = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        UUID held = UUID.randomUUID();
        UUID done = UUID.randomUUID();
        UUID unknown = UUID.randomUUID();
        checkpoints.save(begun(item, "v1"), new Held(1));
        checkpoints.save(begun(archiving, "v1")
                .withState(Checkpoint.State.ARCHIVING), new Held(1));
        checkpoints.save(begun(other, "v2"), new Held(1));
        checkpoints.save(begun(held, "v0")
                .withState(Checkpoint.State.HELD), new Held(1));
        checkpoints.save(begun(done, "v1")
                .withState(Checkpoint.State.COMPLETED), new Held(1));
        checkpoints.save(start(unknown), new Held(1));

        List<ItemCheckpoints.ByVersion> partWay = checkpoints.partWay(1);
        assertThat(partWay).extracting(ItemCheckpoints.ByVersion::version,
                ItemCheckpoints.ByVersion::count).containsExactly(
                        tuple("v1", 2L), tuple("v2", 1L));
        assertThat(partWay.get(0).first()).singleElement()
                .isIn(item, archiving);
        assertThat(checkpoints.held(5)).containsExactly(
                new ItemCheckpoints.ByVersion("v0", 1, List.of(held)));
    }

    /**
     * A new checkpoint for an item begun under a version.
     *
     * @param itemId  the item
     * @param version the version
     * @return the checkpoint
     */
    static Checkpoint begun(UUID itemId, String version)
    {
        return Checkpoint.start(itemId, "item:" + itemId, "csv", 100,
                Instant.parse("2026-11-05T10:00:00Z"), version);
    }

    /**
     * The end of an item's splitting is noted once; it is counted finished
     * once it is {@code COMPLETED}.
     */
    @Test
    void notesWhenAnItemFinished()
    {
        checkpoints.save(start(UUID.randomUUID()), new Held(1));
        assertThat(checkpoints.countFinished()).isZero();
        Checkpoint archiving = start(item)
                .withState(Checkpoint.State.ARCHIVING);
        checkpoints.save(archiving, new Held(1));
        assertThat(checkpoints.countFinished()).isZero();
        Instant ended = checkpoints.load(item).orElseThrow().endedAt();
        assertThat(ended).isNotNull();
        Checkpoint done = archiving.withState(Checkpoint.State.COMPLETED);
        checkpoints.save(done, new Held(1));
        checkpoints.save(done, new Held(1));
        assertThat(checkpoints.countFinished()).isOne();
        assertThat(checkpoints.load(item).orElseThrow().endedAt())
                .isEqualTo(ended);
    }

    /**
     * A worker whose lease was taken over writes nothing: the row has the
     * successor's higher epoch. One whose lease is lost writes nothing
     * either.
     */
    @Test
    void fencesAWorkerWhoseLeaseWasTakenOver()
    {
        Checkpoint first = start(item);
        checkpoints.save(first, new Held(2));
        Checkpoint next = first.committed(ResumePoint.at(40), 3, -1);
        assertThatThrownBy(() -> checkpoints.save(next, new Held(1)))
                .isInstanceOf(LeaseLostException.class);
        assertThatThrownBy(() -> checkpoints.prepare(next, new Held(1)))
                .isInstanceOf(LeaseLostException.class);
        LeaseToken lost = new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 3;
            }

            @Override
            public void checkHeld()
            {
                throw new LeaseLostException(
                        LeaseLostException.Reason.EXPIRED);
            }
        };
        assertThatThrownBy(() -> checkpoints.save(next, lost))
                .isInstanceOf(LeaseLostException.class);
        assertThat(checkpoints.load(item).orElseThrow().checkpoint())
                .isEqualTo(first);
        checkpoints.save(next, new Held(3));
        assertThat(checkpoints.load(item).orElseThrow().checkpoint())
                .isEqualTo(next);
    }

    /**
     * A checkpoint changed in the database, or moved to another item's
     * row, is refused.
     *
     * @throws Exception if the database can't be changed
     */
    @Test
    void refusesAChangedOrMovedCheckpoint() throws Exception
    {
        UUID other = UUID.randomUUID();
        checkpoints.save(start(item), new Held(1));
        checkpoints.save(start(other), new Held(1));
        try (Connection connection = database.getConnection();
                PreparedStatement move = connection.prepareStatement(
                        "UPDATE " + schema + ".split_item SET checkpoint ="
                                + " (SELECT checkpoint FROM " + schema
                                + ".split_item WHERE item_id = ?)"
                                + " WHERE item_id = ?"))
        {
            move.setObject(1, other);
            move.setObject(2, item);
            assertThat(move.executeUpdate()).isEqualTo(1);
        }
        assertThatThrownBy(() -> checkpoints.load(item))
                .isInstanceOf(CheckpointTamperedException.class);
        try (Connection connection = database.getConnection();
                PreparedStatement change = connection.prepareStatement(
                        "UPDATE " + schema + ".split_item SET checkpoint ="
                                + " replace(checkpoint, '100', '101')"
                                + " WHERE item_id = ?"))
        {
            change.setObject(1, other);
            assertThat(change.executeUpdate()).isEqualTo(1);
        }
        assertThatThrownBy(() -> checkpoints.load(other))
                .isInstanceOf(CheckpointTamperedException.class);
    }

    /**
     * When an item landed is kept for measuring (docs/PERFORMANCE.md §10),
     * and the first noted stays: splitting it again, or a reprocess, notes
     * it again without moving it.
     *
     * @throws Exception if the database can't be read
     */
    @Test
    void keepsWhenAnItemFirstLanded() throws Exception
    {
        Instant landed = Instant.parse("2026-11-13T10:00:00.123456Z");
        checkpoints.noteLanded(item, landed);
        checkpoints.noteLanded(item, landed.plusSeconds(60));
        try (Connection connection = database.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT landed_at FROM " + schema
                                + ".split_landed WHERE item_id = ?"))
        {
            select.setObject(1, item);
            try (ResultSet rows = select.executeQuery())
            {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getTimestamp(1).toInstant())
                        .isEqualTo(landed);
                assertThat(rows.next()).isFalse();
            }
        }
    }

    /** A schema name that isn't a plain identifier is refused. */
    @Test
    void refusesAnOddSchemaName()
    {
        assertThatThrownBy(() -> new ItemCheckpoints(database,
                "x; drop table y", null, Duration.ofSeconds(1)))
                .hasMessageContaining("schema");
    }
}
