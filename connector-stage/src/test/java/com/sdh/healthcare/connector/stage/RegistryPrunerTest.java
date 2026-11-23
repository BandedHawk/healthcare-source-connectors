/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.failure.FailureStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link RegistryPruner} and {@link StageProperties.Retention},
 * on a real Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RegistryPrunerTest
{

    /** The time the tests run at. */
    static final Instant NOW = Instant.parse("2026-11-23T12:00:00Z");

    /** Captures audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The registry's schema. */
    String schema;

    /** Creates the tests; JUnit runs each test on a new instance. */
    RegistryPrunerTest()
    {
    }

    /**
     * Makes the tables in a schema of their own.
     *
     * @throws SQLException if they can't be made
     */
    @BeforeEach
    void tables() throws SQLException
    {
        schema = StageTestSupport.freshSchema();
        StageTestSupport.registry(schema, "pod-a");
        try (Connection connection =
                StageTestSupport.database().getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute(FailureStore.ddl(FailureStore.table(schema)));
        }
    }

    /**
     * Adds an item with records.
     *
     * @param completed when it was completed; null if not
     * @param records   how many records
     * @return the item
     * @throws SQLException if it can't be added
     */
    UUID item(Instant completed, int records) throws SQLException
    {
        UUID id = UUID.randomUUID();
        try (Connection connection =
                StageTestSupport.database().getConnection())
        {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + schema + ".stage_record (stage, item_id,"
                            + " record_id, state, owner, attempts) VALUES"
                            + " ('convert', ?, ?, 'STORED', 'pod-a', 1)"))
            {
                for (int i = 0; i < records; i++)
                {
                    insert.setObject(1, id);
                    insert.setString(2, "r" + i);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + schema + ".stage_item (stage, item_id,"
                            + " expected, done, completed_at) VALUES"
                            + " ('convert', ?, ?, ?, ?)"))
            {
                insert.setObject(1, id);
                insert.setLong(2, records);
                insert.setLong(3, records);
                insert.setTimestamp(4,
                        completed == null ? null : Timestamp.from(completed));
                insert.executeUpdate();
            }
        }
        return id;
    }

    /**
     * Adds a failure of an item.
     *
     * @param itemId the item
     * @param state  its state
     * @throws SQLException if it can't be added
     */
    void failure(UUID itemId, String state) throws SQLException
    {
        try (Connection connection =
                StageTestSupport.database().getConnection();
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + FailureStore.table(schema)
                                + " (stage, item_id, seq, error_code,"
                                + " source_offset, failed_at, state,"
                                + " changed_at) VALUES ('convert', ?, 1,"
                                + " 'X', 0, now(), ?, now())"))
        {
            insert.setObject(1, itemId);
            insert.setString(2, state);
            insert.executeUpdate();
        }
    }

    /**
     * The rows an item has.
     *
     * @param table  the table, without its schema
     * @param itemId the item
     * @return how many
     * @throws SQLException if they can't be counted
     */
    long rows(String table, UUID itemId) throws SQLException
    {
        try (Connection connection =
                StageTestSupport.database().getConnection();
                PreparedStatement count = connection.prepareStatement(
                        "SELECT count(*) FROM " + schema + "." + table
                                + " WHERE item_id = ?"))
        {
            count.setObject(1, itemId);
            try (ResultSet result = count.executeQuery())
            {
                result.next();
                return result.getLong(1);
            }
        }
    }

    /**
     * A pruner keeping finished items' rows for 30 days, deleting two
     * items a transaction.
     *
     * @return it
     */
    RegistryPruner pruner()
    {
        return new RegistryPruner(StageTestSupport.database(), schema,
                "convert", new StageProperties.Retention(Duration.ofDays(30),
                        Duration.ofDays(7), Duration.ofHours(1), 2),
                Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(10));
    }

    /**
     * Only items complete for longer than the retention, with no open
     * failure, lose their rows; failures are kept; the run is audited
     * with a count.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void prunesOnlyFinishedItemsPastRetention() throws Exception
    {
        Instant old = NOW.minus(Duration.ofDays(31));
        UUID[] gone = {item(old, 3), item(old, 1), item(old, 2)};
        UUID recent = item(NOW.minus(Duration.ofDays(29)), 2);
        UUID unfinished = item(null, 2);
        UUID open = item(old, 2);
        failure(open, "FAILED");
        UUID declined = item(old, 1);
        failure(declined, "DECLINED");

        pruner().prune();

        for (UUID id : gone)
        {
            assertThat(rows("stage_record", id)).isZero();
            assertThat(rows("stage_item", id)).isZero();
        }
        assertThat(rows("stage_record", declined)).isZero();
        assertThat(rows("stage_failure", declined)).isOne();
        assertThat(rows("stage_record", recent)).isEqualTo(2);
        assertThat(rows("stage_record", unfinished)).isEqualTo(2);
        assertThat(rows("stage_record", open)).isEqualTo(2);
        assertThat(rows("stage_item", open)).isOne();
        assertThat(audit.names()).containsExactly("stage.registry_pruned");
        assertThat(audit.attributes("stage.registry_pruned"))
                .containsEntry("sdh.stage", "convert")
                .containsEntry("sdh.count", 4L);
    }

    /**
     * A second run within the interval does nothing.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void runsOnceAnInterval() throws Exception
    {
        RegistryPruner pruner = pruner();
        pruner.prune();
        UUID later = item(NOW.minus(Duration.ofDays(40)), 1);
        pruner.prune();
        assertThat(rows("stage_record", later)).isOne();
    }

    /**
     * Kept for ever, nothing is deleted.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void foreverPrunesNothing() throws Exception
    {
        UUID old = item(NOW.minus(Duration.ofDays(4000)), 1);
        new RegistryPruner(StageTestSupport.database(), schema, "convert",
                StageProperties.Retention.FOREVER,
                Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(10))
                .prune();
        assertThat(rows("stage_record", old)).isOne();
        assertThat(audit.names()).isEmpty();
    }

    /**
     * A retention shorter than the queue's, or the reconciler's look back,
     * each with a day's margin, is refused.
     */
    @Test
    void retentionBelowTheQueuesIsRefused()
    {
        assertThatThrownBy(() -> new StageProperties.Retention(
                Duration.ofDays(7), Duration.ofDays(7), null, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue-retention");
        StageProperties.Retention ok = new StageProperties.Retention(
                Duration.ofDays(8), Duration.ofDays(7), null, 100);
        assertThat(ok.forever()).isFalse();
        ok.checkLookback(Duration.ofDays(7));
        assertThatThrownBy(() -> ok.checkLookback(Duration.ofDays(7)
                .plusHours(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lookback");
        assertThat(StageProperties.Retention.FOREVER.forever()).isTrue();
    }
}
