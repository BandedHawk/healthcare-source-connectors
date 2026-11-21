/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.stage.control.ControlResults;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The break-glass results (CONTROL-API-DESIGN.md §6): held encrypted under
 * the stage's KEK, never in the clear at rest; fetched once, whole and in
 * order; and then gone, as is one past its time or whose run failed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlResultsTest
{

    /** Synthetic plaintext, recognisable if it reached the table. */
    static final String PART_ONE = "synthetic-plaintext-part-one|";

    /** The rest of it. */
    static final String PART_TWO = "synthetic-plaintext-part-two";

    /** The schema, fresh for each test. */
    String schema;

    /** The results. */
    ControlResults results;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlResultsTest()
    {
    }

    /**
     * Opens the results in a fresh schema, with a local KEK.
     *
     * @throws Exception if the KEK can't be made
     */
    @BeforeEach
    void open() throws Exception
    {
        schema = StageTestSupport.freshSchema();
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        results = new ControlResults(StageTestSupport.database(), schema,
                new AesKeyWrapProvider(Map.of("records",
                        generator.generateKey())), "records");
    }

    /**
     * A result is stored encrypted, fetched once in order, and deleted on
     * its fetch.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void holdsAResultEncryptedForOneFetch() throws Exception
    {
        UUID id = UUID.randomUUID();
        ControlResults.Holding holding = results.hold(id);
        holding.part(PART_ONE.getBytes(UTF_8));
        holding.part(PART_TWO.getBytes(UTF_8));
        Instant until = holding.finish(Duration.ofHours(1));
        assertThat(until).isAfter(Instant.now().plus(Duration.ofMinutes(59)));
        assertThat(holding.parts()).isEqualTo(2);
        assertThat(stored()).doesNotContain("synthetic");

        ControlResults.Claimed claimed = results.claim(id).orElseThrow();
        assertThat(claimed.parts()).isEqualTo(2);
        assertThat(results.claim(id)).isEmpty();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThat(results.write(claimed, out))
                .isEqualTo((PART_ONE + PART_TWO).length());
        assertThat(out.toString(UTF_8)).isEqualTo(PART_ONE + PART_TWO);
        assertThat(rows()).isZero();
        assertThat(results.claim(id)).isEmpty();
    }

    /**
     * A result past its time, unfinished or discarded isn't fetched, and
     * the sweep deletes the one past its time; with no KEK nothing is
     * held.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void fetchesNothingLateUnfinishedOrDiscarded() throws Exception
    {
        UUID late = UUID.randomUUID();
        ControlResults.Holding lateHolding = results.hold(late);
        lateHolding.part(PART_ONE.getBytes(UTF_8));
        lateHolding.finish(Duration.ofMillis(1));
        Thread.sleep(20);
        assertThat(results.claim(late)).isEmpty();

        UUID unfinished = UUID.randomUUID();
        results.hold(unfinished).part(PART_ONE.getBytes(UTF_8));
        assertThat(results.claim(unfinished)).isEmpty();

        UUID discarded = UUID.randomUUID();
        ControlResults.Holding gone = results.hold(discarded);
        gone.part(PART_ONE.getBytes(UTF_8));
        gone.discard();
        assertThat(results.claim(discarded)).isEmpty();

        assertThat(results.sweep()).isEqualTo(1);
        assertThat(rows()).isEqualTo(1);

        ControlResults keyless = new ControlResults(
                StageTestSupport.database(), schema, null, null);
        assertThatThrownBy(() -> keyless.hold(UUID.randomUUID()))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * The parts as stored.
     *
     * @return their bytes, as text
     * @throws Exception if they can't be read
     */
    String stored() throws Exception
    {
        StringBuilder all = new StringBuilder();
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT body FROM "
                        + schema + ".sdh_control_result_parts"))
        {
            while (row.next())
            {
                all.append(new String(row.getBytes(1), UTF_8));
            }
        }
        return all.toString();
    }

    /**
     * How many results are held.
     *
     * @return the count
     * @throws Exception if it can't be read
     */
    long rows() throws Exception
    {
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                Statement statement = connection.createStatement();
                ResultSet row = statement.executeQuery("SELECT count(*) FROM "
                        + schema + ".sdh_control_results"))
        {
            row.next();
            return row.getLong(1);
        }
    }
}
