/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.convert.ConvertProcessor;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The convert stage's admin CLI: its settings bound as the stage binds
 * them, and the shared status read from its registry.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConvertAdminTest
{

    /** The lease database. */
    static EmbeddedPostgres postgres;

    /** An item with one of its two records stored. */
    static final UUID ITEM = UUID.randomUUID();

    /** Creates the tests. */
    ConvertAdminTest()
    {
    }

    /**
     * Starts the database and stores one record of an item.
     *
     * @throws Exception if it can't be started
     */
    @BeforeAll
    static void registry() throws Exception
    {
        postgres = EmbeddedPostgres.start();
        StageRegistry registry = new StageRegistry(
                postgres.getPostgresDatabase(), "sdh_stage",
                ConvertProcessor.STAGE, "pod-a", Duration.ofSeconds(10));
        registry.createTablesIfMissing();
        registry.expect(ITEM, 2);
        registry.claim("r1", ITEM, Duration.ofMinutes(5));
        registry.store(List.of(new StageRegistry.Entry("r1", ITEM, "d", "v",
                false, new OutboundMessage("m", MessageType.RECORD_STORED,
                        ITEM, "r1", new byte[] {1}))));
    }

    /**
     * Stops the database.
     *
     * @throws Exception if it can't be stopped
     */
    @AfterAll
    static void stop() throws Exception
    {
        postgres.close();
    }

    /**
     * Status binds the stage's own settings and shows its registry: the
     * duties lease, never held here, and the item in progress.
     */
    @Test
    void showsTheConvertStagesStatus()
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = ConvertAdmin.cli().run(List.of("status",
                "--sdh.lease.jdbc.url="
                        + postgres.getJdbcUrl("postgres", "postgres"),
                "--sdh.lease.jdbc.username=postgres",
                "--sdh.lease.jdbc.password=postgres"),
                new PrintStream(out, true, UTF_8),
                new PrintStream(err, true, UTF_8),
                Map.of(AdminCli.OPERATOR_ENV, "alice"));
        assertThat(status).as(out.toString(UTF_8) + err.toString(UTF_8))
                .isZero();
        assertThat(out.toString(UTF_8))
                .containsPattern("duties: +convert/convert-stage/duties"
                        + " never held")
                .containsPattern("items: +1 in progress, 0 complete")
                .contains(ITEM + " 1 of 2 stored")
                .containsPattern("records: +0 claimed");
        assertThat(ConvertAdmin.cli().usage())
                .startsWith("usage: java -jar convert-stage.jar admin");
    }
}
