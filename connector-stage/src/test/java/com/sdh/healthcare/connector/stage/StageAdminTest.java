/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.StageAdmin;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The shared admin CLI (design §11.4) and the status of a stage on the
 * shared worker, against a registry and leases in embedded Postgres, with
 * the settings bound as a stage binds them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageAdminTest
{

    /** The connector ID, which names the duties lease. */
    static final String CONNECTOR = "test-stage";

    /** Captures the audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The registry's schema. */
    String schema;

    /** An item part-way through: 2 of 3 stored, 1 failed, 1 claimed. */
    final UUID partway = UUID.randomUUID();

    /** An item whose records haven't arrived: 0 of 5. */
    final UUID waiting = UUID.randomUUID();

    /** An item that is complete. */
    final UUID complete = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageAdminTest()
    {
    }

    /**
     * The settings the test's stage binds.
     *
     * @param connectorId    names the duties lease
     * @param lease          the lease database
     * @param telemetry      where audit events go
     * @param registrySchema the registry's schema
     */
    @ConfigurationProperties("sdh")
    record TestSettings(@DefaultValue(CONNECTOR) String connectorId,
            @DefaultValue CommonSettings.Lease lease,
            @DefaultValue CommonSettings.Telemetry telemetry,
            String registrySchema)
    {
    }

    /** Binds the test's settings and nothing else. */
    @EnableConfigurationProperties(TestSettings.class)
    static class Configuration
    {

        /** Created by Spring. */
        Configuration()
        {
        }
    }

    /** What the test's commands read from. */
    static final class Context implements StageAdmin, AutoCloseable
    {

        /** The settings. */
        private final TestSettings settings;

        /** The lease database. */
        private final AdminDatabase database;

        /**
         * Creates the context.
         *
         * @param settings the settings
         */
        Context(TestSettings settings)
        {
            this.settings = settings;
            this.database = new AdminDatabase(settings.lease());
        }

        @Override
        public String stage()
        {
            return "test";
        }

        @Override
        public String connectorId()
        {
            return settings.connectorId();
        }

        @Override
        public JdbcLeaseStore leases()
        {
            return database.leases();
        }

        @Override
        public RegistryReport registry()
        {
            return new RegistryReport(database.dataSource(),
                    settings.registrySchema(), "test");
        }

        @Override
        public void close()
        {
            database.close();
        }
    }

    /**
     * What a command did.
     *
     * @param status the exit status
     * @param out    the standard output
     * @param err    the standard error
     */
    record Result(int status, String out, String err)
    {
    }

    /**
     * Fills a registry: an item part-way, one waiting, one complete, and
     * the duties lease held.
     *
     * @throws Exception if the setup fails
     */
    @BeforeEach
    void registry() throws Exception
    {
        schema = StageTestSupport.freshSchema();
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        registry.expect(partway, 3);
        for (String record : List.of("r1", "r2"))
        {
            registry.claim(record, partway, Duration.ofMinutes(5));
        }
        registry.store(List.of(entry("r1", partway, false),
                entry("r2", partway, true)));
        registry.claim("r3", partway, Duration.ofMillis(1));
        registry.expect(waiting, 5);
        registry.expect(complete, 1);
        registry.claim("r9", complete, Duration.ofMinutes(5));
        registry.store(List.of(entry("r9", complete, false)));
        registry.markCompleted(complete);
        Thread.sleep(20);

        JdbcLeaseStore leases = new JdbcLeaseStore(StageTestSupport.database(),
                schema + "_lease", Duration.ofSeconds(10));
        leases.createTableIfMissing();
        leases.acquire("test/" + CONNECTOR + "/duties", "pod-a",
                Duration.ofMinutes(5));
    }

    /**
     * A record to store.
     *
     * @param recordId the record
     * @param itemId   its item
     * @param failed   whether it failed
     * @return the entry
     */
    static StageRegistry.Entry entry(String recordId, UUID itemId,
            boolean failed)
    {
        return new StageRegistry.Entry(recordId, itemId, "d-" + recordId,
                "v1", failed, new OutboundMessage("m-" + recordId,
                        MessageType.RECORD_STORED, itemId, recordId,
                        new byte[] {1}));
    }

    /**
     * Runs a command, with the test's database.
     *
     * @param env  the environment
     * @param args the command and its options
     * @return what it did
     */
    Result admin(Map<String, String> env, String... args)
    {
        List<String> all = new ArrayList<>(List.of(args));
        all.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        all.add("--sdh.lease.jdbc.username=postgres");
        all.add("--sdh.lease.jdbc.password=postgres");
        all.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        all.add("--sdh.registry-schema=" + schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = cli().run(all, new PrintStream(out, true, UTF_8),
                new PrintStream(err, true, UTF_8), env);
        return new Result(status, out.toString(UTF_8), err.toString(UTF_8));
    }

    /**
     * Runs a command as operator {@code alice}.
     *
     * @param args the command and its options
     * @return what it did
     */
    Result admin(String... args)
    {
        return admin(Map.of(AdminCli.OPERATOR_ENV, "alice"), args);
    }

    /**
     * The test stage's CLI.
     *
     * @return the CLI
     */
    static AdminCli<TestSettings, Context> cli()
    {
        return new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", Configuration.class, TestSettings.class,
                TestSettings::telemetry, Context::new),
                List.of(new StageStatusCommand<>()));
    }

    /**
     * Status shows the duties lease, the items not yet complete with how
     * far each got, and the claims, one past its time; it is audited.
     */
    @Test
    void showsTheDutiesTheItemsInProgressAndTheClaims()
    {
        Result status = admin("status");
        assertThat(status.status()).as(status.out() + status.err()).isZero();
        assertThat(status.out())
                .containsPattern("duties: +test/test-stage/duties held by"
                        + " pod-a, epoch 1, renewed .+, expires in \\d+")
                .containsPattern("items: +2 in progress, 1 complete")
                .contains(partway + " 2 of 3 stored (1 failed)")
                .contains(waiting + " 0 of 5 stored")
                .doesNotContain(complete.toString())
                .containsPattern("records: +1 claimed")
                .contains("1 of them past their claim")
                .containsPattern("reconciler: +hasn't checked yet");
        assertThat(audit.attributes("admin.status"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.stage", "test")
                .containsEntry("sdh.count", 2L)
                .containsEntry("sdh.result", "ok");
    }

    /**
     * The reconciler's watermark is shown with how long ago it is, and a
     * warning once that is more than an hour: the duties pod is down, or
     * catching up on an outage.
     *
     * @throws Exception if the registry can't be written
     */
    @Test
    void showsHowFarTheReconcilerHasChecked() throws Exception
    {
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        Instant recent = Instant.now().minus(Duration.ofMinutes(20))
                .truncatedTo(ChronoUnit.MICROS);
        registry.markChecked(recent);
        assertThat(admin("status").out())
                .containsPattern("reconciler: +checked up to " + recent
                        + " \\((19|20|21) min ago\\)")
                .doesNotContain("behind");

        Instant days = recent.minus(Duration.ofDays(3));
        StageTestSupport.registry(schema, "pod-a").markChecked(days);
        assertThat(admin("status").out()).as("it only moves forward")
                .contains("checked up to " + recent);

        String fresh = StageTestSupport.freshSchema();
        StageTestSupport.registry(fresh, "pod-a").markChecked(days);
        schema = fresh;
        assertThat(admin("status").out())
                .containsPattern("reconciler: +checked up to " + days)
                .contains("more than 1 h behind: the duties pod is down, or"
                        + " catching up on an outage");
    }

    /**
     * One item shows its records by state; an unknown one says none has
     * arrived; a malformed ID is a usage error.
     */
    @Test
    void showsOneItem()
    {
        Result one = admin("status", "--item", partway.toString());
        assertThat(one.status()).as(one.err()).isZero();
        assertThat(one.out())
                .contains(partway + " 2 of 3 stored (1 failed), in progress")
                .contains("records CLAIMED 1, STORED 2");
        assertThat(audit.attributes("admin.status"))
                .containsEntry("sdh.item_id", partway.toString());

        assertThat(admin("status", "--item", complete.toString()).out())
                .contains(complete + " 1 of 1 stored, complete ");
        UUID unknown = UUID.randomUUID();
        assertThat(admin("status", "--item", unknown.toString()).out())
                .contains(unknown + " none of its records received yet");
        Result malformed = admin("status", "--item", "not-a-uuid");
        assertThat(malformed.status()).isEqualTo(2);
        assertThat(malformed.err()).contains("--item must be a UUID");
    }

    /**
     * A database that can't be reached shows every part as unavailable,
     * and fails without a further error.
     */
    @Test
    void showsUnreadablePartsAsUnavailable()
    {
        Result status = admin("status",
                "--sdh.lease.jdbc.url=jdbc:postgresql://127.0.0.1:1/none");
        assertThat(status.status()).isEqualTo(1);
        assertThat(status.out())
                .containsPattern("duties: +unavailable \\(")
                .containsPattern("items: +unavailable \\(")
                .containsPattern("records: +unavailable \\(")
                .containsPattern("reconciler: +unavailable \\(");
        assertThat(status.err()).isEmpty();
        assertThat(audit.attributes("admin.status"))
                .containsEntry("sdh.result", "failed")
                .containsEntry("sdh.reason", "Unavailable");
    }

    /**
     * A command needs a valid operator, given as an option or in the
     * environment, and refuses unknown options and commands.
     */
    @Test
    void needsAnOperatorAndRefusesUnknownOptions()
    {
        Result none = admin(Map.of(), "status");
        assertThat(none.status()).isEqualTo(2);
        assertThat(none.err()).contains("SDH_OPERATOR")
                .contains("usage: java -jar test-stage.jar admin");
        Result byOption = admin(Map.of(AdminCli.OPERATOR_ENV, "alice"),
                "status", "--operator", "carol");
        assertThat(byOption.status()).as(byOption.err()).isZero();
        assertThat(audit.attributes("admin.status"))
                .containsEntry("sdh.operator", "carol");
        assertThat(admin(Map.of(), "status", "--operator", "not ok")
                .status()).isEqualTo(2);

        Result typo = admin("status", "--itme", partway.toString());
        assertThat(typo.status()).isEqualTo(2);
        assertThat(typo.err()).contains("Unknown option --itme for status");
        assertThat(admin("frobnicate").status()).isEqualTo(2);
        Result help = admin("help");
        assertThat(help.status()).isZero();
        assertThat(help.out()).contains("status [--item <id>]");
    }
}
