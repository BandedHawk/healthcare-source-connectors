/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.receive.BatchHandler.Outcome;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.file.FileConnectorApplication;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The split stage's admin commands (design §11.4) against data the stage
 * wrote: embedded Postgres (leases, item rows and JDBC catalog), in-memory
 * object storage, local keys. The stage's input isn't started; the test
 * hands it its {@code item.landed} messages, as the input route would.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = {"camel.main.run-controller=false",
        "sdh.split.autostart=false"},
        classes = FileConnectorApplication.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class AdminCommandsTest
{

    /** The test's temporary directory. */
    static final Path ROOT;

    /** The landing intake's name. */
    static final String INTAKE = "claims";

    /** The JDBC catalog's properties, for the landing stand-in. */
    static final Map<String, String> CATALOG = new LinkedHashMap<>();

    /** The key files. */
    static final TestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres PG;

    /**
     * The connector's settings, also given to the admin commands as
     * --key=value overrides.
     */
    static final Map<String, String> CONFIG = new LinkedHashMap<>();

    /** Fatal errors the connector reported; there should be none. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** A file that splits completely. */
    static final String GOOD = "1,alpha\n2,beta\n3,gamma\n";

    /**
     * Seven records, so at least one batch of five is committed before
     * the unterminated quote fails the item.
     */
    static final String BAD =
            "1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n8,\"never closed\n9,h\n";

    static
    {
        try
        {
            ROOT = Files.createTempDirectory("sdh-admin");
            KEYS = new TestKeys(
                    Files.createDirectories(ROOT.resolve("keys")));
            PG = EmbeddedPostgres.start();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
        String jdbc = PG.getJdbcUrl("postgres", "postgres");
        CATALOG.put("type", "jdbc");
        CATALOG.put("uri", jdbc);
        CATALOG.put("warehouse", "memory://wh-" + UUID.randomUUID());
        CATALOG.put("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO");
        CATALOG.put("jdbc.user", "postgres");
        CATALOG.put("jdbc.password", "postgres");
        CATALOG.put("jdbc.schema-version", "V1");
        CONFIG.put("sdh.landing.intake", INTAKE);
        CONFIG.put("sdh.stage.input.type", "kafka");
        CONFIG.put("sdh.stage.input.uri",
                "kafka:sdh.landed?brokers=localhost:1&groupId=split");
        CONFIG.put("sdh.stage.registry-schema", "sdh_split");
        CONFIG.put("sdh.stage.output.type", "camel");
        CONFIG.put("sdh.stage.output.uri", "seda:sdh-admin-out?size=100000");
        CONFIG.put("sdh.split.format", "csv");
        CONFIG.put("sdh.storage.catalog-name", TestLanding.CATALOG);
        CATALOG.forEach((key, value) ->
                CONFIG.put("sdh.storage.catalog[" + key + "]", value));
        CONFIG.put("sdh.storage.commit.max-records", "5");
        CONFIG.put("sdh.lease.jdbc.url", jdbc);
        CONFIG.put("sdh.lease.jdbc.username", "postgres");
        CONFIG.put("sdh.lease.jdbc.password", "postgres");
        CONFIG.put("sdh.crypto.keystore.path", KEYS.keystore.toString());
        CONFIG.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        CONFIG.put("sdh.crypto.keys.records-kek", TestKeys.RECORDS);
        CONFIG.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        CONFIG.put("sdh.crypto.keys.inbox.kid", TestKeys.INBOX_KID);
        CONFIG.put("sdh.crypto.keys.inbox.pem", KEYS.inboxPem.toString());
        CONFIG.put("sdh.crypto.keys.inbox.passphrase",
                TestKeys.INBOX_PASSPHRASE);
        CONFIG.put("sdh.crypto.keys.landing-signer.kid",
                TestKeys.LANDING_SIGNER_KID);
        CONFIG.put("sdh.crypto.keys.landing-signer.pem",
                KEYS.landingSignerPem.toString());
        CONFIG.put("sdh.crypto.keys.signing.kid", "connector-2026");
        CONFIG.put("sdh.crypto.keys.signing.pem", KEYS.signingPem.toString());
        CONFIG.put("sdh.crypto.keys.signing.passphrase",
                TestKeys.SIGNING_PASSPHRASE);
        CONFIG.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        CONFIG.put("sdh.crypto.keys.recipient.pem",
                KEYS.recipientPem.toString());
        try
        {
            CONFIG.put("sdh.crypto.keys.recipient.sha256",
                    KeyMaterial.fingerprint(KEYS.recipient.getPublic()));
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
        CONFIG.put("sdh.retry.max-attempts", "2");
        CONFIG.put("sdh.retry.initial-backoff", "10ms");
    }

    /** Records fatal errors instead of exiting the test JVM. */
    @TestConfiguration
    static class Overrides
    {

        /**
         * The recording exit hook.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Gives the connector its settings.
     *
     * @param registry the Spring property registry
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry)
    {
        CONFIG.forEach((key, value) -> registry.add(key, () -> value));
    }

    /** The stage's runtime. */
    @Autowired
    ConnectorRuntime runtime;

    /** The item that split completely. */
    UUID goodId;

    /** The item that failed part-way. */
    UUID badId;

    /**
     * Lands a good and a bad item and hands the stage their messages.
     *
     * @throws Exception if the setup fails
     */
    @BeforeAll
    void split() throws Exception
    {
        TestLanding landing = new TestLanding(CATALOG, runtime.keys().wrap(),
                KEYS, INTAKE);
        LandedItem good = landing.land(GOOD, "good-smith.csv");
        LandedItem bad = landing.land(BAD, "bad-jones.csv");
        SplitHandler handler = new SplitHandler(runtime.splitter(),
                runtime.keys().inbox(), INTAKE, TestLanding.CATALOG,
                LandingTables.DEFAULT_NAMESPACE, FATAL::add);
        for (LandedItem item : List.of(good, bad))
        {
            assertThat(handler.handle(List.of(new BrokerMessage(
                    landing.announcement(item), UUID.randomUUID().toString(),
                    Map.of())))).isEqualTo(Outcome.DONE);
        }
        assertThat(FATAL).isEmpty();
        goodId = good.itemId();
        badId = bad.itemId();
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
     * Runs an admin command with the connector's settings.
     *
     * @param env  the environment
     * @param args the command and its options
     * @return what the command did
     */
    static Result admin(Map<String, String> env, String... args)
    {
        List<String> all = new ArrayList<>(List.of(args));
        CONFIG.forEach((key, value) -> all.add("--" + key + "=" + value));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = SplitAdmin.cli().run(all,
                new PrintStream(out, true, UTF_8),
                new PrintStream(err, true, UTF_8), env);
        return new Result(status, out.toString(UTF_8), err.toString(UTF_8));
    }

    /**
     * Runs an admin command as operator {@code alice}.
     *
     * @param args the command and its options
     * @return what the command did
     */
    static Result admin(String... args)
    {
        return admin(Map.of(AdminCli.OPERATOR_ENV, "alice"), args);
    }

    /**
     * A new, empty output directory.
     *
     * @return the directory
     * @throws Exception if it can't be created
     */
    static Path newDir() throws Exception
    {
        return Files.createTempDirectory(ROOT, "out");
    }

    /**
     * The names of the files in a directory.
     *
     * @param dir the directory
     * @return the names, sorted
     * @throws Exception if the directory can't be listed
     */
    static List<String> names(Path dir) throws Exception
    {
        try (Stream<Path> paths = Files.list(dir))
        {
            return paths.map(path -> path.getFileName().toString()).sorted()
                    .toList();
        }
    }

    /**
     * A failed item restores whole, and as only its unprocessed tail;
     * neither overwrites, and both are audited.
     *
     * @param log the captured log
     * @throws Exception if the test fails to run
     */
    @Test
    void restoresAFailedItemWholeAndOnlyItsUnprocessedTail(CapturedOutput log)
            throws Exception
    {
        Path dir = newDir();
        Result whole = admin("restore", "--item-id", badId.toString(), "--to",
                dir.toString());
        assertThat(whole.status()).as(whole.err()).isZero();
        assertThat(whole.out()).contains("from landing, digest verified");
        assertThat(Files.readString(dir.resolve(badId.toString())))
                .isEqualTo(BAD);
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(
                dir.resolve(badId.toString())))).isEqualTo("rw-------");

        // The tail starts right after the last split record: at least one
        // batch of five was committed.
        int from = (int) RestoreCommand.unprocessedFrom(
                runtime.tables().splitRecords(), badId);
        assertThat(BAD.substring(0, from)).endsWith("\n");
        assertThat(BAD.substring(0, from).chars()
                .filter(character -> character == '\n').count())
                .isGreaterThanOrEqualTo(5);
        Result tail = admin("restore", "--item-id", badId.toString(), "--to",
                dir.toString(), "--unprocessed");
        assertThat(tail.status()).as(tail.err()).isZero();
        assertThat(Files.readString(dir.resolve(badId + ".from-" + from)))
                .isEqualTo(BAD.substring(from));

        // Never overwrites, and leaves no partial file behind.
        Result again = admin("restore", "--item-id", badId.toString(), "--to",
                dir.toString());
        assertThat(again.status()).isEqualTo(1);
        assertThat(again.err()).contains("already exists");
        assertThat(names(dir)).containsExactly(badId.toString(),
                badId + ".from-" + from);

        assertThat(log.getAll())
                .contains("admin.restore operator=alice item_id=" + badId)
                .contains("result=ok")
                .contains("admin.restore operator=alice item_id=" + badId
                        + " table=landed_items offset=0 result=failed"
                        + " reason=FileAlreadyExistsException");
    }

    /**
     * A completed item restores under its original name without printing
     * it, from an offset, and with an empty unprocessed tail.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void restoresACompletedItemUnderItsOriginalNameWithoutPrintingIt()
            throws Exception
    {
        Path dir = newDir();
        Result named = admin("restore", "--item-id", goodId.toString(),
                "--to", dir.toString(), "--original-name");
        assertThat(named.status()).as(named.err()).isZero();
        assertThat(named.out()).contains("from landing")
                .contains("under its original name")
                .doesNotContain("good-smith");
        assertThat(Files.readString(dir.resolve("good-smith.csv")))
                .isEqualTo(GOOD);

        Result partial = admin("restore", "--item-id", goodId.toString(),
                "--to", dir.toString(), "--from-offset", "8");
        assertThat(partial.status()).as(partial.err()).isZero();
        assertThat(Files.readString(dir.resolve(goodId + ".from-8")))
                .isEqualTo(GOOD.substring(8));

        // A completed item has no unprocessed tail.
        Result unprocessed = admin("restore", "--item-id", goodId.toString(),
                "--to", dir.toString(), "--unprocessed");
        assertThat(unprocessed.status()).as(unprocessed.err()).isZero();
        assertThat(Files.size(dir.resolve(goodId + ".from-" + GOOD.length())))
                .isZero();
    }

    /**
     * Status shows the duties lease and the items by state, from the item
     * rows alone, so it needs no keys.
     */
    @Test
    void statusShowsTheItemsByStateWithoutKeys()
    {
        Result status = admin("status");
        assertThat(status.status()).as(status.out() + status.err()).isZero();
        assertThat(status.out())
                .containsPattern("duties: +split/file-connector/duties never"
                        + " held")
                .containsPattern("items: +1 COMPLETED, 1 FAILED");
        Result keyless = admin("status",
                "--sdh.crypto.keystore.password=wrong");
        assertThat(keyless.status()).as(keyless.err()).isZero();
        assertThat(keyless.out()).containsPattern("items: +1 COMPLETED");
    }

    /**
     * One item's status shows its lease and its checkpoint, with why it
     * failed; its checkpoint needs the keys, and without them is shown as
     * unavailable.
     */
    @Test
    void statusShowsOneItemsCheckpoint()
    {
        Result status = admin("status", "--item", badId.toString());
        assertThat(status.status()).as(status.out() + status.err()).isZero();
        assertThat(status.out())
                .containsPattern("lease: +split/file-connector/" + badId)
                .containsPattern("checkpoint: +FAILED \\(csv\\)")
                .contains("stage split, PARSE_UNTERMINATED_QUOTE");
        assertThat(admin("status", "--item", goodId.toString()).out())
                .containsPattern("checkpoint: +COMPLETED \\(csv\\), "
                        + GOOD.length() + " bytes, records committed 3");
        assertThat(admin("status", "--item", UUID.randomUUID().toString())
                .out()).contains("none: the stage hasn't started this item");

        Result keyless = admin("status", "--item", badId.toString(),
                "--sdh.crypto.keystore.password=wrong");
        assertThat(keyless.status()).isEqualTo(1);
        assertThat(keyless.out())
                .containsPattern("lease: +split/file-connector/" + badId)
                .containsPattern("checkpoint: +unavailable \\(");
    }

    /**
     * A command needs a valid operator and refuses unknown options,
     * commands and IDs.
     *
     * @param log the captured log
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesToRunWithoutAnOperatorOrWithUnknownOptions(CapturedOutput log)
            throws Exception
    {
        Result noOperator = admin(Map.of(), "status");
        assertThat(noOperator.status()).isEqualTo(2);
        assertThat(noOperator.err()).contains("SDH_OPERATOR");
        // As an option instead, for kubectl exec into an image without env;
        // it wins over the environment.
        Result byOption = admin(Map.of(AdminCli.OPERATOR_ENV, "alice"),
                "status", "--operator", "carol");
        assertThat(byOption.status()).as(byOption.err()).isZero();
        assertThat(log.getAll()).contains("admin.status operator=carol")
                .doesNotContain("admin.status operator=alice");
        assertThat(admin(Map.of(), "status", "--operator", "not ok").status())
                .isEqualTo(2);

        Result typo = admin("restore", "--item-id", badId.toString(), "--to",
                newDir().toString(), "--form-offset", "3");
        assertThat(typo.status()).isEqualTo(2);
        assertThat(typo.err()).contains("Unknown option --form-offset");

        assertThat(admin("frobnicate").status()).isEqualTo(2);
        assertThat(admin("restore", "--item-id", "not-a-uuid", "--to",
                ROOT.toString()).err()).contains("must be a UUID");
        assertThat(admin("restore", "--item-id", UUID.randomUUID().toString(),
                "--to", ROOT.toString()).err())
                .contains("No fully landed item");
    }

    /** A stored name can never write outside the output directory. */
    @Test
    void storedNamesCanNeverEscapeTheOutputDirectory()
    {
        assertThat(OutputFiles.safeName("../../etc/passwd"))
                .isEqualTo("passwd");
        assertThat(OutputFiles.safeName("C:\\in\\x.csv")).isEqualTo("x.csv");
        assertThatThrownBy(() -> OutputFiles.safeName("dir/.."))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> OutputFiles.safeName(".hidden"))
                .isInstanceOf(IllegalStateException.class);
    }
}
