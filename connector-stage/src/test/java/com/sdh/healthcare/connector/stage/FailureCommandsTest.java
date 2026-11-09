/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.FailureAdmin;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Tests for the fail store's commands ({@link FailureCommands}): listing,
 * declining with a reason from standard input, and reopening, on a real
 * Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class FailureCommandsTest
{

    /** The KEK the reasons' keys are wrapped with. */
    static final String KEK = "kek-test";

    /** Wraps the reasons' keys. */
    static final KeyProvider KEYS = new AesKeyWrapProvider(Map.of(KEK,
            new SecretKeySpec(new byte[32], "AES")));

    /** A reason, synthetic. */
    static final String REASON = "Synthetic test reason 7f3a";

    /** What the failure failed on, synthetic. */
    static final String FAILED_CONTENT = "<synthetic failed record/>";

    /** What the next command reads as its standard input. */
    static volatile byte[] input = new byte[0];

    /** The items the stage was told had changed. */
    static final List<UUID> CHANGED = new CopyOnWriteArrayList<>();

    /** Captures the audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The registry's schema. */
    String schema;

    /** The item with a failure. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    FailureCommandsTest()
    {
    }

    /**
     * The settings the test's stage binds.
     *
     * @param lease          the lease database
     * @param telemetry      where audit events go
     * @param registrySchema the registry's schema
     */
    @ConfigurationProperties("sdh")
    record TestSettings(@DefaultValue CommonSettings.Lease lease,
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

    /** What the test's commands use. */
    static final class Context implements FailureAdmin, AutoCloseable
    {

        /** The settings. */
        private final TestSettings settings;

        /** The lease database, read-only. */
        private final AdminDatabase database;

        /** The lease database, writing. */
        private final AdminDatabase writing;

        /**
         * Creates the context.
         *
         * @param settings the settings
         */
        Context(TestSettings settings)
        {
            this.settings = settings;
            this.database = new AdminDatabase(settings.lease());
            this.writing = new AdminDatabase(settings.lease(), true);
        }

        @Override
        public String stage()
        {
            return "test";
        }

        @Override
        public FailureStore failures()
        {
            return new FailureStore(database.dataSource(),
                    settings.registrySchema(), "test", Duration.ofSeconds(5));
        }

        @Override
        public FailureStore failuresToChange()
        {
            return new FailureStore(writing.dataSource(),
                    settings.registrySchema(), "test", Duration.ofSeconds(5));
        }

        @Override
        public KeyProvider reasonKeys()
        {
            return KEYS;
        }

        @Override
        public String reasonKek()
        {
            return KEK;
        }

        @Override
        public InputStream reasonInput()
        {
            return new ByteArrayInputStream(input);
        }

        @Override
        public boolean corrects()
        {
            return true;
        }

        @Override
        public long maxCorrectionBytes()
        {
            return 64;
        }

        @Override
        public CorrectionStore correctionsToChange()
        {
            return new CorrectionStore(writing.dataSource(),
                    settings.registrySchema(), "test", Duration.ofSeconds(5));
        }

        @Override
        public CorrectionStore corrections()
        {
            return new CorrectionStore(database.dataSource(),
                    settings.registrySchema(), "test", Duration.ofSeconds(5));
        }

        @Override
        public byte[] failedContent(UUID itemId, long seq)
        {
            return FAILED_CONTENT.getBytes(UTF_8);
        }

        @Override
        public void changed(UUID itemId, AuditRecord audit, PrintStream out)
        {
            CHANGED.add(itemId);
            out.println("told the stage");
        }

        @Override
        public void close()
        {
            database.close();
            writing.close();
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
     * An item with one record failed, seq 2.
     *
     * @throws Exception if the setup fails
     */
    @BeforeEach
    void registry() throws Exception
    {
        schema = StageTestSupport.freshSchema();
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        registry.expect(item, 2);
        registry.claim("r1", item, Duration.ofMinutes(5));
        registry.claim("r2", item, Duration.ofMinutes(5));
        registry.store(List.of(StageAdminTest.entry("r1", item, false),
                new StageRegistry.Entry("r2", item, "d-r2", "v1", true, null,
                        new FailureStore.Failure(item, 2, "r2",
                                "TEST_FAILED", 10, "v1"))));
        CHANGED.clear();
        input = new byte[0];
    }

    /**
     * Runs a command as operator {@code alice}.
     *
     * @param stdin what it reads as its standard input
     * @param args  the command and its options
     * @return what it did
     */
    Result admin(String stdin, String... args)
    {
        return admin(stdin.getBytes(UTF_8), args);
    }

    /**
     * Runs a command as operator {@code alice}, with bytes as its input.
     *
     * @param stdin what it reads as its standard input
     * @param args  the command and its options
     * @return what it did
     */
    Result admin(byte[] stdin, String... args)
    {
        input = stdin;
        List<String> all = new ArrayList<>(List.of(args));
        all.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        all.add("--sdh.lease.jdbc.username=postgres");
        all.add("--sdh.lease.jdbc.password=postgres");
        all.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        all.add("--sdh.registry-schema=" + schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = new AdminCli<TestSettings, Context>(
                new AdminCli.Stage<>("test-stage.jar", "test-stage",
                        Configuration.class, TestSettings.class,
                        TestSettings::telemetry, Context::new),
                FailureCommands.<Context>all()).run(all,
                        new PrintStream(out, true, UTF_8),
                        new PrintStream(err, true, UTF_8),
                        Map.of(AdminCli.OPERATOR_ENV, "alice"));
        return new Result(status, out.toString(UTF_8), err.toString(UTF_8));
    }

    /** The store, read directly. */
    FailureStore store()
    {
        return StageTestSupport.registry(schema, "pod-a").failureStore();
    }

    /**
     * {@code failures} lists the open failure by item, record number,
     * state and error code, and is audited.
     */
    @Test
    void listsTheOpenFailures()
    {
        Result listed = admin("", "failures");
        assertThat(listed.status()).as(listed.out() + listed.err()).isZero();
        assertThat(listed.out())
                .contains("1 open at the test stage")
                .contains(item + " seq=2 FAILED TEST_FAILED offset=10");
        assertThat(audit.attributes("admin.failures"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.count", 1L)
                .containsEntry("sdh.result", "ok");
    }

    /**
     * A decline needs a reason on standard input; with none, nothing
     * changes.
     */
    @Test
    void refusesADeclineWithoutAReason()
    {
        Result declined = admin("  \n", "decline", "--item-id",
                item.toString(), "--seq", "2");
        assertThat(declined.status()).isEqualTo(2);
        assertThat(declined.err()).contains("standard input");
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(1, 0, 0));
        assertThat(CHANGED).isEmpty();
    }

    /**
     * A decline moves the failure to DECLINED, keeps the reason encrypted
     * and bound to its event, audits the change without the reason, and
     * tells the stage; a second decline is refused; a reopen moves it back,
     * and the item's history shows both changes, without the reasons.
     */
    @Test
    void declinesAndReopensAFailureWithItsReason()
    {
        Result declined = admin(REASON + "\n", "decline", "--item-id",
                item.toString(), "--seq", "2");
        assertThat(declined.status()).as(declined.out() + declined.err())
                .isZero();
        assertThat(declined.out()).contains("FAILED -> DECLINED")
                .doesNotContain(REASON);
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(0, 1, 0));
        assertThat(CHANGED).containsExactly(item);
        assertThat(audit.attributes("failure.declined"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.from_state", "FAILED")
                .containsEntry("sdh.state", "DECLINED")
                .doesNotContainValue(REASON);
        assertThat(audit.attributes("admin.decline"))
                .containsEntry("sdh.result", "ok");
        List<FailureStore.Event> events = store().events(item, 2);
        assertThat(events).singleElement().satisfies(event ->
        {
            assertThat(event.operator()).isEqualTo("alice");
            assertThat(event.to()).isEqualTo(FailureStore.State.DECLINED);
            assertThat(event.reason().keyRef()).isEqualTo(KEK);
            assertThat(new String(event.reason().sealed(), UTF_8))
                    .doesNotContain(REASON);
            assertThat(event.reason().open(KEYS, item, 2, event.event()))
                    .isEqualTo(REASON);
        });

        Result again = admin(REASON, "decline", "--item-id",
                item.toString(), "--seq", "2");
        assertThat(again.status()).isEqualTo(1);
        assertThat(CHANGED).hasSize(1);

        Result reopened = admin("New information arrived", "reopen",
                "--item-id", item.toString(), "--seq", "2");
        assertThat(reopened.status()).as(reopened.out() + reopened.err())
                .isZero();
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(1, 0, 0));
        assertThat(CHANGED).containsExactly(item, item);

        Result history = admin("", "failures", "--item-id", item.toString());
        assertThat(history.status()).isZero();
        assertThat(history.out())
                .contains(item + " seq=2 FAILED TEST_FAILED")
                .contains("FAILED -> DECLINED by alice (reason kept,"
                        + " encrypted)")
                .contains("DECLINED -> FAILED by alice")
                .doesNotContain(REASON)
                .doesNotContain("New information");
    }

    /** A failure that isn't there can't be declined. */
    @Test
    void refusesAFailureThatIsNotThere()
    {
        Result declined = admin(REASON, "decline", "--item-id",
                item.toString(), "--seq", "7");
        assertThat(declined.status()).isEqualTo(1);
        assertThat(CHANGED).isEmpty();
    }

    /**
     * {@code correct} stores the corrected record, encrypted, and moves the
     * failure to CORRECTION_PENDING, printing nothing of it; one too long,
     * or a second while one is pending, is refused.
     */
    @Test
    void submitsACorrection()
    {
        String corrected = "<synthetic corrected record/>";
        Result submitted = admin(corrected, "correct", "--item-id",
                item.toString(), "--seq", "2");
        assertThat(submitted.status()).as(submitted.out() + submitted.err())
                .isZero();
        assertThat(submitted.out()).contains("submitted correction 1")
                .doesNotContain(corrected);
        assertThat(store().failure(item, 2)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.CORRECTION_PENDING);
        CorrectionStore corrections = new CorrectionStore(
                StageTestSupport.database(), schema, "test",
                Duration.ofSeconds(5));
        assertThat(corrections.pending(10)).singleElement().satisfies(
                pending ->
                {
                    assertThat(new String(pending.sealed(), UTF_8))
                            .doesNotContain("corrected");
                    assertThat(new String(pending.open(KEYS), UTF_8))
                            .isEqualTo(corrected);
                    assertThat(pending.submittedBy()).isEqualTo("alice");
                });
        assertThat(audit.attributes("admin.correct"))
                .containsEntry("sdh.correction", 1L)
                .containsEntry("sdh.result", "ok");

        assertThat(admin("again", "correct", "--item-id", item.toString(),
                "--seq", "2").status()).isEqualTo(1);
        assertThat(admin("x".repeat(65), "correct", "--item-id",
                item.toString(), "--seq", "2").status()).isEqualTo(2);
        Result history = admin("", "failures", "--item-id", item.toString());
        assertThat(history.out()).contains("correction 1 PENDING by alice")
                .doesNotContain(corrected);
    }

    /**
     * {@code export} prints the failed record encrypted to the key given,
     * which only its private key opens, and audits the key's fingerprint;
     * the plaintext is never printed. Without a key it is refused.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void exportsAFailureEncryptedToTheOperatorsKey() throws Exception
    {
        java.security.KeyPair operator = StageTestSupport.ec();
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + java.util.Base64.getMimeEncoder().encodeToString(
                        operator.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        Result exported = admin(pem, "export", "--item-id", item.toString(),
                "--seq", "2");
        assertThat(exported.status()).as(exported.out() + exported.err())
                .isZero();
        assertThat(exported.out()).doesNotContain(FAILED_CONTENT);
        String line = exported.out().lines()
                .filter(each -> each.startsWith("sdh-export: "))
                .findFirst().orElseThrow();
        com.nimbusds.jose.JWEObject jwe = com.nimbusds.jose.JWEObject.parse(
                line.substring("sdh-export: ".length()));
        jwe.decrypt(new com.nimbusds.jose.crypto.ECDHDecrypter(
                (java.security.interfaces.ECPrivateKey) operator
                        .getPrivate()));
        assertThat(jwe.getPayload().toString()).isEqualTo(FAILED_CONTENT);
        assertThat(audit.attributes("admin.export"))
                .containsKey("sdh.recipient_key")
                .containsEntry("sdh.result", "ok");

        assertThat(admin("", "export", "--item-id", item.toString(), "--seq",
                "2").status()).isEqualTo(2);
        assertThat(admin(pem, "export", "--item-id", item.toString(),
                "--seq", "9").status()).isEqualTo(1);
    }
}
