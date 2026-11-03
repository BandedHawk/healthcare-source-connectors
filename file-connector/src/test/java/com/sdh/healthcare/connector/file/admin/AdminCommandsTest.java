/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.core.crypto.RecoveryEnvelope;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.runtime.FatalExit;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
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
 * The admin commands (design §11.4) against data written by the running connector: embedded Postgres (lease and
 * JDBC catalog), in-memory object storage, local keys. The connector holds the lease throughout, which the
 * commands must neither need nor disturb.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false",
        classes = com.sdh.healthcare.connector.file.FileConnectorApplication.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class AdminCommandsTest {
    static final Path ROOT;
    static final String INTAKE = "claims";
    static final String LANDED_QUEUE = "seda:sdh-admin-landed?size=1000";
    /** The JDBC catalog's properties, for the landing stand-in. */
    static final Map<String, String> CATALOG = new LinkedHashMap<>();
    static final TestKeys KEYS;
    static final EmbeddedPostgres PG;
    /** The connector's settings, also given to the admin commands as --key=value overrides. */
    static final Map<String, String> CONFIG = new LinkedHashMap<>();
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    static final String GOOD = "1,alpha\n2,beta\n3,gamma\n";
    /** Seven sections, so at least one batch of five is committed before the unterminated quote fails the file. */
    static final String BAD = "1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n8,\"never closed\n9,h\n";

    static {
        try {
            ROOT = Files.createTempDirectory("sdh-admin");
            KEYS = new TestKeys(Files.createDirectories(ROOT.resolve("keys")));
            PG = EmbeddedPostgres.start();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
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
        CONFIG.put("sdh.landing.queue-uri", LANDED_QUEUE);
        CONFIG.put("sdh.landing.idle-wait", "100ms");
        CONFIG.put("sdh.split.format", "csv");
        CONFIG.put("sdh.storage.catalog-name", TestLanding.CATALOG);
        CATALOG.forEach((k, v) -> CONFIG.put("sdh.storage.catalog[" + k + "]", v));
        CONFIG.put("sdh.storage.commit.max-sections", "5");
        CONFIG.put("sdh.lease.jdbc.url", jdbc);
        CONFIG.put("sdh.lease.jdbc.username", "postgres");
        CONFIG.put("sdh.lease.jdbc.password", "postgres");
        CONFIG.put("sdh.crypto.keystore.path", KEYS.keystore.toString());
        CONFIG.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        CONFIG.put("sdh.crypto.keys.sections-kek", TestKeys.SECTIONS);
        CONFIG.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        CONFIG.put("sdh.crypto.keys.inbox.kid", TestKeys.INBOX_KID);
        CONFIG.put("sdh.crypto.keys.inbox.pem", KEYS.inboxPem.toString());
        CONFIG.put("sdh.crypto.keys.inbox.passphrase", TestKeys.INBOX_PASSPHRASE);
        CONFIG.put("sdh.crypto.keys.landing-signer.kid", TestKeys.LANDING_SIGNER_KID);
        CONFIG.put("sdh.crypto.keys.landing-signer.pem", KEYS.landingSignerPem.toString());
        CONFIG.put("sdh.crypto.keys.signing.kid", "connector-2026");
        CONFIG.put("sdh.crypto.keys.signing.pem", KEYS.signingPem.toString());
        CONFIG.put("sdh.crypto.keys.signing.passphrase", TestKeys.SIGNING_PASSPHRASE);
        CONFIG.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        CONFIG.put("sdh.crypto.keys.recipient.pem", KEYS.recipientPem.toString());
        try {
            CONFIG.put("sdh.crypto.keys.recipient.sha256", KeyMaterial.fingerprint(KEYS.recipient.getPublic()));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
        CONFIG.put("sdh.queue.type", "camel");
        CONFIG.put("sdh.queue.uri", "seda:sdh-admin-out?size=100000");
        CONFIG.put("sdh.retry.max-attempts", "2");
        CONFIG.put("sdh.retry.initial-backoff", "10ms");
    }

    @TestConfiguration
    static class Overrides {
        @Bean
        @Primary
        FatalExit recordingFatalExit() {
            return FATAL::add;
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        CONFIG.forEach((k, v) -> r.add(k, () -> v));
    }

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    CamelContext camel;

    UUID goodId;
    UUID badId;

    @BeforeAll
    void ingest() throws Exception {
        TestLanding landing = new TestLanding(CATALOG, runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem good = landing.land(GOOD, "good-smith.csv");
        LandedItem bad = landing.land(BAD, "bad-jones.csv");
        landing.announce(camel, LANDED_QUEUE, good);
        landing.announce(camel, LANDED_QUEUE, bad);
        MessageOpener opener = new MessageOpener(Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("connector-2026", (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<FileStarted> started = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline && (started.size() < 2 || runtime.inbox().backlog() > 0
                || runtime.processor().inProgress().isPresent())) {
            String body = consumer.receiveBody("seda:sdh-admin-out?size=100000", 200, String.class);
            if (body != null && opener.open(body.getBytes(UTF_8)) instanceof FileStarted s) {
                started.add(s);
            }
        }
        assertThat(FATAL).isEmpty();
        assertThat(started).extracting(FileStarted::fileId).containsExactly(good.itemId(), bad.itemId());
        assertThat(runtime.inbox().backlog()).isZero();
        goodId = good.itemId();
        badId = bad.itemId();
    }

    record Result(int status, String out, String err) {}

    static Result admin(Map<String, String> env, String... args) {
        List<String> all = new ArrayList<>(List.of(args));
        CONFIG.forEach((k, v) -> all.add("--" + k + "=" + v));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = AdminCli.run(all, new PrintStream(out, true, UTF_8), new PrintStream(err, true, UTF_8), env);
        return new Result(status, out.toString(UTF_8), err.toString(UTF_8));
    }

    static Result admin(String... args) {
        return admin(Map.of(AdminCli.OPERATOR_ENV, "alice"), args);
    }

    static Path newDir() throws Exception {
        return Files.createTempDirectory(ROOT, "out");
    }

    static List<String> names(Path dir) throws Exception {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void restoresAFailedFileWholeAndOnlyItsUnprocessedTail(CapturedOutput log) throws Exception {
        Path dir = newDir();
        Result whole = admin("restore", "--file-id", badId.toString(), "--to", dir.toString());
        assertThat(whole.status()).as(whole.err()).isZero();
        assertThat(whole.out()).contains("from landing, digest verified");
        assertThat(Files.readString(dir.resolve(badId.toString()))).isEqualTo(BAD);
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(dir.resolve(badId.toString()))))
                .isEqualTo("rw-------");

        // The tail starts right after the last stored section: at least one batch of five was committed.
        int from = (int) RestoreCommand.unprocessedFrom(runtime.tables().sections(), badId);
        assertThat(BAD.substring(0, from)).endsWith("\n");
        assertThat(BAD.substring(0, from).chars().filter(c -> c == '\n').count()).isGreaterThanOrEqualTo(5);
        Result tail = admin("restore", "--file-id", badId.toString(), "--to", dir.toString(), "--unprocessed");
        assertThat(tail.status()).as(tail.err()).isZero();
        assertThat(Files.readString(dir.resolve(badId + ".from-" + from))).isEqualTo(BAD.substring(from));

        // Never overwrites, and leaves no partial file behind.
        Result again = admin("restore", "--file-id", badId.toString(), "--to", dir.toString());
        assertThat(again.status()).isEqualTo(1);
        assertThat(again.err()).contains("already exists");
        assertThat(names(dir)).containsExactly(badId.toString(), badId + ".from-" + from);

        assertThat(log.getAll()).contains("admin.restore operator=alice file_id=" + badId)
                .contains("result=ok").contains("admin.restore operator=alice file_id=" + badId + " table=landed_items"
                        + " offset=0 result=failed reason=FileAlreadyExistsException");
    }

    @Test
    void restoresACompletedFileUnderItsOriginalNameWithoutPrintingIt() throws Exception {
        Path dir = newDir();
        Result r = admin("restore", "--file-id", goodId.toString(), "--to", dir.toString(), "--original-name");
        assertThat(r.status()).as(r.err()).isZero();
        assertThat(r.out()).contains("from landing").contains("under its original name")
                .doesNotContain("good-smith");
        assertThat(Files.readString(dir.resolve("good-smith.csv"))).isEqualTo(GOOD);

        Result partial = admin("restore", "--file-id", goodId.toString(), "--to", dir.toString(), "--from-offset",
                "8");
        assertThat(partial.status()).as(partial.err()).isZero();
        assertThat(Files.readString(dir.resolve(goodId + ".from-8"))).isEqualTo(GOOD.substring(8));

        // A completed item has no unprocessed tail.
        Result unprocessed = admin("restore", "--file-id", goodId.toString(), "--to", dir.toString(),
                "--unprocessed");
        assertThat(unprocessed.status()).as(unprocessed.err()).isZero();
        assertThat(Files.size(dir.resolve(goodId + ".from-" + GOOD.length()))).isZero();
    }

    @Test
    void statusShowsTheLeaseCheckpointAndBacklogWithoutTakingTheLease() throws Exception {
        Result r = admin("status");
        assertThat(r.status()).as(r.out() + r.err()).isZero();
        assertThat(r.out())
                .containsPattern("lease: +file-connector held by .+, epoch 1, renewed .+, expires in \\d+s")
                .containsPattern("checkpoint: +item " + badId + " FAILED \\(csv\\)")
                .contains("stage split, PARSE_UNTERMINATED_QUOTE")
                .containsPattern("inbox: +no items waiting");
        // Still ours: the connector renews the same epoch.
        assertThat(runtime.committer()).isNotNull();
        assertThat(admin("status").out()).contains("epoch 1,");
    }

    @Test
    void statusReportsUnreadablePartsAndFails() {
        Result r = admin("status", "--sdh.crypto.keystore.password=wrong");
        assertThat(r.status()).isEqualTo(1);
        assertThat(r.out()).containsPattern("lease: +file-connector held by")
                .containsPattern("checkpoint: +unavailable \\(")
                .containsPattern("inbox: +unavailable \\(");
    }

    @Test
    void decryptsALocalFallbackFile() throws Exception {
        UUID id = UUID.randomUUID();
        byte[] content = "1,x\n2,\"PHI\n".repeat(50_000).getBytes(UTF_8);
        Path enc = Files.createDirectories(ROOT.resolve("fallback")).resolve(id + ".enc");
        try (OutputStream o = Files.newOutputStream(enc)) {
            RecoveryEnvelope.encrypt(SeekableByteSource.of(content), id, Optional.of("orig-brown.csv"),
                    (ECPublicKey) KEYS.recovery.getPublic(), o);
        }
        Path key = TestKeys.encryptedPrivatePem(ROOT.resolve("keys/recovery-private.pem"),
                KEYS.recovery.getPrivate(), "recover-me");
        Map<String, String> env = Map.of(AdminCli.OPERATOR_ENV, "bob",
                DecryptFallbackCommand.PASSPHRASE_ENV, "recover-me");

        Path dir = newDir();
        Result r = admin(env, "decrypt-fallback", "--file", enc.toString(), "--key", key.toString(), "--to",
                dir.toString());
        assertThat(r.status()).as(r.err()).isZero();
        assertThat(Files.readAllBytes(dir.resolve(id.toString()))).isEqualTo(content);

        Path named = newDir();
        Result n = admin(env, "decrypt-fallback", "--file", enc.toString(), "--key", key.toString(), "--to",
                named.toString(), "--original-name");
        assertThat(n.status()).as(n.err()).isZero();
        assertThat(n.out()).doesNotContain("orig-brown");
        assertThat(names(named)).containsExactly("orig-brown.csv");

        // A damaged file fails integrity and leaves nothing behind.
        byte[] damaged = Files.readAllBytes(enc);
        damaged[damaged.length - 1] ^= 1;
        Path bad = enc.resolveSibling(id + "-damaged.enc");
        Files.write(bad, damaged);
        Path empty = newDir();
        Result d = admin(env, "decrypt-fallback", "--file", bad.toString(), "--key", key.toString(), "--to",
                empty.toString());
        assertThat(d.status()).isEqualTo(1);
        assertThat(names(empty)).isEmpty();

        // The wrong passphrase is refused before anything is written.
        Result w = admin(Map.of(AdminCli.OPERATOR_ENV, "bob", DecryptFallbackCommand.PASSPHRASE_ENV, "nope"),
                "decrypt-fallback", "--file", enc.toString(), "--key", key.toString(), "--to", empty.toString());
        assertThat(w.status()).isEqualTo(1);
        assertThat(names(empty)).isEmpty();
    }

    @Test
    void refusesToRunWithoutAnOperatorOrWithUnknownOptions(CapturedOutput log) throws Exception {
        Result noOperator = admin(Map.of(), "status");
        assertThat(noOperator.status()).isEqualTo(2);
        assertThat(noOperator.err()).contains("SDH_OPERATOR");
        // As an option instead, for kubectl exec into an image without env; it wins over the environment.
        Result byOption = admin(Map.of(AdminCli.OPERATOR_ENV, "alice"), "status", "--operator", "carol");
        assertThat(byOption.status()).as(byOption.err()).isZero();
        assertThat(log.getAll()).contains("admin.status operator=carol").doesNotContain("admin.status operator=alice");
        assertThat(admin(Map.of(), "status", "--operator", "not ok").status()).isEqualTo(2);

        Result typo = admin("restore", "--file-id", badId.toString(), "--to", newDir().toString(), "--form-offset",
                "3");
        assertThat(typo.status()).isEqualTo(2);
        assertThat(typo.err()).contains("Unknown option --form-offset");

        assertThat(admin("frobnicate").status()).isEqualTo(2);
        assertThat(admin("restore", "--file-id", "not-a-uuid", "--to", ROOT.toString()).err())
                .contains("must be a UUID");
        assertThat(admin("restore", "--file-id", UUID.randomUUID().toString(), "--to", ROOT.toString()).err())
                .contains("No fully landed item");
    }

    @Test
    void storedNamesCanNeverEscapeTheOutputDirectory() {
        assertThat(OutputFiles.safeName("../../etc/passwd")).isEqualTo("passwd");
        assertThat(OutputFiles.safeName("C:\\in\\x.csv")).isEqualTo("x.csv");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> OutputFiles.safeName("dir/.."))
                .isInstanceOf(IllegalStateException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> OutputFiles.safeName(".hidden"))
                .isInstanceOf(IllegalStateException.class);
    }
}
