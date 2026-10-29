package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.storage.ArchiveReader;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.admin.AdminCli;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.runtime.FatalExit;
import com.sdh.healthcare.connector.reader.QueueReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The connector on an Iceberg REST catalog, as the reference configuration deploys it (design §8.1, §14): Apache
 * Polaris with SeaweedFS storage, plus Postgres for the lease. Covers what depends on the catalog: fencing (§9.3)
 * with two separate REST clients, ingest and downstream read-back, archive restore, maintenance (§8.4) and the
 * admin status command. Polaris writes table metadata itself, with its own S3 credentials; SeaweedFS has no STS,
 * so the catalog is created with {@code stsUnavailable} and clients use their own credentials.
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class PolarisIT {
    static final String BUCKET = "sdh-warehouse";
    static final String ACCESS_KEY = "sdh-it-access";
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);
    static final String CLIENT_ID = "root";
    static final String CLIENT_SECRET = "polaris-it-secret";
    /** The Polaris catalog, which Iceberg REST clients name as their warehouse. */
    static final String POLARIS_CATALOG = "sdh_it";
    static final String MARKER = "PHI-MARKER-9b2e";

    static final Network NETWORK = Network.newNetwork();
    static final Path INBOUND;
    static final TestKeys KEYS;
    static final PostgreSQLContainer POSTGRES;
    static final GenericContainer<?> SEAWEED;
    static final GenericContainer<?> POLARIS;
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    static {
        ItSupport.requireDocker();
        try {
            Path root = Files.createTempDirectory("sdh-polaris");
            INBOUND = Files.createDirectories(root.resolve("inbound"));
            KEYS = new TestKeys(Files.createDirectories(root.resolve("keys")));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
        POSTGRES = new PostgreSQLContainer("postgres:18.4");
        SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withNetwork(NETWORK).withNetworkAliases("seaweed")
                .withCopyToContainer(Transferable.of(S3_CONFIG), "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-ip.bind=0.0.0.0",
                        "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        POLARIS = new GenericContainer<>("apache/polaris:1.8.0")
                .withNetwork(NETWORK)
                .withEnv("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS," + CLIENT_ID + "," + CLIENT_SECRET)
                .withEnv("polaris.realm-context.realms", "POLARIS")
                .withEnv("quarkus.otel.sdk.disabled", "true")
                // Polaris's own S3 access, for the metadata files it writes.
                .withEnv("AWS_REGION", "us-east-1")
                .withEnv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
                .withEnv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
                .withExposedPorts(8181, 8182)
                .waitingFor(Wait.forHttp("/q/health").forPort(8182).withStartupTimeout(Duration.ofMinutes(2)));
        POSTGRES.start();
        SEAWEED.start();
        POLARIS.start();
        try (S3Client s3 = s3()) {
            s3.createBucket(b -> b.bucket(BUCKET));
        }
        try {
            createCatalog();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static String s3Endpoint() {
        return "http://" + SEAWEED.getHost() + ":" + SEAWEED.getMappedPort(8333);
    }

    static String polarisUri() {
        return "http://" + POLARIS.getHost() + ":" + POLARIS.getMappedPort(8181) + "/api/catalog";
    }

    static S3Client s3() {
        return S3Client.builder().endpointOverride(URI.create(s3Endpoint())).region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    /** Creates the catalog on SeaweedFS through the management API, as an administrator would. */
    static void createCatalog() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        String base = "http://" + POLARIS.getHost() + ":" + POLARIS.getMappedPort(8181);
        String form = "grant_type=client_credentials&scope=" + URLEncoder.encode("PRINCIPAL_ROLE:ALL", UTF_8)
                + "&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET;
        HttpResponse<String> token = http.send(HttpRequest.newBuilder(URI.create(base + "/api/catalog/v1/oauth/tokens"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(token.statusCode()).as(token.body()).isEqualTo(200);
        String bearer = "Bearer " + JSONObjectUtils.getString(JSONObjectUtils.parse(token.body()), "access_token");

        String location = "s3://" + BUCKET + "/polaris";
        Map<String, Object> storage = new LinkedHashMap<>();
        storage.put("storageType", "S3");
        storage.put("allowedLocations", List.of(location));
        storage.put("region", "us-east-1");
        // Clients (this JVM) reach SeaweedFS through the VM; Polaris reaches it on the container network.
        storage.put("endpoint", s3Endpoint());
        storage.put("endpointInternal", "http://seaweed:8333");
        storage.put("pathStyleAccess", true);
        storage.put("stsUnavailable", true);
        Map<String, Object> catalog = Map.of("catalog", Map.of(
                "name", POLARIS_CATALOG, "type", "INTERNAL", "readOnly", false,
                "properties", Map.of("default-base-location", location),
                "storageConfigInfo", storage));
        send(http, bearer, "POST", base + "/api/management/v1/catalogs", catalog, 201);
        send(http, bearer, "PUT", base + "/api/management/v1/catalogs/" + POLARIS_CATALOG
                        + "/catalog-roles/catalog_admin/grants",
                Map.of("grant", Map.of("type", "catalog", "privilege", "CATALOG_MANAGE_CONTENT")), 201);
    }

    static void send(HttpClient http, String bearer, String method, String url, Map<String, Object> body,
            int expected) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", bearer).header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(JSONObjectUtils.toJSONString(body))).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).as(method + " " + url + ": " + r.body()).isEqualTo(expected);
    }

    /** Iceberg REST client properties: OAuth client credentials, S3FileIO with the client's own credentials. */
    static Map<String, String> catalogProps() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("type", "rest");
        m.put("uri", polarisUri());
        m.put("warehouse", POLARIS_CATALOG);
        // Iceberg 1.12 warns when the auth type is only inferred from `credential`.
        m.put("rest.auth.type", "oauth2");
        m.put("credential", CLIENT_ID + ":" + CLIENT_SECRET);
        m.put("scope", "PRINCIPAL_ROLE:ALL");
        m.put("oauth2-server-uri", polarisUri() + "/v1/oauth/tokens");
        m.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        m.put("s3.endpoint", s3Endpoint());
        m.put("s3.path-style-access", "true");
        m.put("s3.access-key-id", ACCESS_KEY);
        m.put("s3.secret-access-key", SECRET_KEY);
        m.put("client.region", "us-east-1");
        return m;
    }

    /** A separate REST client, as another process would have. */
    static Catalog newClient() {
        return CatalogUtil.buildIcebergCatalog("sdh-" + UUID.randomUUID(), catalogProps(), null);
    }

    @TestConfiguration
    static class Overrides {
        @Bean
        @Primary
        FatalExit recordingFatalExit() {
            return FATAL::add;
        }
    }

    static final Map<String, String> CONFIG = new LinkedHashMap<>();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        CONFIG.put("sdh.file.inbound-dir", INBOUND.toString());
        CONFIG.put("sdh.file.min-age", "300ms");
        CONFIG.put("sdh.file.poll-interval", "200ms");
        CONFIG.put("sdh.split.format", "csv");
        CONFIG.put("sdh.split.options.csv.header", "true");
        CONFIG.put("sdh.storage.catalog-name", "sdh");
        catalogProps().forEach((k, v) -> CONFIG.put("sdh.storage.catalog[" + k + "]", v));
        CONFIG.put("sdh.storage.commit.max-sections", "7");
        CONFIG.put("sdh.lease.jdbc.url", POSTGRES.getJdbcUrl());
        CONFIG.put("sdh.lease.jdbc.username", POSTGRES.getUsername());
        CONFIG.put("sdh.lease.jdbc.password", POSTGRES.getPassword());
        CONFIG.put("sdh.crypto.keystore.path", KEYS.keystore.toString());
        CONFIG.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        CONFIG.put("sdh.crypto.keys.sections-kek", TestKeys.SECTIONS);
        CONFIG.put("sdh.crypto.keys.source-files-kek", TestKeys.SOURCE);
        CONFIG.put("sdh.crypto.keys.failed-files-kek", TestKeys.FAILED);
        CONFIG.put("sdh.crypto.keys.signing.kid", "connector-2026");
        CONFIG.put("sdh.crypto.keys.signing.pem", KEYS.signingPem.toString());
        CONFIG.put("sdh.crypto.keys.signing.passphrase", TestKeys.SIGNING_PASSPHRASE);
        CONFIG.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        CONFIG.put("sdh.crypto.keys.recipient.pem", KEYS.recipientPem.toString());
        CONFIG.put("sdh.crypto.keys.recipient.sha256", KeyMaterial.fingerprint(KEYS.recipient.getPublic()));
        CONFIG.put("sdh.crypto.keys.recovery-public", KEYS.recoveryPem.toString());
        CONFIG.put("sdh.queue.type", "camel");
        CONFIG.put("sdh.queue.uri", "seda:sdh-polaris?size=100000");
        CONFIG.forEach((k, v) -> r.add(k, () -> v));
    }

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    CamelContext camel;

    @Autowired
    ReentrantLock writerLock;

    @Test
    void ingestsReadsBackAndMaintainsTablesOnPolaris() throws Exception {
        String header = "id,name,note\n";
        List<String> records = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int i = 0; i < 30; i++) {
            String rec = i + "," + MARKER + "-" + i + ",\"multi\nline\"\n";
            records.add(rec);
            file.append(rec);
        }
        Path good = INBOUND.resolve("good.csv");
        Files.writeString(good, file);
        Files.setLastModifiedTime(good, FileTime.from(Instant.now().minusSeconds(30)));
        Path bad = INBOUND.resolve("bad.csv");
        Files.writeString(bad, header + "1," + MARKER + ",\"never closed\n");

        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline && (Files.exists(good) || Files.exists(bad))) {
            Thread.sleep(250);
        }
        assertThat(FATAL).isEmpty();
        assertThat(good).doesNotExist();
        assertThat(bad).doesNotExist();

        // Downstream, with its own REST client.
        List<ConnectorMessage> msgs = new ArrayList<>();
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body;
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", KEYS.recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) KEYS.signing.getPublic())
                .catalog(newClient())
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build()) {
            while ((body = consumer.receiveBody("seda:sdh-polaris", 1000, String.class)) != null) {
                msgs.add(reader.decode(body.getBytes(UTF_8)));
            }
            List<SectionStored> sections = msgs.stream().filter(SectionStored.class::isInstance)
                    .map(SectionStored.class::cast).toList();
            assertThat(msgs.stream().filter(FileStarted.class::isInstance)).hasSize(2);
            assertThat(sections).hasSize(30);
            for (SectionStored s : sections) {
                assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                        .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
            }
            FileCompleted done = msgs.stream().filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).findFirst().orElseThrow();
            ByteArrayOutputStream restored = new ByteArrayOutputStream();
            ArchiveReader.restore(runtime.sourceFiles(), done.fileId(), runtime.keys().wrap(), runtime.ids(), 0,
                    restored);
            assertThat(restored.toString(UTF_8)).isEqualTo(file.toString());

            // Admin status reads the checkpoint through its own REST client, while the connector holds the lease.
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            List<String> args = new ArrayList<>(List.of("status"));
            CONFIG.forEach((k, v) -> args.add("--" + k + "=" + v));
            int status = AdminCli.run(args, new PrintStream(out, true, UTF_8), new PrintStream(out, true, UTF_8),
                    Map.of(AdminCli.OPERATOR_ENV, "polaris-it"));
            assertThat(status).as(out.toString(UTF_8)).isZero();
            assertThat(out.toString(UTF_8)).containsPattern("checkpoint: +file .+ FAILED \\(csv\\)")
                    .containsPattern("archive source_files: +last file " + done.fileId() + " finished");

            // Maintenance through the REST catalog, between files.
            writerLock.lock();
            try {
                String dataPrefix = keyOf(runtime.sections().location()) + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                try (S3Client s3 = s3()) {
                    s3.putObject(b -> b.bucket(BUCKET).key(stray),
                            software.amazon.awssdk.core.sync.RequestBody.fromString("half-written"));
                }
                // Object timestamps come from the storage server's clock (the minikube VM's), which can run a little
                // ahead of this JVM's; a minute's lead keeps the just-planted stray old enough to count as an orphan.
                var report = new TableMaintenance(runtime.committer(), new MaintenanceSettings(Duration.ofDays(7), 10,
                        true, 0.75, 2, 8L << 30, Duration.ZERO), Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1)))
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.empty(), true);
                assertThat(report.compactedFiles()).as("30 sections in batches of 7").isEqualTo(5);
                assertThat(report.orphansDeleted()).isEqualTo(1);
                assertThat(keys(dataPrefix)).doesNotContain(stray);
                for (SectionStored s : sections) {
                    assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                            .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
                }
                var purge = new TableMaintenance(runtime.committer(), MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(keys(dataPrefix)).as("PHI data objects physically removed").isEmpty();
            } finally {
                writerLock.unlock();
            }
        }

        // Nothing Polaris or the connector wrote is readable: data, manifests and metadata alike.
        try (S3Client s3 = s3()) {
            List<S3Object> objects = s3.listObjectsV2Paginator(b -> b.bucket(BUCKET)).contents().stream().toList();
            assertThat(objects).isNotEmpty();
            for (S3Object o : objects) {
                String content = new String(s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(o.key())).asByteArray(),
                        java.nio.charset.StandardCharsets.ISO_8859_1);
                assertThat(content).as(o.key()).doesNotContain(MARKER).doesNotContain("good.csv")
                        .doesNotContain("bad.csv");
            }
        }
    }

    // ---- fencing (design §9.3) against the real server, as FencedCommitterTest does in-process ----

    record Token(long epoch) implements LeaseToken {
        @Override
        public void checkHeld() {
            // A stale instance that believes it still holds the lease (e.g. resumed after a long pause).
        }
    }

    static final String CHECKPOINT = "sdh.checkpoint.file-connector";

    /** The same table through two REST clients, as two processes would see it. */
    static Table[] twoProcesses() {
        String ns = "fence_" + UUID.randomUUID().toString().replace("-", "");
        Table a = IngestTables.sections(newClient(), ns);
        Table b = newClient().loadTable(TableIdentifier.of(ns, SectionsTable.DEFAULT_NAME));
        return new Table[] {a, b};
    }

    @Test
    void propertiesOnlyCheckpointCommitsAsAMarkerSnapshot() {
        Table a = twoProcesses()[0];
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(a);
        int before = size(a);
        fa.commit(a, tx -> tx.updateProperties().set(CHECKPOINT, "seq=10").commit());
        a.refresh();
        assertThat(a.properties()).containsEntry(CHECKPOINT, "seq=10").containsEntry(FencedCommitter.EPOCH_PROPERTY, "1");
        assertThat(size(a)).as("marker snapshot added").isEqualTo(before + 1);
    }

    @Test
    void staleHolderIsFencedAfterSuccessorEstablishes() {
        Table[] t = twoProcesses();
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(t[0]);
        new FencedCommitter(new Token(2)).establish(t[1]);
        assertThatThrownBy(() -> fa.commit(t[0], tx -> tx.updateProperties().set(CHECKPOINT, "A").commit()))
                .isInstanceOf(LeaseLostException.class)
                .extracting(e -> ((LeaseLostException) e).reason())
                .isEqualTo(LeaseLostException.Reason.FENCED);
        t[1].refresh();
        assertThat(t[1].properties()).doesNotContainKey(CHECKPOINT);
    }

    /** The successor establishes after the stale holder built its change, before it commits. */
    @Test
    void raceBetweenBuildAndCommitIsFencedByTheServer() {
        Table[] t = twoProcesses();
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(t[0]);
        FencedCommitter fb = new FencedCommitter(new Token(2));
        AtomicBoolean once = new AtomicBoolean();
        assertThatThrownBy(() -> fa.commit(t[0], tx -> {
                    tx.updateProperties().set(CHECKPOINT, "A").commit();
                    if (once.compareAndSet(false, true)) {
                        fb.establish(t[1]);
                    }
                }))
                .isInstanceOf(LeaseLostException.class);
        t[1].refresh();
        assertThat(t[1].properties()).containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .doesNotContainKey(CHECKPOINT);
    }

    /** Control: a plain Iceberg transaction from the stale holder lands on the real server too. */
    @Test
    void plainIcebergTransactionIsNotFenced() {
        Table[] t = twoProcesses();
        new FencedCommitter(new Token(1)).establish(t[0]);
        t[0].refresh();
        Transaction stale = t[0].newTransaction();
        stale.updateProperties().set(CHECKPOINT, "A").commit();
        stale.newFastAppend().commit();
        new FencedCommitter(new Token(2)).establish(t[1]);
        stale.commitTransaction();
        t[1].refresh();
        assertThat(t[1].properties()).containsEntry(FencedCommitter.EPOCH_PROPERTY, "2").containsEntry(CHECKPOINT, "A");
    }

    static int size(Table t) {
        int n = 0;
        for (var ignored : t.snapshots()) {
            n++;
        }
        return n;
    }

    /** "s3://bucket/a/b" → "a/b". */
    static String keyOf(String location) {
        String noScheme = location.substring(location.indexOf("://") + 3);
        return noScheme.substring(noScheme.indexOf('/') + 1);
    }

    static List<String> keys(String prefix) {
        try (S3Client s3 = s3()) {
            return s3.listObjectsV2Paginator(b -> b.bucket(BUCKET).prefix(prefix)).contents().stream()
                    .map(S3Object::key).toList();
        }
    }
}
