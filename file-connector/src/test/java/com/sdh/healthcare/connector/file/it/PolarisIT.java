/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.INTAKE;
import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance.Report;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.file.admin.SplitAdmin;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The split stage on an Iceberg REST catalog, as the reference configuration
 * deploys it (design §8.1, §14): Apache Polaris with SeaweedFS storage,
 * plus Postgres for the lease. Covers what depends on the catalog: fencing
 * (§9.3) with two separate REST clients, ingest and downstream read-back,
 * archive restore, maintenance (§8.4) and the admin status command.
 * Polaris writes table metadata itself, with its own S3 credentials;
 * SeaweedFS has no STS, so the catalog is created with
 * {@code stsUnavailable} and clients use their own credentials.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = {"camel.main.run-controller=false",
        "sdh.split.autostart=false"})
class PolarisIT
{

    /** The bucket holding the warehouse. */
    static final String BUCKET = "sdh-warehouse";

    /** The S3 access key. */
    static final String ACCESS_KEY = "sdh-it-access";

    /** The S3 secret key. */
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";

    /** SeaweedFS's S3 identities. */
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":\
            [{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}"""
            .formatted(ACCESS_KEY, SECRET_KEY);

    /** Polaris's bootstrap client ID. */
    static final String CLIENT_ID = "root";

    /** Polaris's bootstrap client secret. */
    static final String CLIENT_SECRET = "polaris-it-secret";

    /**
     * The Polaris catalog, which Iceberg REST clients name as their
     * warehouse.
     */
    static final String POLARIS_CATALOG = "sdh_it";

    /** A marker in every record, which must never appear in storage. */
    static final String MARKER = "PHI-MARKER-9b2e";

    /** The network SeaweedFS and Polaris share. */
    static final Network NETWORK = Network.newNetwork();

    /** The key files. */
    static final TestKeys KEYS;

    /** The lease database. */
    static final PostgreSQLContainer POSTGRES;

    /** The object store. */
    static final GenericContainer<?> SEAWEED;

    /** The REST catalog. */
    static final GenericContainer<?> POLARIS;

    /** The reasons the app asked to exit, if any. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** Where the split stage's messages go. */
    static final String OUTPUT = "seda:sdh-polaris?size=100000";

    static
    {
        requireDocker();
        try
        {
            Path root = Files.createTempDirectory("sdh-polaris");
            KEYS = new TestKeys(Files.createDirectories(root.resolve("keys")));
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
        POSTGRES = new PostgreSQLContainer("postgres:18.4");
        SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withNetwork(NETWORK).withNetworkAliases("seaweed")
                .withCopyToContainer(Transferable.of(S3_CONFIG),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        "-ip.bind=0.0.0.0", "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        POLARIS = new GenericContainer<>("apache/polaris:1.8.0")
                .withNetwork(NETWORK)
                .withEnv("POLARIS_BOOTSTRAP_CREDENTIALS",
                        "POLARIS," + CLIENT_ID + "," + CLIENT_SECRET)
                .withEnv("polaris.realm-context.realms", "POLARIS")
                .withEnv("quarkus.otel.sdk.disabled", "true")
                // Polaris's own S3 access, for the metadata files it writes.
                .withEnv("AWS_REGION", "us-east-1")
                .withEnv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
                .withEnv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
                .withExposedPorts(8181, 8182)
                .waitingFor(Wait.forHttp("/q/health").forPort(8182)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        POSTGRES.start();
        SEAWEED.start();
        POLARIS.start();
        try (S3Client s3 = s3())
        {
            s3.createBucket(request -> request.bucket(BUCKET));
        }
        try
        {
            createCatalog();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The app's settings, which the admin command is given too. */
    static final Map<String, String> CONFIG = new LinkedHashMap<>();

    /** The checkpoint's table property. */
    static final String CHECKPOINT = "sdh.checkpoint.file-connector";

    /** The running connector. */
    @Autowired
    ConnectorRuntime runtime;

    /** The app's Camel context. */
    @Autowired
    CamelContext camel;


    /**
     * The object store's endpoint as this JVM reaches it.
     *
     * @return the URL
     */
    static String s3Endpoint()
    {
        return "http://" + SEAWEED.getHost() + ":"
                + SEAWEED.getMappedPort(8333);
    }

    /**
     * Polaris's Iceberg REST endpoint.
     *
     * @return the URL
     */
    static String polarisUri()
    {
        return "http://" + POLARIS.getHost() + ":"
                + POLARIS.getMappedPort(8181) + "/api/catalog";
    }

    /**
     * An S3 client for the object store.
     *
     * @return the client
     */
    static S3Client s3()
    {
        return S3Client.builder().endpointOverride(URI.create(s3Endpoint()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    /**
     * Creates the catalog on SeaweedFS through the management API, as an
     * administrator would.
     *
     * @throws Exception if a request fails or is refused
     */
    static void createCatalog() throws Exception
    {
        HttpClient http = HttpClient.newHttpClient();
        String base = "http://" + POLARIS.getHost() + ":"
                + POLARIS.getMappedPort(8181);
        String form = "grant_type=client_credentials&scope="
                + URLEncoder.encode("PRINCIPAL_ROLE:ALL", UTF_8)
                + "&client_id=" + CLIENT_ID + "&client_secret="
                + CLIENT_SECRET;
        HttpResponse<String> token = http.send(HttpRequest.newBuilder(
                        URI.create(base + "/api/catalog/v1/oauth/tokens"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(token.statusCode()).as(token.body()).isEqualTo(200);
        String bearer = "Bearer " + JSONObjectUtils.getString(
                JSONObjectUtils.parse(token.body()), "access_token");

        String location = "s3://" + BUCKET + "/polaris";
        Map<String, Object> storage = new LinkedHashMap<>();
        storage.put("storageType", "S3");
        storage.put("allowedLocations", List.of(location));
        storage.put("region", "us-east-1");
        // Clients (this JVM) reach SeaweedFS through the VM; Polaris reaches
        // it on the container network.
        storage.put("endpoint", s3Endpoint());
        storage.put("endpointInternal", "http://seaweed:8333");
        storage.put("pathStyleAccess", true);
        storage.put("stsUnavailable", true);
        Map<String, Object> catalog = Map.of("catalog", Map.of(
                "name", POLARIS_CATALOG, "type", "INTERNAL", "readOnly", false,
                "properties", Map.of("default-base-location", location),
                "storageConfigInfo", storage));
        send(http, bearer, "POST", base + "/api/management/v1/catalogs",
                catalog, 201);
        send(http, bearer, "PUT", base + "/api/management/v1/catalogs/"
                        + POLARIS_CATALOG
                        + "/catalog-roles/catalog_admin/grants",
                Map.of("grant", Map.of("type", "catalog",
                        "privilege", "CATALOG_MANAGE_CONTENT")), 201);
    }

    /**
     * Sends a JSON request to the management API and checks its status.
     *
     * @param http     the client
     * @param bearer   the Authorization header's value
     * @param method   the HTTP method
     * @param url      the URL
     * @param body     the request body, written as JSON
     * @param expected the status expected
     * @throws Exception if the request fails
     */
    static void send(HttpClient http, String bearer, String method,
            String url, Map<String, Object> body, int expected)
            throws Exception
    {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", bearer)
                        .header("Content-Type", "application/json")
                        .method(method, HttpRequest.BodyPublishers.ofString(
                                JSONObjectUtils.toJSONString(body)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .as(method + " " + url + ": " + response.body())
                .isEqualTo(expected);
    }

    /**
     * Iceberg REST client properties: OAuth client credentials, S3FileIO
     * with the client's own credentials.
     *
     * @return the properties
     */
    static Map<String, String> catalogProperties()
    {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "rest");
        properties.put("uri", polarisUri());
        properties.put("warehouse", POLARIS_CATALOG);
        // Iceberg 1.12 warns when the auth type is only inferred from
        // `credential`.
        properties.put("rest.auth.type", "oauth2");
        properties.put("credential", CLIENT_ID + ":" + CLIENT_SECRET);
        properties.put("scope", "PRINCIPAL_ROLE:ALL");
        properties.put("oauth2-server-uri", polarisUri() + "/v1/oauth/tokens");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", s3Endpoint());
        properties.put("s3.path-style-access", "true");
        properties.put("s3.access-key-id", ACCESS_KEY);
        properties.put("s3.secret-access-key", SECRET_KEY);
        properties.put("client.region", "us-east-1");
        return properties;
    }

    /**
     * A separate REST client, as another process would have.
     *
     * @return the client
     */
    static Catalog newClient()
    {
        return CatalogUtil.buildIcebergCatalog("sdh-" + UUID.randomUUID(),
                catalogProperties(), null);
    }

    /**
     * Beans replaced for the test.
     */
    @TestConfiguration
    static class Overrides
    {

        /**
         * Records fatal exits instead of exiting.
         *
         * @return the recording exit
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Points the app at the containers, keeping the settings for the admin
     * command.
     *
     * @param registry receives the properties
     * @throws Exception if a key can't be read
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        CONFIG.putAll(ItSupport.splitStage(KEYS, catalogProperties(),
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword(), OUTPUT));
        CONFIG.forEach((key, value) -> registry.add(key, () -> value));
    }

    /**
     * Two files are landed and split; downstream reads every split record
     * back with its own REST client, the admin status command reads the
     * items' rows, maintenance runs through the REST catalog, and nothing
     * stored is readable.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void ingestsReadsBackAndMaintainsTablesOnPolaris() throws Exception
    {
        String header = "id,name,note\n";
        List<String> rows = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int index = 0; index < 30; index++)
        {
            String row = index + "," + MARKER + "-" + index
                    + ",\"multi\nline\"\n";
            rows.add(row);
            file.append(row);
        }
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem good = landing.land(file.toString(), "good.csv");
        LandedItem bad = landing.land(
                header + "1," + MARKER + ",\"never closed\n", "bad.csv");
        ItSupport.split(runtime, landing, FATAL, good, bad);
        assertThat(FATAL).isEmpty();

        // Downstream, with its own REST client.
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", KEYS.recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) KEYS.signing.getPublic())
                .catalog(newClient())
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build())
        {
            List<ConnectorMessage> messages =
                    ItSupport.drain(camel, OUTPUT, reader);
            List<RecordSplit> records = ItSupport.of(messages,
                    RecordSplit.class);
            ItSupport.assertGoodSplitBadOpen(messages, good.itemId(),
                    bad.itemId());
            assertThat(records).hasSize(30);
            ItSupport.assertSplitRecords(reader, records, header, rows);
            assertThat(new String(landing.restore(good.itemId()), UTF_8))
                    .isEqualTo(file.toString());

            // Admin status reads the item rows, while the stage runs.
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            List<String> args = new ArrayList<>(List.of("status"));
            CONFIG.forEach((key, value) -> args.add("--" + key + "=" + value));
            int status = SplitAdmin.cli().run(args,
                    new PrintStream(out, true, UTF_8),
                    new PrintStream(out, true, UTF_8),
                    Map.of(AdminCli.OPERATOR_ENV, "polaris-it"));
            assertThat(status).as(out.toString(UTF_8)).isZero();
            assertThat(out.toString(UTF_8))
                    .containsPattern("items: +.*1 FAILED");

            // Maintenance through the REST catalog, between files.
            {
                String dataPrefix =
                        keyOf(runtime.tables().splitRecords().location())
                                + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                try (S3Client s3 = s3())
                {
                    s3.putObject(request -> request.bucket(BUCKET).key(stray),
                            RequestBody.fromString("half-written"));
                }
                // Object timestamps come from the storage server's clock (the
                // minikube VM's), which can run a little ahead of this JVM's;
                // a minute's lead keeps the just-planted stray old enough to
                // count as an orphan.
                Report report = new TableMaintenance(ItSupport.committer(),
                        new MaintenanceSettings(Duration.ofDays(7), 10, true,
                                0.75, 2, 8L << 30, Duration.ZERO),
                        Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1)))
                        .run(runtime.tables().splitRecords(),
                                RecordsTable.INGESTED_AT, Optional.empty(),
                                true);
                assertThat(report.compactedFiles())
                        .as("30 records in batches of 7").isEqualTo(5);
                assertThat(report.orphansDeleted()).isEqualTo(1);
                assertThat(keys(dataPrefix)).doesNotContain(stray);
                ItSupport.assertSplitRecords(reader, records, header, rows);
                Report purge = new TableMaintenance(ItSupport.committer(),
                        MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.tables().splitRecords(),
                                RecordsTable.INGESTED_AT,
                                Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(keys(dataPrefix))
                        .as("PHI data objects physically removed").isEmpty();
            }
        }

        // Nothing Polaris or the connector wrote is readable: data,
        // manifests and metadata alike.
        try (S3Client s3 = s3())
        {
            List<S3Object> objects = s3.listObjectsV2Paginator(
                    request -> request.bucket(BUCKET)).contents().stream()
                    .toList();
            assertThat(objects).isNotEmpty();
            for (S3Object object : objects)
            {
                String content = new String(s3.getObjectAsBytes(request ->
                        request.bucket(BUCKET).key(object.key()))
                        .asByteArray(), ISO_8859_1);
                assertThat(content).as(object.key()).doesNotContain(MARKER)
                        .doesNotContain("good.csv")
                        .doesNotContain("bad.csv");
            }
        }
    }

    // ---- fencing (design §9.3) against the real server, as
    // FencedCommitterTest does in-process ----

    /**
     * A lease token that always believes it is held.
     *
     * @param epoch the lease epoch
     */
    record Token(long epoch) implements LeaseToken
    {

        /**
         * Does nothing: a stale instance that believes it still holds the
         * lease (e.g. resumed after a long pause).
         */
        @Override
        public void checkHeld()
        {
            // A stale instance that believes it still holds the lease (e.g.
            // resumed after a long pause).
        }
    }

    /**
     * The same table through two REST clients, as two processes would see
     * it.
     *
     * @return the table as each process sees it
     */
    static Table[] twoProcesses()
    {
        String namespace =
                "fence_" + UUID.randomUUID().toString().replace("-", "");
        Table first = IngestTables.records(newClient(), namespace);
        Table second = newClient().loadTable(
                TableIdentifier.of(namespace, RecordsTable.DEFAULT_NAME));
        return new Table[] {first, second};
    }

    /**
     * A checkpoint that only changes properties still commits, as a marker
     * snapshot.
     */
    @Test
    void propertiesOnlyCheckpointCommitsAsAMarkerSnapshot()
    {
        Table table = twoProcesses()[0];
        FencedCommitter holder = new FencedCommitter(new Token(1));
        holder.establish(table);
        int before = size(table);
        holder.commit(table, transaction -> transaction.updateProperties()
                .set(CHECKPOINT, "seq=10").commit());
        table.refresh();
        assertThat(table.properties()).containsEntry(CHECKPOINT, "seq=10")
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "1");
        assertThat(size(table)).as("marker snapshot added")
                .isEqualTo(before + 1);
    }

    /**
     * Once a successor establishes, the stale holder's commit is fenced.
     */
    @Test
    void staleHolderIsFencedAfterSuccessorEstablishes()
    {
        Table[] tables = twoProcesses();
        FencedCommitter holder = new FencedCommitter(new Token(1));
        holder.establish(tables[0]);
        new FencedCommitter(new Token(2)).establish(tables[1]);
        assertThatThrownBy(() -> holder.commit(tables[0], transaction ->
                        transaction.updateProperties().set(CHECKPOINT, "A")
                                .commit()))
                .isInstanceOf(LeaseLostException.class)
                .extracting(thrown -> ((LeaseLostException) thrown).reason())
                .isEqualTo(LeaseLostException.Reason.FENCED);
        tables[1].refresh();
        assertThat(tables[1].properties()).doesNotContainKey(CHECKPOINT);
    }

    /**
     * The successor establishes after the stale holder built its change,
     * before it commits.
     */
    @Test
    void raceBetweenBuildAndCommitIsFencedByTheServer()
    {
        Table[] tables = twoProcesses();
        FencedCommitter holder = new FencedCommitter(new Token(1));
        holder.establish(tables[0]);
        FencedCommitter successor = new FencedCommitter(new Token(2));
        AtomicBoolean once = new AtomicBoolean();
        assertThatThrownBy(() -> holder.commit(tables[0], transaction ->
                {
                    transaction.updateProperties().set(CHECKPOINT, "A")
                            .commit();
                    if (once.compareAndSet(false, true))
                    {
                        successor.establish(tables[1]);
                    }
                }))
                .isInstanceOf(LeaseLostException.class);
        tables[1].refresh();
        assertThat(tables[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .doesNotContainKey(CHECKPOINT);
    }

    /**
     * Control: a plain Iceberg transaction from the stale holder lands on
     * the real server too.
     */
    @Test
    void plainIcebergTransactionIsNotFenced()
    {
        Table[] tables = twoProcesses();
        new FencedCommitter(new Token(1)).establish(tables[0]);
        tables[0].refresh();
        Transaction stale = tables[0].newTransaction();
        stale.updateProperties().set(CHECKPOINT, "A").commit();
        stale.newFastAppend().commit();
        new FencedCommitter(new Token(2)).establish(tables[1]);
        stale.commitTransaction();
        tables[1].refresh();
        assertThat(tables[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .containsEntry(CHECKPOINT, "A");
    }

    /**
     * How many snapshots a table has.
     *
     * @param table the table
     * @return the number of snapshots
     */
    static int size(Table table)
    {
        int count = 0;
        for (Snapshot ignored : table.snapshots())
        {
            count++;
        }
        return count;
    }

    /**
     * An object's key from its location: "s3://bucket/a/b" → "a/b".
     *
     * @param location the object's location
     * @return its key
     */
    static String keyOf(String location)
    {
        String noScheme = location.substring(location.indexOf("://") + 3);
        return noScheme.substring(noScheme.indexOf('/') + 1);
    }

    /**
     * The keys of every object under a prefix.
     *
     * @param prefix the prefix
     * @return the keys
     */
    static List<String> keys(String prefix)
    {
        try (S3Client s3 = s3())
        {
            return s3.listObjectsV2Paginator(request -> request.bucket(BUCKET)
                            .prefix(prefix))
                    .contents().stream()
                    .map(S3Object::key).toList();
        }
    }
}
