package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.AccessMode;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Volume;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.reader.QueueReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The image as deployed (design §7.5, §14.1): built from {@code file-connector/Dockerfile} and the packaged jar, run
 * with a read-only root filesystem, a noexec /tmp, all capabilities dropped, no-new-privileges and a memory limit.
 * Secrets come from a volume shaped like a Kubernetes secret with an fsGroup (root:10001, 0440), and the inbound
 * directory is a volume. It ingests a good and a bad CSV file through Postgres, SeaweedFS and Kafka, all reached by
 * network alias, and a downstream reader reads every section back.
 */
class ContainerImageIT {
    static final String MARKER = "PHI-MARKER-c0e1";
    static final String BUCKET = "sdh-warehouse";
    static final String ACCESS_KEY = "sdh-it-access";
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);
    static final String IMAGE = "sdh/file-connector:it";
    static final String APP_UID = "10001";

    static Network network;
    static PostgreSQLContainer postgres;
    static GenericContainer<?> seaweed;
    static KafkaContainer kafka;
    static GenericContainer<?> app;
    /** Stands in for whatever delivers files (SFTP server, sync job): shares the inbound volume with the app. */
    static GenericContainer<?> upstream;
    static final ToStringConsumer APP_LOG = new ToStringConsumer();
    static final List<String> VOLUMES = new ArrayList<>();
    static TestKeys keys;
    static UUID goodId;
    static UUID badId;

    @BeforeAll
    static void start() throws Exception {
        ItSupport.requireDocker();
        keys = new TestKeys(Files.createTempDirectory("sdh-image-it"));
        network = Network.newNetwork();
        postgres = new PostgreSQLContainer("postgres:18.4").withNetwork(network).withNetworkAliases("postgres");
        seaweed = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withNetwork(network).withNetworkAliases("seaweed")
                .withCopyToContainer(Transferable.of(S3_CONFIG), "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-ip.bind=0.0.0.0",
                        "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        kafka = new KafkaContainer("apache/kafka:4.2.2").withNetwork(network).withListener("kafka:19092");
        Stream.of(postgres, seaweed, kafka).parallel().forEach(GenericContainer::start);
        try (S3Client s3 = s3()) {
            s3.createBucket(b -> b.bucket(BUCKET));
        }

        String secrets = volume("secrets");
        String inbound = volume("inbound");
        prepareVolumes(secrets, inbound);
        upstream = new GenericContainer<>("busybox:1.37.0")
                .withCommand("sleep", "3600")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withBinds(new Bind(inbound, new Volume("/data/inbound"))));
        upstream.start();

        app = new GenericContainer<>(image())
                .withNetwork(network)
                .withEnv("SPRING_APPLICATION_JSON", JSONObjectUtils.toJSONString(appConfig()))
                .withEnv("SDH_CRYPTO_KEYSTORE_PASSWORD", TestKeys.KS_PASSWORD)
                .withEnv("SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE", TestKeys.SIGNING_PASSPHRASE)
                .withEnv("JDK_JAVA_OPTIONS", "-Xss1m")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withReadonlyRootfs(true)
                        .withTmpFs(Map.of("/tmp", "rw,noexec,nosuid,nodev,size=256m"))
                        .withCapDrop(Capability.ALL)
                        .withSecurityOpts(List.of("no-new-privileges"))
                        .withMemory(1536L << 20)
                        .withBinds(new Bind(secrets, new Volume("/run/secrets"), AccessMode.ro),
                                new Bind(inbound, new Volume("/data/inbound"))))
                .withLogConsumer(APP_LOG)
                .waitingFor(Wait.forLogMessage(".*Started FileConnectorApplication.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        app.start();
    }

    @AfterAll
    static void stop() throws Exception {
        // Kept for diagnosis: the console output is PHI-free by design, and this test asserts it.
        Files.writeString(Path.of("target", "container-image-it.log"), APP_LOG.toUtf8String());
        Stream.of(app, upstream, kafka, seaweed, postgres).filter(c -> c != null).forEach(GenericContainer::stop);
        DockerClient docker = DockerClientFactory.lazyClient();
        for (String v : VOLUMES) {
            try {
                docker.removeVolumeCmd(v).exec();
            } catch (RuntimeException ignored) {
                // Ryuk removes it with the session's other resources.
            }
        }
        if (network != null) {
            network.close();
        }
    }

    /** Built from the Dockerfile and the jar Maven just packaged; the tag keeps Docker's layer cache between runs. */
    static ImageFromDockerfile image() throws Exception {
        Path jar;
        try (Stream<Path> s = Files.list(Path.of("target"))) {
            jar = s.filter(p -> p.getFileName().toString().matches("file-connector-.*\\.jar")).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Package file-connector first (mvn -Pit verify)"));
        }
        return new ImageFromDockerfile(IMAGE, false)
                .withFileFromPath("Dockerfile", Path.of("Dockerfile"))
                .withFileFromPath("docker", Path.of("docker"))
                .withFileFromPath("app.jar", jar)
                .withBuildArg("JAR_FILE", "app.jar");
    }

    /** A labelled named volume, so Ryuk cleans it up if the test dies. */
    static String volume(String purpose) {
        String name = "sdh-image-it-" + purpose + "-" + UUID.randomUUID();
        Map<String, String> labels = new HashMap<>(DockerClientFactory.DEFAULT_LABELS);
        labels.put(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL, DockerClientFactory.SESSION_ID);
        DockerClientFactory.lazyClient().createVolumeCmd().withName(name).withLabels(labels).exec();
        VOLUMES.add(name);
        return name;
    }

    /**
     * Fills the volumes as Kubernetes would: secret files root-owned, group 10001 (the fsGroup), mode 0440; the
     * inbound directory owned by the app user.
     */
    static void prepareVolumes(String secrets, String inbound) throws Exception {
        try (GenericContainer<?> init = new GenericContainer<>("busybox:1.37.0")
                .withCopyToContainer(Transferable.of(Files.readAllBytes(keys.keystore)), "/seed/keks.p12")
                .withCopyToContainer(Transferable.of(Files.readAllBytes(keys.signingPem)), "/seed/signing.pem")
                .withCopyToContainer(Transferable.of(Files.readAllBytes(keys.recipientPem)), "/seed/recipient.pem")
                .withCopyToContainer(Transferable.of(Files.readAllBytes(keys.recoveryPem)), "/seed/recovery.pem")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withBinds(
                        new Bind(secrets, new Volume("/secrets")), new Bind(inbound, new Volume("/inbound"))))
                .withCommand("sh", "-c", "cp /seed/* /secrets/ && chown 0:" + APP_UID + " /secrets/* "
                        + "&& chmod 0440 /secrets/* && chown " + APP_UID + ":" + APP_UID + " /inbound "
                        + "&& chmod 0750 /inbound")
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)))) {
            init.start();
        }
    }

    /** Everything else the deployment would put in application.yml, addressed by network alias. */
    static Map<String, Object> appConfig() throws Exception {
        Map<String, Object> catalog = new LinkedHashMap<>(catalogProps("http://seaweed:8333",
                "jdbc:postgresql://postgres:5432/" + postgres.getDatabaseName()));
        Map<String, Object> sdh = new LinkedHashMap<>();
        sdh.put("file", Map.of("min-age", "500ms", "poll-interval", "200ms"));
        sdh.put("split", Map.of("format", "csv", "options", Map.of("csv", Map.of("header", "true"))));
        sdh.put("storage", Map.of("catalog-name", "sdh", "catalog", catalog));
        sdh.put("lease", Map.of("jdbc", Map.of("url", "jdbc:postgresql://postgres:5432/" + postgres.getDatabaseName(),
                "username", postgres.getUsername(), "password", postgres.getPassword())));
        sdh.put("crypto", Map.of(
                "keystore", Map.of("path", "/run/secrets/keks.p12"),
                "keys", Map.of(
                        "sections-kek", TestKeys.SECTIONS,
                        "source-files-kek", TestKeys.SOURCE,
                        "failed-files-kek", TestKeys.FAILED,
                        "signing", Map.of("kid", "connector-2026", "pem", "/run/secrets/signing.pem"),
                        "recipient", Map.of("kid", "downstream-2026", "pem", "/run/secrets/recipient.pem",
                                "sha256", KeyMaterial.fingerprint(keys.recipient.getPublic())),
                        "recovery-public", "/run/secrets/recovery.pem")));
        sdh.put("queue", Map.of("type", "kafka", "uri", "kafka:sdh-sections?brokers=kafka:19092"));
        return Map.of("sdh", sdh);
    }

    static Map<String, String> catalogProps(String s3Endpoint, String jdbcUrl) {
        Map<String, String> m = new HashMap<>();
        m.put("type", "jdbc");
        m.put("uri", jdbcUrl);
        m.put("jdbc.user", postgres.getUsername());
        m.put("jdbc.password", postgres.getPassword());
        m.put("jdbc.schema-version", "V1");
        m.put("warehouse", "s3://" + BUCKET + "/wh");
        m.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        m.put("s3.endpoint", s3Endpoint);
        m.put("s3.path-style-access", "true");
        m.put("s3.access-key-id", ACCESS_KEY);
        m.put("s3.secret-access-key", SECRET_KEY);
        m.put("client.region", "us-east-1");
        return m;
    }

    static String hostS3() {
        return "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(8333);
    }

    static S3Client s3() {
        return S3Client.builder().endpointOverride(URI.create(hostS3())).region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    static String exec(String... cmd) throws Exception {
        assertThat(app.isRunning()).as("connector still running; its log:\n%s", APP_LOG.toUtf8String()).isTrue();
        ExecResult r = app.execInContainer(cmd);
        return r.getExitCode() + ":" + r.getStdout().strip() + r.getStderr().strip();
    }

    @Test
    void lockedDownImageIngestsAndDownstreamReadsEverySectionBack() throws Exception {
        // Runs as the image's non-root user, and nothing outside the volumes and /tmp is writable.
        assertThat(exec("id", "-u")).isEqualTo("0:" + APP_UID);
        assertThat(exec("touch", "/app/x")).startsWith("1:");
        assertThat(exec("touch", "/x")).startsWith("1:");

        String header = "id,name,note\n";
        List<String> records = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int i = 0; i < 20; i++) {
            String rec = i + "," + MARKER + "-" + i + ",\"multi\nline\"\n";
            records.add(rec);
            file.append(rec);
        }
        // Docker refuses copies into a container with a read-only root filesystem, even into its volumes.
        upstream.copyFileToContainer(Transferable.of(file.toString().getBytes(UTF_8)), "/data/inbound/good.csv");
        upstream.copyFileToContainer(Transferable.of((header + "1," + MARKER + ",\"never closed\n").getBytes(UTF_8)),
                "/data/inbound/bad.csv");

        long deadline = System.currentTimeMillis() + 120_000;
        String listing = "";
        while (System.currentTimeMillis() < deadline) {
            listing = exec("ls", "/data/inbound");
            if (!listing.contains(".csv")) {
                break;
            }
            Thread.sleep(500);
        }
        assertThat(listing).as("both files picked up\n" + APP_LOG.toUtf8String()).doesNotContain(".csv");
        assertThat(exec("sh", "-c", "ls /data/inbound/completed | grep -c '\\.json$'")).isEqualTo("0:1");
        assertThat(exec("sh", "-c", "ls /data/inbound/failed | grep -c '\\.json$'")).isEqualTo("0:1");

        // Downstream, from the host: Kafka → connector-queue-reader → sections from SeaweedFS.
        ConnectorProperties.Crypto hostCrypto = new ConnectorProperties.Crypto("local", "local",
                new ConnectorProperties.Keystore(keys.keystore, TestKeys.KS_PASSWORD),
                new ConnectorProperties.Keys(TestKeys.SECTIONS, TestKeys.SOURCE, TestKeys.FAILED,
                        new ConnectorProperties.Signing("connector-2026", keys.signingPem, TestKeys.SIGNING_PASSPHRASE,
                                null),
                        new ConnectorProperties.Recipient("downstream-2026", keys.recipientPem,
                                KeyMaterial.fingerprint(keys.recipient.getPublic())),
                        keys.recoveryPem),
                new ConnectorProperties.Aws(null, null), new ConnectorProperties.Gcp(null),
                new ConnectorProperties.Vault(null, null, null, "transit", null));
        List<ConnectorMessage> msgs = new ArrayList<>();
        try (KeyMaterial km = KeyMaterial.load(hostCrypto);
                QueueReader reader = QueueReader.builder()
                        .recipientKey("downstream-2026", keys.recipient.getPrivate())
                        .trustedSigner("connector-2026", (ECPublicKey) keys.signing.getPublic())
                        .catalog(CatalogUtil.buildIcebergCatalog("sdh", catalogProps(hostS3(), postgres.getJdbcUrl()),
                                null))
                        .expectedCatalogName("sdh")
                        .keyProvider(km.wrap())
                        .build();
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                        "bootstrap.servers", kafka.getBootstrapServers(), "group.id", "downstream",
                        "auto.offset.reset", "earliest"), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("sdh-sections"));
            // 2 × file.started, 20 × section.stored, 1 × file.completed (the bad file stores nothing).
            long until = System.currentTimeMillis() + 60_000;
            while (msgs.size() < 23 && System.currentTimeMillis() < until) {
                for (var rec : consumer.poll(Duration.ofMillis(500))) {
                    msgs.add(reader.decode(rec.value().getBytes(UTF_8)));
                }
            }
            List<SectionStored> sections = msgs.stream().filter(SectionStored.class::isInstance)
                    .map(SectionStored.class::cast).toList();
            assertThat(msgs.stream().filter(FileStarted.class::isInstance)).hasSize(2);
            List<FileCompleted> completed = msgs.stream().filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).toList();
            assertThat(completed).hasSize(1);
            goodId = completed.getFirst().fileId();
            badId = msgs.stream().filter(FileStarted.class::isInstance).map(ConnectorMessage::fileId)
                    .filter(id -> !id.equals(goodId)).findFirst().orElseThrow();
            assertThat(sections).hasSize(20);
            for (SectionStored s : sections) {
                assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                        .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
            }
        }

        // Operator commands inside the running container (design §11.4), as `kubectl exec` would run them: same
        // configuration from the environment, same JVM options, no lease taken.
        String status = exec("env", "SDH_OPERATOR=it-operator", "sdh-admin", "status");
        assertThat(status).startsWith("0:").containsPattern("lease: +file-connector held by .+, epoch 1,")
                .contains("admin.status operator=it-operator");
        assertThat(exec("env", "SDH_OPERATOR=it-operator", "sdh-admin", "restore", "--file-id", badId.toString(),
                "--to", "/tmp")).startsWith("0:").contains("digest verified");
        assertThat(exec("cat", "/tmp/" + badId)).isEqualTo("0:" + (header + "1," + MARKER + ",\"never closed").strip());
        assertThat(exec("rm", "/tmp/" + badId)).startsWith("0:");

        // The container's console output (what a log shipper would collect) holds no PHI or filenames.
        String log = APP_LOG.toUtf8String();
        assertThat(log).contains("Started FileConnectorApplication");
        assertThat(log).doesNotContain(MARKER).doesNotContain("good.csv").doesNotContain("bad.csv");
        // Native libraries load from /app/native, so nothing was unpacked into /tmp (mounted noexec here).
        assertThat(exec("sh", "-c", "ls -A /tmp | grep -ci 'zstd\\|snappy\\|hs_err'")).isEqualTo("1:0");

        // Third-party attribution ships in the image, readable by the app user (design §14.2): our notice, every
        // component with its license and NOTICE, the license texts, and the native libraries' sources.
        assertThat(exec("grep", "-c", "Smile Digital Health Inc.", "/app/legal/LICENSE", "/app/legal/NOTICE"))
                .startsWith("0:");
        assertThat(exec("grep", "-q", "derived from the HL7 CDA Release 2", "/app/legal/NOTICE")).isEqualTo("0:");
        assertThat(exec("grep", "-c", "^Coordinates: com.github.luben:zstd-jni:", "/app/legal/THIRD-PARTY.txt"))
                .isEqualTo("0:1");
        assertThat(exec("grep", "-c", "^Coordinates: org.xerial.snappy:snappy-java:", "/app/legal/THIRD-PARTY.txt"))
                .isEqualTo("0:1");
        assertThat(exec("grep", "-c", "PLACEHOLDER", "/app/legal/THIRD-PARTY.txt")).startsWith("1:0");
        assertThat(exec("sh", "-c", "test -s /app/legal/licenses/apache-2.0.txt && test -s /app/legal/licenses/mit.txt"))
                .isEqualTo("0:");
        // What the base image brings is attributed too: every OS package with its copyright file, and the JRE's notices.
        assertThat(exec("grep", "-qE", "^libc6 [^ ]+ glibc ", "/app/legal/os-packages.txt")).isEqualTo("0:");
        assertThat(exec("sh", "-c", "grep -v '^#' /app/legal/os-packages.txt | while read p rest; do "
                + "test -s /usr/share/doc/${p%%:*}/copyright || echo \"$p\"; done | head -5")).isEqualTo("0:");
        assertThat(exec("sh", "-c", "test -s /opt/java/openjdk/legal/java.base/LICENSE")).isEqualTo("0:");
    }
}
