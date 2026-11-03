/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
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
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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
 * Both stages as deployed (design §5.3.11, §5.3.12, §7.5, §14.1, §14.3): two landing nodes for one intake (active and
 * standby) and the splitter, each run with a read-only root filesystem, a noexec /tmp, all capabilities dropped,
 * no-new-privileges and a memory limit. Subclasses build the service images or the customer images; checks that need
 * a shell inside a container run only where there is one.
 * <p>
 * Secrets come from a volume shaped like a Kubernetes secret with an fsGroup (root:10001, 0440), and the intake
 * directory is a volume shared by an upstream container and both landing nodes. Postgres, SeaweedFS and Kafka are
 * reached by network alias. A good and a bad CSV file are landed, announced on Kafka and split, and a downstream reader
 * reads every section back. Then the active landing node is killed and the standby lands the next file, and the
 * splitter is restarted and splits another.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ContainerImageItBase {
    static final String MARKER = "PHI-MARKER-c0e1";
    static final String BUCKET = "sdh-warehouse";
    static final String ACCESS_KEY = "sdh-it-access";
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);
    static final String APP_UID = "10001";
    static final String SPLITTER = "file-connector";
    static final String LANDING = "landing-connector";
    static final String INTAKE_DIR = "/data/intake";
    static final String LANDED_DIR = INTAKE_DIR + "/landed";
    static final String LANDING_TOPIC = "sdh-landing";
    static final String SECTIONS_TOPIC = "sdh-sections";
    static final String HEADER = "id,name,note\n";

    static Network network;
    static PostgreSQLContainer postgres;
    static GenericContainer<?> seaweed;
    static KafkaContainer kafka;
    /** The splitter. */
    static GenericContainer<?> app;
    /** The landing node that starts active, and the one that starts as its standby. */
    static GenericContainer<?> landingActive;
    static GenericContainer<?> landingStandby;
    /** Stands in for whatever delivers files (SFTP server, sync job): shares the intake volume with landing. */
    static GenericContainer<?> upstream;
    /** Console output, per test class: both subclasses run in one JVM. */
    static ToStringConsumer APP_LOG;
    static ToStringConsumer ACTIVE_LOG;
    static ToStringConsumer STANDBY_LOG;
    static final List<String> VOLUMES = new ArrayList<>();
    static TestKeys keys;
    static String secrets;
    static String intake;

    /** Downstream: reads the splitter's messages from Kafka and its sections from SeaweedFS. */
    static KeyMaterial downstreamKeys;
    static QueueReader reader;
    static KafkaConsumer<String, String> consumer;
    static final List<ConnectorMessage> MESSAGES = new ArrayList<>();

    /** An application's image under test ({@value #SPLITTER} or {@value #LANDING}), built from what Maven packaged. */
    abstract ImageFromDockerfile image(String module) throws Exception;

    /** Whether the images have a shell and the usual tools (the service images) or not (the customer images). */
    abstract boolean hasShell();

    /** Runs a splitter operator command (design §11.4) inside the running container, as {@code it-operator}. */
    abstract String admin(String... args) throws Exception;

    /** Short name for the saved logs. */
    abstract String variant();

    /** Image-specific checks of what the base image contains, given its OS package names. */
    abstract void checkBaseImage(GenericContainer<?> container, List<String> packages) throws Exception;

    @BeforeAll
    void start() throws Exception {
        ItSupport.requireDocker();
        APP_LOG = new ToStringConsumer();
        ACTIVE_LOG = new ToStringConsumer();
        STANDBY_LOG = new ToStringConsumer();
        MESSAGES.clear();
        VOLUMES.clear();
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

        secrets = volume("secrets");
        intake = volume("intake");
        prepareVolumes();
        upstream = new GenericContainer<>("busybox:1.37.0")
                .withCommand("sleep", "3600")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withBinds(new Bind(intake, new Volume(INTAKE_DIR))));
        upstream.start();

        // Both images are built first, so the startup timeouts below don't include a build.
        ImageFromDockerfile landingImage = image(LANDING);
        landingImage.get();
        ImageFromDockerfile splitterImage = image(SPLITTER);
        splitterImage.get();

        landingActive = lockedDown(new GenericContainer<>(landingImage), landingConfig(false), ACTIVE_LOG,
                new Bind(intake, new Volume(INTAKE_DIR)))
                .waitingFor(Wait.forLogMessage(".*Started LandingConnectorApplication.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        landingActive.start();
        landingStandby = lockedDown(new GenericContainer<>(landingImage), landingConfig(true), STANDBY_LOG,
                new Bind(intake, new Volume(INTAKE_DIR)))
                .waitingFor(Wait.forLogMessage(".*lease\\.standby.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
        landingStandby.start();

        app = lockedDown(new GenericContainer<>(splitterImage), splitterConfig(), APP_LOG)
                .withEnv("SDH_CRYPTO_KEYS_INBOX_PASSPHRASE", TestKeys.INBOX_PASSPHRASE)
                .waitingFor(Wait.forLogMessage(".*Started FileConnectorApplication.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        app.start();

        CommonSettings.Crypto hostCrypto = new CommonSettings.Crypto("local", "local",
                new CommonSettings.Keystore(keys.keystore, TestKeys.KS_PASSWORD),
                keys.splitterKeys(KeyMaterial.fingerprint(keys.recipient.getPublic()), keys.signingPem, null),
                new CommonSettings.Aws(null, null), new CommonSettings.Gcp(null),
                new CommonSettings.Vault(null, null, null, "transit", null));
        downstreamKeys = KeyMaterial.load(hostCrypto);
        reader = QueueReader.builder()
                .recipientKey("downstream-2026", keys.recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) keys.signing.getPublic())
                .catalog(CatalogUtil.buildIcebergCatalog("sdh", catalogProps(hostS3(), postgres.getJdbcUrl()), null))
                .expectedCatalogName("sdh")
                .keyProvider(downstreamKeys.wrap())
                .build();
        consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                "group.id", "downstream", "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(SECTIONS_TOPIC));
    }

    @AfterAll
    void stop() throws Exception {
        // Kept for diagnosis: the console output is PHI-free by design, and this test asserts it.
        Files.writeString(Path.of("target", variant() + "-image-it.log"), splitterLog());
        Files.writeString(Path.of("target", variant() + "-landing-image-it.log"),
                ACTIVE_LOG.toUtf8String() + "\n---- standby ----\n" + STANDBY_LOG.toUtf8String());
        for (AutoCloseable c : new AutoCloseable[] {consumer, reader, downstreamKeys}) {
            if (c != null) {
                c.close();
            }
        }
        Stream.of(app, landingStandby, landingActive, upstream, kafka, seaweed, postgres).filter(c -> c != null)
                .forEach(GenericContainer::stop);
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

    /** The container as deployed: locked down, secrets read-only, settings from the environment. */
    static GenericContainer<?> lockedDown(GenericContainer<?> container, Map<String, Object> config,
            ToStringConsumer log, Bind... extraBinds) throws Exception {
        List<Bind> binds = new ArrayList<>(List.of(new Bind(secrets, new Volume("/run/secrets"), AccessMode.ro)));
        binds.addAll(List.of(extraBinds));
        return container
                .withNetwork(network)
                .withEnv("SPRING_APPLICATION_JSON", JSONObjectUtils.toJSONString(config))
                .withEnv("SDH_CRYPTO_KEYSTORE_PASSWORD", TestKeys.KS_PASSWORD)
                .withEnv("SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE", TestKeys.SIGNING_PASSPHRASE)
                .withEnv("JDK_JAVA_OPTIONS", "-Xss1m")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withReadonlyRootfs(true)
                        .withTmpFs(Map.of("/tmp", "rw,noexec,nosuid,nodev,size=256m"))
                        .withCapDrop(Capability.ALL)
                        .withSecurityOpts(List.of("no-new-privileges"))
                        .withMemory(1536L << 20)
                        .withBinds(binds))
                .withLogConsumer(log);
    }

    /**
     * A base image pinned in {@code docker/base-images.env} (design §14.5), as scripts/build-image.sh uses it, so the
     * tests build on exactly the bases a release does.
     */
    static String baseImage(String name) throws Exception {
        for (String line : Files.readAllLines(Path.of("..", "docker", "base-images.env"), UTF_8)) {
            if (line.startsWith(name + "=")) {
                return line.substring(name.length() + 1).strip();
            }
        }
        throw new IllegalStateException(name + " is not pinned in docker/base-images.env");
    }

    /** An application's module directory, from file-connector's. */
    static Path module(String module) {
        return Path.of("..", module);
    }

    /** The jar Maven just packaged for a module. */
    static Path packagedJar(String module) throws Exception {
        try (Stream<Path> s = Files.list(module(module).resolve("target"))) {
            return s.filter(p -> p.getFileName().toString().matches(module + "-[^-]+(-SNAPSHOT)?\\.jar")).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Package " + module + " first (mvn -Pit verify)"));
        }
    }

    /** Reads a file out of a container (works without a shell, and on a read-only root filesystem). */
    static String read(GenericContainer<?> container, String path) throws Exception {
        return container.copyFileFromContainer(path, in -> new String(in.readAllBytes(), UTF_8));
    }

    /**
     * Reads a file in the splitter's /tmp as the README's troubleshooting steps do: a busybox container sharing its
     * process namespace, as the same user, reads it under /proc/1/root (docker cp can't see a tmpfs).
     */
    static String debugRead(String path) throws Exception {
        String containerId = app.getContainerId();
        try (GenericContainer<?> debug = new GenericContainer<>("busybox:1.37.0")
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withUser(APP_UID + ":" + APP_UID);
                    cmd.getHostConfig().withPidMode("container:" + containerId).withCapDrop(Capability.ALL)
                            .withSecurityOpts(List.of("no-new-privileges"));
                })
                .withCommand("cat", "/proc/1/root" + path)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)))) {
            debug.start();
            return debug.getLogs();
        }
    }

    /** Runs a command in the upstream container, which shares the intake volume. */
    static String upstreamExec(String... cmd) throws Exception {
        ExecResult r = upstream.execInContainer(cmd);
        return r.getExitCode() + ":" + r.getStdout().strip() + r.getStderr().strip();
    }

    /** Delivers a file to the intake. Docker refuses copies into a read-only root filesystem, even into volumes. */
    static void deliver(String name, String content) {
        upstream.copyFileToContainer(Transferable.of(content.getBytes(UTF_8)), INTAKE_DIR + "/" + name);
    }

    /** Waits until landing has taken every delivered file out of the intake (it moves them to landed/). */
    static void awaitLanded(int landedFiles) throws Exception {
        long deadline = System.currentTimeMillis() + 180_000;
        String expected = "0:0 " + landedFiles;
        String counts = "";
        while (System.currentTimeMillis() < deadline) {
            // Files still in the intake, and files moved to landed/.
            counts = upstreamExec("sh", "-c", "echo $(ls " + INTAKE_DIR + " | grep -c '\\.csv$')"
                    + " $(ls " + LANDED_DIR + " 2>/dev/null | grep -c '\\.csv$')");
            if (counts.equals(expected)) {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(counts).as("files landed (left in intake, moved)\nactive:\n%s\nstandby:\n%s",
                ACTIVE_LOG.toUtf8String(), STANDBY_LOG.toUtf8String()).isEqualTo(expected);
    }

    /** Reads downstream messages until {@code until} holds or two minutes pass. */
    static void awaitMessages(Predicate<List<ConnectorMessage>> until) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (!until.test(MESSAGES) && System.currentTimeMillis() < deadline) {
            for (var rec : consumer.poll(Duration.ofMillis(500))) {
                MESSAGES.add(reader.decode(rec.value().getBytes(UTF_8)));
            }
        }
        assertThat(until.test(MESSAGES)).as("downstream messages %s\nsplitter:\n%s", MESSAGES, splitterLog())
                .isTrue();
    }

    /** How many items the splitter has completed, by downstream message. */
    static long completed(List<ConnectorMessage> messages) {
        return messages.stream().filter(FileCompleted.class::isInstance).count();
    }

    /** The splitter's whole console output, across restarts. */
    static String splitterLog() {
        return app == null ? APP_LOG.toUtf8String() : app.getLogs();
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
     * intake directory owned by the app user.
     */
    static void prepareVolumes() throws Exception {
        Map<String, Path> files = new LinkedHashMap<>();
        files.put("keks.p12", keys.keystore);
        files.put("signing.pem", keys.signingPem);
        files.put("recipient.pem", keys.recipientPem);
        files.put("inbox.pem", keys.inboxPem);
        files.put("landing-signer.pem", keys.landingSignerPem);
        files.put("landing-signing.pem", keys.landingSigningPem);
        files.put("inbox-public.pem", keys.inboxPublicPem);
        GenericContainer<?> init = new GenericContainer<>("busybox:1.37.0");
        for (var f : files.entrySet()) {
            init.withCopyToContainer(Transferable.of(Files.readAllBytes(f.getValue())), "/seed/" + f.getKey());
        }
        try (init) {
            init.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withBinds(
                            new Bind(secrets, new Volume("/secrets")), new Bind(intake, new Volume("/intake"))))
                    .withCommand("sh", "-c", "cp /seed/* /secrets/ && chown 0:" + APP_UID + " /secrets/* "
                            + "&& chmod 0440 /secrets/* && chown " + APP_UID + ":" + APP_UID + " /intake "
                            + "&& chmod 0770 /intake")
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)))
                    .start();
        }
    }

    /** The settings a deployment would put in the landing node's application.yml, addressed by network alias. */
    static Map<String, Object> landingConfig(boolean standby) throws Exception {
        Map<String, Object> sdh = new LinkedHashMap<>();
        sdh.put("connector-id", "landing-" + ItSupport.INTAKE);
        sdh.put("landing", Map.of("intake", ItSupport.INTAKE, "standby", standby,
                "source", Map.of("path", INTAKE_DIR, "access", "read-write", "after", "move", "move-to", LANDED_DIR,
                        "settle", "1s", "poll-interval", "200ms")));
        sdh.put("storage", Map.of("catalog-name", "sdh", "catalog", containerCatalog()));
        // A short lease, so the standby takes over within seconds of the active node dying.
        sdh.put("lease", Map.of("jdbc", leaseJdbc(), "ttl", "15s", "renew-interval", "4s", "safety-margin", "3s"));
        sdh.put("crypto", Map.of(
                "keystore", Map.of("path", "/run/secrets/keks.p12"),
                "keys", Map.of(
                        "landed-items-kek", TestKeys.LANDED,
                        "signing", Map.of("kid", TestKeys.LANDING_SIGNER_KID,
                                "pem", "/run/secrets/landing-signing.pem"),
                        "recipient", Map.of("kid", TestKeys.INBOX_KID, "pem", "/run/secrets/inbox-public.pem",
                                "sha256", KeyMaterial.fingerprint(keys.inbox.getPublic())))));
        sdh.put("queue", Map.of("type", "kafka", "uri", "kafka:" + LANDING_TOPIC + "?brokers=kafka:19092"));
        return Map.of("sdh", sdh);
    }

    /** The settings a deployment would put in the splitter's application.yml, addressed by network alias. */
    static Map<String, Object> splitterConfig() throws Exception {
        Map<String, Object> sdh = new LinkedHashMap<>();
        // Offsets are committed automatically, one message per poll, so only for messages already handled;
        // breakOnFirstError keeps a failed message from being skipped (design §5.3.12).
        sdh.put("landing", Map.of("intake", ItSupport.INTAKE, "queue-uri", "kafka:" + LANDING_TOPIC
                + "?brokers=kafka:19092&groupId=file-connector&autoOffsetReset=earliest&maxPollRecords=1"
                + "&breakOnFirstError=true"));
        sdh.put("split", Map.of("format", "csv", "options", Map.of("csv", Map.of("header", "true"))));
        sdh.put("storage", Map.of("catalog-name", "sdh", "catalog", containerCatalog()));
        sdh.put("lease", Map.of("jdbc", leaseJdbc()));
        sdh.put("crypto", Map.of(
                "keystore", Map.of("path", "/run/secrets/keks.p12"),
                "keys", Map.of(
                        "sections-kek", TestKeys.SECTIONS,
                        "landed-items-kek", TestKeys.LANDED,
                        "signing", Map.of("kid", "connector-2026", "pem", "/run/secrets/signing.pem"),
                        "recipient", Map.of("kid", "downstream-2026", "pem", "/run/secrets/recipient.pem",
                                "sha256", KeyMaterial.fingerprint(keys.recipient.getPublic())),
                        "inbox", Map.of("kid", TestKeys.INBOX_KID, "pem", "/run/secrets/inbox.pem"),
                        "landing-signer", Map.of("kid", TestKeys.LANDING_SIGNER_KID,
                                "pem", "/run/secrets/landing-signer.pem"))));
        sdh.put("queue", Map.of("type", "kafka", "uri", "kafka:" + SECTIONS_TOPIC + "?brokers=kafka:19092"));
        return Map.of("sdh", sdh);
    }

    static Map<String, Object> leaseJdbc() {
        return Map.of("url", "jdbc:postgresql://postgres:5432/" + postgres.getDatabaseName(),
                "username", postgres.getUsername(), "password", postgres.getPassword());
    }

    static Map<String, Object> containerCatalog() {
        return new LinkedHashMap<>(catalogProps("http://seaweed:8333",
                "jdbc:postgresql://postgres:5432/" + postgres.getDatabaseName()));
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

    static String exec(GenericContainer<?> container, String... cmd) throws Exception {
        assertThat(container.isRunning()).as("container still running").isTrue();
        ExecResult r = container.execInContainer(cmd);
        return r.getExitCode() + ":" + r.getStdout().strip() + r.getStderr().strip();
    }

    /** A CSV file of {@code count} records, each with the PHI marker and a quoted newline. */
    static String csv(String tag, int count, List<String> records) {
        StringBuilder file = new StringBuilder(HEADER);
        for (int i = 0; i < count; i++) {
            String rec = i + "," + MARKER + "-" + tag + "-" + i + ",\"multi\nline\"\n";
            records.add(rec);
            file.append(rec);
        }
        return file.toString();
    }

    @Test
    void bothStagesRunLockedDownAndSurviveFailover() throws Exception {
        for (GenericContainer<?> c : List.of(app, landingActive)) {
            // Runs as the image's non-root user.
            assertThat(DockerClientFactory.lazyClient().inspectImageCmd(c.getDockerImageName()).exec().getConfig()
                    .getUser()).isEqualTo(APP_UID + ":" + APP_UID);
            if (hasShell()) {
                // Nothing outside the volumes and /tmp is writable.
                assertThat(exec(c, "id", "-u")).isEqualTo("0:" + APP_UID);
                assertThat(exec(c, "touch", "/app/x")).startsWith("1:");
                assertThat(exec(c, "touch", "/x")).startsWith("1:");
            } else {
                assertThat(c.execInContainer("sh", "-c", "true").getExitCode()).as("no shell in the image")
                        .isNotZero();
            }
        }

        // 1. Landing, announcing and splitting: a good file and a bad one.
        List<String> records = new ArrayList<>();
        deliver("good.csv", csv("a", 20, records));
        String bad = HEADER + "1," + MARKER + ",\"never closed\n";
        deliver("bad.csv", bad);
        awaitLanded(2);
        // 2 × file.started, 20 × section.stored, 1 × file.completed (the bad file stores nothing).
        awaitMessages(m -> m.stream().filter(FileStarted.class::isInstance).count() == 2 && completed(m) == 1
                && m.stream().filter(SectionStored.class::isInstance).count() == 20);
        UUID goodId = MESSAGES.stream().filter(FileCompleted.class::isInstance).findFirst().orElseThrow().fileId();
        UUID badId = MESSAGES.stream().filter(FileStarted.class::isInstance).map(ConnectorMessage::fileId)
                .filter(id -> !id.equals(goodId)).findFirst().orElseThrow();
        for (SectionStored s : MESSAGES.stream().filter(SectionStored.class::isInstance)
                .map(SectionStored.class::cast).toList()) {
            assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                    .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
        }

        // 2. Operator commands inside the running splitter (design §11.4), as `kubectl exec` would run them: same
        // configuration from the environment, same JVM options, no lease taken. The bad item is restored from landing
        // into /tmp and read out the way the README describes.
        assertThat(admin("status")).startsWith("0:").containsPattern("lease: +file-connector held by .+, epoch 1,")
                .contains("admin.status operator=it-operator");
        assertThat(admin("restore", "--file-id", badId.toString(), "--to", "/tmp"))
                .startsWith("0:").contains("digest verified");
        assertThat(debugRead("/tmp/" + badId)).isEqualTo(bad);

        // 3. Landing failover: the active node dies (SIGKILL, no lease release); the standby takes the lease once it
        // lapses and lands the next file.
        DockerClientFactory.lazyClient().killContainerCmd(landingActive.getContainerId()).exec();
        List<String> afterFailover = new ArrayList<>();
        deliver("next.csv", csv("b", 3, afterFailover));
        awaitLanded(3);
        assertThat(STANDBY_LOG.toUtf8String()).contains("Started LandingConnectorApplication");
        awaitMessages(m -> completed(m) == 2);

        // 4. The splitter restarts (as a pod restart would) and splits the next file.
        DockerClientFactory.lazyClient().restartContainerCmd(app.getContainerId()).withTimeout(30).exec();
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline
                && splitterLog().split("Started FileConnectorApplication", -1).length < 3) {
            Thread.sleep(500);
        }
        assertThat(splitterLog().split("Started FileConnectorApplication", -1)).as("splitter restarted").hasSize(3);
        deliver("last.csv", csv("c", 2, new ArrayList<>()));
        awaitLanded(4);
        awaitMessages(m -> completed(m) == 3);
        assertThat(admin("status")).startsWith("0:").containsPattern("lease: +file-connector held by .+, epoch 2,");

        // 5. No container's console output (what a log shipper would collect) holds PHI or filenames.
        for (String log : List.of(splitterLog(), ACTIVE_LOG.toUtf8String(), STANDBY_LOG.toUtf8String())) {
            assertThat(log).doesNotContain(MARKER).doesNotContain(".csv");
        }
        assertThat(ACTIVE_LOG.toUtf8String()).contains("Started LandingConnectorApplication");
        // Native libraries load from /app/native, so nothing was unpacked into /tmp (mounted noexec here). Without
        // a shell, the work itself shows zstd loaded: sections and landed chunks couldn't be compressed otherwise.
        if (hasShell()) {
            for (GenericContainer<?> c : List.of(app, landingStandby)) {
                assertThat(exec(c, "sh", "-c", "ls -A /tmp | grep -ci 'zstd\\|snappy\\|hs_err'")).isEqualTo("1:0");
            }
        }

        // 6. Third-party attribution ships in both images (design §14.2).
        checkAttribution(app);
        checkAttribution(landingStandby);
    }

    /**
     * Our notice, every component with its license and NOTICE, the license texts, the native libraries' sources, and
     * every OS package the base image brings.
     */
    void checkAttribution(GenericContainer<?> c) throws Exception {
        assertThat(read(c, "/app/legal/LICENSE")).contains("Smile Digital Health Inc.");
        assertThat(read(c, "/app/legal/NOTICE")).contains("Smile Digital Health Inc.");
        if (c == app) {
            assertThat(read(c, "/app/legal/NOTICE")).contains("derived from the HL7 CDA Release 2");
        }
        String thirdParty = read(c, "/app/legal/THIRD-PARTY.txt");
        assertThat(thirdParty).contains("Coordinates: com.github.luben:zstd-jni:")
                .contains("Coordinates: org.xerial.snappy:snappy-java:").doesNotContain("PLACEHOLDER");
        // Every component offered under a choice of licenses names the one we distribute it under, and none is a GNU
        // license where something else was offered (design §14.2).
        List<String> lines = thirdParty.lines().toList();
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith("Licenses: ") && lines.get(i).contains(",")) {
                String elected = lines.get(i + 1);
                assertThat(elected).as("election for " + lines.get(i)).startsWith("Elected license: ")
                        .doesNotContain("NONE").doesNotContain("GPL");
            }
        }
        assertThat(thirdParty).contains("Elected license: Apache-2.0").contains("Elected license: EPL-2.0");
        assertThat(read(c, "/app/legal/licenses/apache-2.0.txt")).contains("Apache License");
        assertThat(read(c, "/app/legal/licenses/mit.txt")).contains("MIT License");

        // What the base image brings is attributed too: every OS package, with its source package and a copyright
        // file that exists in the image.
        List<String[]> packages = read(c, "/app/legal/os-packages.txt").lines().filter(l -> !l.startsWith("#"))
                .map(l -> l.split(" ")).toList();
        assertThat(packages).anySatisfy(p -> assertThat(p).startsWith("libc6").contains("glibc"));
        for (String[] p : packages) {
            assertThat(p).as(String.join(" ", p)).hasSize(5);
            assertThat(read(c, p[4])).as("copyright of " + p[0]).isNotBlank();
        }
        checkBaseImage(c, packages.stream().map(p -> p[0]).toList());
    }
}
