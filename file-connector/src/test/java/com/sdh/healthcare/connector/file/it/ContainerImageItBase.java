/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.INTAKE;
import static com.sdh.healthcare.connector.file.it.ItSupport.csvRowJson;
import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Both stages as deployed (design §5.3.11, §5.3.12, §7.5, §14.1, §14.3):
 * two landing nodes for one intake (active and standby) and the splitter,
 * each run with a read-only root filesystem, a noexec /tmp, all
 * capabilities dropped, no-new-privileges and a memory limit. Subclasses
 * build the service images or the customer images; checks that need a
 * shell inside a container run only where there is one.
 *
 * <p>Secrets come from a volume shaped like a Kubernetes secret with an
 * fsGroup (root:10001, 0440), and the intake directory is a volume shared
 * by an upstream container and both landing nodes. Postgres, SeaweedFS and
 * Kafka are reached by network alias. A good and a bad CSV file are
 * landed, announced on Kafka and split, and a downstream reader reads
 * every section back. Then the active landing node is killed and the
 * standby lands the next file, and the splitter is restarted and splits
 * another.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ContainerImageItBase
{

    /** A marker in every record, which must never appear in a log. */
    static final String MARKER = "PHI-MARKER-c0e1";

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

    /** The images' non-root user and group ID. */
    static final String APP_UID = "10001";

    /** The splitter's module. */
    static final String SPLITTER = "file-connector";

    /** The landing node's module. */
    static final String LANDING = "landing-connector";

    /** The intake directory, as the containers see it. */
    static final String INTAKE_DIR = "/data/intake";

    /** Where landing moves files it has landed. */
    static final String LANDED_DIR = INTAKE_DIR + "/landed";

    /** The topic landing announces items on. */
    static final String LANDING_TOPIC = "sdh-landing";

    /** The topic the splitter publishes to. */
    static final String SECTIONS_TOPIC = "sdh-sections";

    /** The test files' header row. */
    static final String HEADER = "id,name,note\n";

    /** The network every container shares. */
    static Network network;

    /** The lease database and catalog. */
    static PostgreSQLContainer postgres;

    /** The object store. */
    static GenericContainer<?> seaweed;

    /** The broker. */
    static KafkaContainer kafka;

    /** The splitter. */
    static GenericContainer<?> app;

    /** The landing node that starts active. */
    static GenericContainer<?> landingActive;

    /** The landing node that starts as the active one's standby. */
    static GenericContainer<?> landingStandby;

    /**
     * Stands in for whatever delivers files (SFTP server, sync job): shares
     * the intake volume with landing.
     */
    static GenericContainer<?> upstream;

    /**
     * The splitter's console output, per test class: both subclasses run
     * in one JVM.
     */
    static ToStringConsumer APP_LOG;

    /** The active landing node's console output. */
    static ToStringConsumer ACTIVE_LOG;

    /** The standby landing node's console output. */
    static ToStringConsumer STANDBY_LOG;

    /** The Docker volumes created, to remove afterwards. */
    static final List<String> VOLUMES = new ArrayList<>();

    /** The key files. */
    static TestKeys keys;

    /** The secrets volume. */
    static String secrets;

    /** The intake volume. */
    static String intake;

    /** Downstream's keys, for reading sections back. */
    static KeyMaterial downstreamKeys;

    /**
     * Downstream: reads the splitter's sections from SeaweedFS.
     */
    static QueueReader reader;

    /** Downstream: reads the splitter's messages from Kafka. */
    static KafkaConsumer<String, String> consumer;

    /** The messages downstream has read so far. */
    static final List<ConnectorMessage> MESSAGES = new ArrayList<>();

    /**
     * An application's image under test ({@value #SPLITTER} or
     * {@value #LANDING}), built from what Maven packaged.
     *
     * @param module the application's module
     * @return the image
     * @throws Exception if the image can't be described
     */
    abstract ImageFromDockerfile image(String module) throws Exception;

    /**
     * Whether the images have a shell and the usual tools (the service
     * images) or not (the customer images).
     *
     * @return true if they have
     */
    abstract boolean hasShell();

    /**
     * Runs a splitter operator command (design §11.4) inside the running
     * container, as {@code it-operator}.
     *
     * @param args the command and its options
     * @return the exit status, a colon, and the output
     * @throws Exception if the command can't be run
     */
    abstract String admin(String... args) throws Exception;

    /**
     * Short name for the saved logs.
     *
     * @return the name
     */
    abstract String variant();

    /**
     * Image-specific checks of what the base image contains.
     *
     * @param container a running container of the image
     * @param packages the image's OS package names
     * @throws Exception if a file can't be read from the container
     */
    abstract void checkBaseImage(GenericContainer<?> container,
            List<String> packages) throws Exception;

    /**
     * Starts the infrastructure, builds the images, and starts both
     * landing nodes, the splitter and the downstream reader.
     *
     * @throws Exception if anything fails to start
     */
    @BeforeAll
    void start() throws Exception
    {
        requireDocker();
        APP_LOG = new ToStringConsumer();
        ACTIVE_LOG = new ToStringConsumer();
        STANDBY_LOG = new ToStringConsumer();
        MESSAGES.clear();
        VOLUMES.clear();
        keys = new TestKeys(Files.createTempDirectory("sdh-image-it"));
        network = Network.newNetwork();
        postgres = new PostgreSQLContainer("postgres:18.4")
                .withNetwork(network).withNetworkAliases("postgres");
        seaweed = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withNetwork(network).withNetworkAliases("seaweed")
                .withCopyToContainer(Transferable.of(S3_CONFIG),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        "-ip.bind=0.0.0.0", "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        kafka = new KafkaContainer("apache/kafka:4.2.2").withNetwork(network)
                .withListener("kafka:19092");
        Stream.of(postgres, seaweed, kafka).parallel()
                .forEach(GenericContainer::start);
        try (S3Client s3 = s3())
        {
            s3.createBucket(request -> request.bucket(BUCKET));
        }

        secrets = volume("secrets");
        intake = volume("intake");
        prepareVolumes();
        upstream = new GenericContainer<>("busybox:1.37.0")
                .withCommand("sleep", "3600")
                .withCreateContainerCmdModifier(command -> command
                        .getHostConfig()
                        .withBinds(new Bind(intake, new Volume(INTAKE_DIR))));
        upstream.start();

        // Both images are built first, so the startup timeouts below don't
        // include a build.
        ImageFromDockerfile landingImage = image(LANDING);
        landingImage.get();
        ImageFromDockerfile splitterImage = image(SPLITTER);
        splitterImage.get();

        landingActive = lockedDown(new GenericContainer<>(landingImage),
                landingConfig(false), ACTIVE_LOG,
                new Bind(intake, new Volume(INTAKE_DIR)))
                .waitingFor(Wait.forLogMessage(
                        ".*Started LandingConnectorApplication.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        landingActive.start();
        landingStandby = lockedDown(new GenericContainer<>(landingImage),
                landingConfig(true), STANDBY_LOG,
                new Bind(intake, new Volume(INTAKE_DIR)))
                .waitingFor(Wait.forLogMessage(".*lease\\.standby.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        landingStandby.start();

        app = lockedDown(new GenericContainer<>(splitterImage),
                splitterConfig(), APP_LOG)
                .withEnv("SDH_CRYPTO_KEYS_INBOX_PASSPHRASE",
                        TestKeys.INBOX_PASSPHRASE)
                .waitingFor(Wait.forLogMessage(
                        ".*Started FileConnectorApplication.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        app.start();

        CommonSettings.Crypto hostCrypto = new CommonSettings.Crypto("local",
                "local",
                new CommonSettings.Keystore(keys.keystore,
                        TestKeys.KS_PASSWORD),
                keys.splitterKeys(
                        KeyMaterial.fingerprint(keys.recipient.getPublic()),
                        keys.signingPem, null),
                new CommonSettings.Aws(null, null),
                new CommonSettings.Gcp(null),
                new CommonSettings.Vault(null, null, null, "transit", null));
        downstreamKeys = KeyMaterial.load(hostCrypto);
        reader = QueueReader.builder()
                .recipientKey("downstream-2026", keys.recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) keys.signing.getPublic())
                .catalog(CatalogUtil.buildIcebergCatalog("sdh",
                        catalogProperties(hostS3(), postgres.getJdbcUrl()),
                        null))
                .expectedCatalogName("sdh")
                .keyProvider(downstreamKeys.wrap())
                .build();
        consumer = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "group.id", "downstream", "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(SECTIONS_TOPIC));
    }

    /**
     * Saves the logs, then stops and removes everything.
     *
     * @throws Exception if a log can't be saved or a client closed
     */
    @AfterAll
    void stop() throws Exception
    {
        // Kept for diagnosis: the console output is PHI-free by design, and
        // this test asserts it.
        Files.writeString(Path.of("target", variant() + "-image-it.log"),
                splitterLog());
        Files.writeString(
                Path.of("target", variant() + "-landing-image-it.log"),
                ACTIVE_LOG.toUtf8String() + "\n---- standby ----\n"
                        + STANDBY_LOG.toUtf8String());
        for (AutoCloseable closeable :
                new AutoCloseable[] {consumer, reader, downstreamKeys})
        {
            if (closeable != null)
            {
                closeable.close();
            }
        }
        Stream.of(app, landingStandby, landingActive, upstream, kafka,
                        seaweed, postgres)
                .filter(container -> container != null)
                .forEach(GenericContainer::stop);
        DockerClient docker = DockerClientFactory.lazyClient();
        for (String volume : VOLUMES)
        {
            try
            {
                docker.removeVolumeCmd(volume).exec();
            }
            catch (RuntimeException ignored)
            {
                // Ryuk removes it with the session's other resources.
            }
        }
        if (network != null)
        {
            network.close();
        }
    }

    /**
     * The container as deployed: locked down, secrets read-only, settings
     * from the environment.
     *
     * @param container the container
     * @param config its settings, as SPRING_APPLICATION_JSON
     * @param log where its console output goes
     * @param extraBinds volumes besides the secrets
     * @return the container, configured
     * @throws Exception if the settings can't be written as JSON
     */
    static GenericContainer<?> lockedDown(GenericContainer<?> container,
            Map<String, Object> config, ToStringConsumer log,
            Bind... extraBinds) throws Exception
    {
        List<Bind> binds = new ArrayList<>(List.of(new Bind(secrets,
                new Volume("/run/secrets"), AccessMode.ro)));
        binds.addAll(List.of(extraBinds));
        return container
                .withNetwork(network)
                .withEnv("SPRING_APPLICATION_JSON",
                        JSONObjectUtils.toJSONString(config))
                .withEnv("SDH_CRYPTO_KEYSTORE_PASSWORD", TestKeys.KS_PASSWORD)
                .withEnv("SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE",
                        TestKeys.SIGNING_PASSPHRASE)
                .withEnv("JDK_JAVA_OPTIONS", "-Xss1m")
                .withCreateContainerCmdModifier(command -> command
                        .getHostConfig()
                        .withReadonlyRootfs(true)
                        .withTmpFs(Map.of("/tmp",
                                "rw,noexec,nosuid,nodev,size=256m"))
                        .withCapDrop(Capability.ALL)
                        .withSecurityOpts(List.of("no-new-privileges"))
                        .withMemory(1536L << 20)
                        .withBinds(binds))
                .withLogConsumer(log);
    }

    /**
     * A base image pinned in {@code docker/base-images.env} (design §14.5),
     * as scripts/build-image.sh uses it, so the tests build on exactly the
     * bases a release does.
     *
     * @param name the variable's name
     * @return the image reference
     * @throws Exception if the file can't be read or the name isn't pinned
     */
    static String baseImage(String name) throws Exception
    {
        for (String line : Files.readAllLines(
                Path.of("..", "docker", "base-images.env"), UTF_8))
        {
            if (line.startsWith(name + "="))
            {
                return line.substring(name.length() + 1).strip();
            }
        }
        throw new IllegalStateException(
                name + " is not pinned in docker/base-images.env");
    }

    /**
     * An application's module directory, from file-connector's.
     *
     * @param module the module
     * @return the directory
     */
    static Path module(String module)
    {
        return Path.of("..", module);
    }

    /**
     * The jar Maven just packaged for a module.
     *
     * @param module the module
     * @return the jar
     * @throws Exception if the module hasn't been packaged
     */
    static Path packagedJar(String module) throws Exception
    {
        try (Stream<Path> paths = Files.list(module(module).resolve("target")))
        {
            return paths.filter(path -> path.getFileName().toString()
                            .matches(module + "-[^-]+(-SNAPSHOT)?\\.jar"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Package "
                            + module + " first (mvn -Pit verify)"));
        }
    }

    /**
     * Reads a file out of a container (works without a shell, and on a
     * read-only root filesystem).
     *
     * @param container the container
     * @param path the file's path inside it
     * @return the file as UTF-8
     * @throws Exception if the file can't be copied out
     */
    static String read(GenericContainer<?> container, String path)
            throws Exception
    {
        return container.copyFileFromContainer(path,
                in -> new String(in.readAllBytes(), UTF_8));
    }

    /**
     * Reads a file in the splitter's /tmp as the README's troubleshooting
     * steps do: a busybox container sharing its process namespace, as the
     * same user, reads it under /proc/1/root (docker cp can't see a tmpfs).
     *
     * @param path the file's path in the splitter
     * @return the file's content
     * @throws Exception if the file can't be read
     */
    static String debugRead(String path) throws Exception
    {
        String containerId = app.getContainerId();
        try (GenericContainer<?> debug =
                new GenericContainer<>("busybox:1.37.0")
                        .withCreateContainerCmdModifier(command ->
                        {
                            command.withUser(APP_UID + ":" + APP_UID);
                            command.getHostConfig()
                                    .withPidMode("container:" + containerId)
                                    .withCapDrop(Capability.ALL)
                                    .withSecurityOpts(
                                            List.of("no-new-privileges"));
                        })
                        .withCommand("cat", "/proc/1/root" + path)
                        .withStartupCheckStrategy(
                                new OneShotStartupCheckStrategy()
                                        .withTimeout(Duration.ofMinutes(1))))
        {
            debug.start();
            return debug.getLogs();
        }
    }

    /**
     * Runs a command in the upstream container, which shares the intake
     * volume.
     *
     * @param command the command and its arguments
     * @return the exit status, a colon, and the output
     * @throws Exception if the command can't be run
     */
    static String upstreamExec(String... command) throws Exception
    {
        ExecResult result = upstream.execInContainer(command);
        return result.getExitCode() + ":" + result.getStdout().strip()
                + result.getStderr().strip();
    }

    /**
     * Delivers a file to the intake. Docker refuses copies into a read-only
     * root filesystem, even into volumes, so it goes through upstream.
     *
     * @param name the file name
     * @param content the file's content
     */
    static void deliver(String name, String content)
    {
        upstream.copyFileToContainer(Transferable.of(content.getBytes(UTF_8)),
                INTAKE_DIR + "/" + name);
    }

    /**
     * Waits until landing has taken every delivered file out of the intake
     * (it moves them to landed/).
     *
     * @param landedFiles how many files should have been moved
     * @throws Exception if the files aren't landed within 3 minutes
     */
    static void awaitLanded(int landedFiles) throws Exception
    {
        long deadline = System.currentTimeMillis() + 180_000;
        String expected = "0:0 " + landedFiles;
        String counts = "";
        while (System.currentTimeMillis() < deadline)
        {
            // Files still in the intake, and files moved to landed/.
            counts = upstreamExec("sh", "-c", "echo $(ls " + INTAKE_DIR
                    + " | grep -c '\\.csv$')"
                    + " $(ls " + LANDED_DIR
                    + " 2>/dev/null | grep -c '\\.csv$')");
            if (counts.equals(expected))
            {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(counts).as("files landed (left in intake, moved)"
                + "\nactive:\n%s\nstandby:\n%s", ACTIVE_LOG.toUtf8String(),
                STANDBY_LOG.toUtf8String()).isEqualTo(expected);
    }

    /**
     * Reads downstream messages until a condition holds or two minutes
     * pass.
     *
     * @param until the condition, on every message read so far
     * @throws Exception if a message can't be decoded or time runs out
     */
    static void awaitMessages(Predicate<List<ConnectorMessage>> until)
            throws Exception
    {
        long deadline = System.currentTimeMillis() + 120_000;
        while (!until.test(MESSAGES) && System.currentTimeMillis() < deadline)
        {
            for (ConsumerRecord<String, String> record :
                    consumer.poll(Duration.ofMillis(500)))
            {
                MESSAGES.add(reader.decode(record.value().getBytes(UTF_8)));
            }
        }
        assertThat(until.test(MESSAGES)).as("downstream messages %s"
                + "\nsplitter:\n%s", MESSAGES, splitterLog()).isTrue();
    }

    /**
     * How many items the splitter has completed, by downstream message.
     *
     * @param messages the messages read
     * @return the number of {@code file.completed} messages
     */
    static long completed(List<ConnectorMessage> messages)
    {
        return messages.stream().filter(FileCompleted.class::isInstance)
                .count();
    }

    /**
     * The splitter's whole console output, across restarts.
     *
     * @return the output
     */
    static String splitterLog()
    {
        return app == null ? APP_LOG.toUtf8String() : app.getLogs();
    }

    /**
     * A labelled named volume, so Ryuk cleans it up if the test dies.
     *
     * @param purpose what it's for, in its name
     * @return the volume's name
     */
    static String volume(String purpose)
    {
        String name = "sdh-image-it-" + purpose + "-" + UUID.randomUUID();
        Map<String, String> labels =
                new HashMap<>(DockerClientFactory.DEFAULT_LABELS);
        labels.put(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL,
                DockerClientFactory.SESSION_ID);
        DockerClientFactory.lazyClient().createVolumeCmd().withName(name)
                .withLabels(labels).exec();
        VOLUMES.add(name);
        return name;
    }

    /**
     * Fills the volumes as Kubernetes would: secret files root-owned, group
     * 10001 (the fsGroup), mode 0440; the intake directory owned by the app
     * user.
     *
     * @throws Exception if a key file can't be read or the copy fails
     */
    static void prepareVolumes() throws Exception
    {
        Map<String, Path> files = new LinkedHashMap<>();
        files.put("keks.p12", keys.keystore);
        files.put("signing.pem", keys.signingPem);
        files.put("recipient.pem", keys.recipientPem);
        files.put("inbox.pem", keys.inboxPem);
        files.put("landing-signer.pem", keys.landingSignerPem);
        files.put("landing-signing.pem", keys.landingSigningPem);
        files.put("inbox-public.pem", keys.inboxPublicPem);
        GenericContainer<?> init = new GenericContainer<>("busybox:1.37.0");
        for (Map.Entry<String, Path> file : files.entrySet())
        {
            init.withCopyToContainer(
                    Transferable.of(Files.readAllBytes(file.getValue())),
                    "/seed/" + file.getKey());
        }
        try (init)
        {
            init.withCreateContainerCmdModifier(command -> command
                            .getHostConfig().withBinds(
                                    new Bind(secrets, new Volume("/secrets")),
                                    new Bind(intake, new Volume("/intake"))))
                    .withCommand("sh", "-c", "cp /seed/* /secrets/"
                            + " && chown 0:" + APP_UID + " /secrets/* "
                            + "&& chmod 0440 /secrets/* && chown " + APP_UID
                            + ":" + APP_UID + " /intake "
                            + "&& chmod 0770 /intake")
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                            .withTimeout(Duration.ofMinutes(1)))
                    .start();
        }
    }

    /**
     * The settings a deployment would put in the landing node's
     * application.yml, addressed by network alias.
     *
     * @param standby whether the node starts as a standby
     * @return the settings
     * @throws Exception if the inbox key's fingerprint can't be made
     */
    static Map<String, Object> landingConfig(boolean standby) throws Exception
    {
        Map<String, Object> sdh = new LinkedHashMap<>();
        sdh.put("connector-id", "landing-" + INTAKE);
        sdh.put("landing", Map.of("intake", INTAKE,
                "standby", standby,
                "source", Map.of("path", INTAKE_DIR, "access", "read-write",
                        "after", "move", "move-to", LANDED_DIR,
                        "settle", "1s", "poll-interval", "200ms")));
        sdh.put("storage", Map.of("catalog-name", "sdh",
                "catalog", containerCatalog()));
        // A short lease, so the standby takes over within seconds of the
        // active node dying.
        sdh.put("lease", Map.of("jdbc", leaseJdbc(), "ttl", "15s",
                "renew-interval", "4s", "safety-margin", "3s"));
        sdh.put("crypto", Map.of(
                "keystore", Map.of("path", "/run/secrets/keks.p12"),
                "keys", Map.of(
                        "landed-items-kek", TestKeys.LANDED,
                        "signing", Map.of("kid", TestKeys.LANDING_SIGNER_KID,
                                "pem", "/run/secrets/landing-signing.pem"),
                        "recipient", Map.of("kid", TestKeys.INBOX_KID,
                                "pem", "/run/secrets/inbox-public.pem",
                                "sha256", KeyMaterial.fingerprint(
                                        keys.inbox.getPublic())))));
        sdh.put("queue", Map.of("type", "kafka",
                "uri", "kafka:" + LANDING_TOPIC + "?brokers=kafka:19092"));
        return Map.of("sdh", sdh);
    }

    /**
     * The settings a deployment would put in the splitter's
     * application.yml, addressed by network alias.
     *
     * @return the settings
     * @throws Exception if the recipient key's fingerprint can't be made
     */
    static Map<String, Object> splitterConfig() throws Exception
    {
        Map<String, Object> sdh = new LinkedHashMap<>();
        // Offsets are committed automatically, one message per poll, so
        // only for messages already handled; breakOnFirstError keeps a
        // failed message from being skipped (design §5.3.12).
        sdh.put("landing", Map.of("intake", INTAKE,
                "queue-uri", "kafka:" + LANDING_TOPIC
                        + "?brokers=kafka:19092&groupId=file-connector"
                        + "&autoOffsetReset=earliest&maxPollRecords=1"
                        + "&breakOnFirstError=true"));
        sdh.put("split", Map.of("format", "csv",
                "options", Map.of("csv", Map.of("header", "true"))));
        sdh.put("storage", Map.of("catalog-name", "sdh",
                "catalog", containerCatalog()));
        sdh.put("lease", Map.of("jdbc", leaseJdbc()));
        sdh.put("crypto", Map.of(
                "keystore", Map.of("path", "/run/secrets/keks.p12"),
                "keys", Map.of(
                        "sections-kek", TestKeys.SECTIONS,
                        "landed-items-kek", TestKeys.LANDED,
                        "signing", Map.of("kid", "connector-2026",
                                "pem", "/run/secrets/signing.pem"),
                        "recipient", Map.of("kid", "downstream-2026",
                                "pem", "/run/secrets/recipient.pem",
                                "sha256", KeyMaterial.fingerprint(
                                        keys.recipient.getPublic())),
                        "inbox", Map.of("kid", TestKeys.INBOX_KID,
                                "pem", "/run/secrets/inbox.pem"),
                        "landing-signer", Map.of(
                                "kid", TestKeys.LANDING_SIGNER_KID,
                                "pem", "/run/secrets/landing-signer.pem"))));
        sdh.put("queue", Map.of("type", "kafka",
                "uri", "kafka:" + SECTIONS_TOPIC + "?brokers=kafka:19092"));
        return Map.of("sdh", sdh);
    }

    /**
     * The lease database settings, by network alias.
     *
     * @return the settings
     */
    static Map<String, Object> leaseJdbc()
    {
        return Map.of("url", "jdbc:postgresql://postgres:5432/"
                        + postgres.getDatabaseName(),
                "username", postgres.getUsername(),
                "password", postgres.getPassword());
    }

    /**
     * The catalog settings, by network alias.
     *
     * @return the settings
     */
    static Map<String, Object> containerCatalog()
    {
        return new LinkedHashMap<>(catalogProperties("http://seaweed:8333",
                "jdbc:postgresql://postgres:5432/"
                        + postgres.getDatabaseName()));
    }

    /**
     * The catalog's properties for an S3 endpoint and database.
     *
     * @param s3Endpoint the object store's endpoint
     * @param jdbcUrl the catalog database's URL
     * @return the properties
     */
    static Map<String, String> catalogProperties(String s3Endpoint,
            String jdbcUrl)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "jdbc");
        properties.put("uri", jdbcUrl);
        properties.put("jdbc.user", postgres.getUsername());
        properties.put("jdbc.password", postgres.getPassword());
        properties.put("jdbc.schema-version", "V1");
        properties.put("warehouse", "s3://" + BUCKET + "/wh");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", s3Endpoint);
        properties.put("s3.path-style-access", "true");
        properties.put("s3.access-key-id", ACCESS_KEY);
        properties.put("s3.secret-access-key", SECRET_KEY);
        properties.put("client.region", "us-east-1");
        return properties;
    }

    /**
     * The object store's endpoint as this JVM reaches it.
     *
     * @return the URL
     */
    static String hostS3()
    {
        return "http://" + seaweed.getHost() + ":"
                + seaweed.getMappedPort(8333);
    }

    /**
     * An S3 client for the object store.
     *
     * @return the client
     */
    static S3Client s3()
    {
        return S3Client.builder().endpointOverride(URI.create(hostS3()))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    /**
     * Runs a command in a running container.
     *
     * @param container the container
     * @param command the command and its arguments
     * @return the exit status, a colon, and the output
     * @throws Exception if the command can't be run
     */
    static String exec(GenericContainer<?> container, String... command)
            throws Exception
    {
        assertThat(container.isRunning()).as("container still running")
                .isTrue();
        ExecResult result = container.execInContainer(command);
        return result.getExitCode() + ":" + result.getStdout().strip()
                + result.getStderr().strip();
    }

    /**
     * A CSV file of records, each with the PHI marker and a quoted newline.
     *
     * @param tag tells this file's records from others'
     * @param count how many records
     * @param records receives each record
     * @return the file's content
     */
    static String csv(String tag, int count, List<String> records)
    {
        StringBuilder file = new StringBuilder(HEADER);
        for (int index = 0; index < count; index++)
        {
            String record = index + "," + MARKER + "-" + tag + "-" + index
                    + ",\"multi\nline\"\n";
            records.add(record);
            file.append(record);
        }
        return file.toString();
    }

    /**
     * Both stages run locked down, land, split, restore and survive the
     * loss of the active landing node and a splitter restart, without PHI
     * in any log.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void bothStagesRunLockedDownAndSurviveFailover() throws Exception
    {
        for (GenericContainer<?> container : List.of(app, landingActive))
        {
            // Runs as the image's non-root user.
            assertThat(DockerClientFactory.lazyClient()
                    .inspectImageCmd(container.getDockerImageName()).exec()
                    .getConfig().getUser())
                    .isEqualTo(APP_UID + ":" + APP_UID);
            if (hasShell())
            {
                // Nothing outside the volumes and /tmp is writable.
                assertThat(exec(container, "id", "-u"))
                        .isEqualTo("0:" + APP_UID);
                assertThat(exec(container, "touch", "/app/x")).startsWith("1:");
                assertThat(exec(container, "touch", "/x")).startsWith("1:");
            }
            else
            {
                assertThat(container.execInContainer("sh", "-c", "true")
                        .getExitCode()).as("no shell in the image")
                        .isNotZero();
            }
        }

        // 1. Landing, announcing and splitting: a good file and a bad one.
        List<String> records = new ArrayList<>();
        deliver("good.csv", csv("a", 20, records));
        String bad = HEADER + "1," + MARKER + ",\"never closed\n";
        deliver("bad.csv", bad);
        awaitLanded(2);
        // 2 × file.started, 20 × section.stored, 1 × file.completed (the bad
        // file stores nothing).
        awaitMessages(messages -> messages.stream()
                .filter(FileStarted.class::isInstance).count() == 2
                && completed(messages) == 1
                && messages.stream().filter(SectionStored.class::isInstance)
                        .count() == 20);
        UUID goodId = MESSAGES.stream().filter(FileCompleted.class::isInstance)
                .findFirst().orElseThrow().fileId();
        UUID badId = MESSAGES.stream().filter(FileStarted.class::isInstance)
                .map(ConnectorMessage::fileId)
                .filter(id -> !id.equals(goodId)).findFirst().orElseThrow();
        for (SectionStored section : MESSAGES.stream()
                .filter(SectionStored.class::isInstance)
                .map(SectionStored.class::cast).toList())
        {
            assertThat(new String(reader.readSection(section, 1 << 20), UTF_8))
                    .isEqualTo(csvRowJson(
                            records.get((int) section.seq())));
        }

        // 2. Operator commands inside the running splitter (design §11.4),
        // as `kubectl exec` would run them: same configuration from the
        // environment, same JVM options, no lease taken. The bad item is
        // restored from landing into /tmp and read out the way the README
        // describes.
        assertThat(admin("status")).startsWith("0:")
                .containsPattern("lease: +file-connector held by .+, epoch 1,")
                .contains("admin.status operator=it-operator");
        assertThat(admin("restore", "--file-id", badId.toString(), "--to",
                "/tmp")).startsWith("0:").contains("digest verified");
        assertThat(debugRead("/tmp/" + badId)).isEqualTo(bad);

        // 3. Landing failover: the active node dies (SIGKILL, no lease
        // release); the standby takes the lease once it lapses and lands
        // the next file.
        DockerClientFactory.lazyClient()
                .killContainerCmd(landingActive.getContainerId()).exec();
        List<String> afterFailover = new ArrayList<>();
        deliver("next.csv", csv("b", 3, afterFailover));
        awaitLanded(3);
        assertThat(STANDBY_LOG.toUtf8String())
                .contains("Started LandingConnectorApplication");
        awaitMessages(messages -> completed(messages) == 2);

        // 4. The splitter restarts (as a pod restart would) and splits the
        // next file.
        DockerClientFactory.lazyClient()
                .restartContainerCmd(app.getContainerId()).withTimeout(30)
                .exec();
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline
                && splitterLog().split("Started FileConnectorApplication", -1)
                        .length < 3)
        {
            Thread.sleep(500);
        }
        assertThat(splitterLog().split("Started FileConnectorApplication", -1))
                .as("splitter restarted").hasSize(3);
        deliver("last.csv", csv("c", 2, new ArrayList<>()));
        awaitLanded(4);
        awaitMessages(messages -> completed(messages) == 3);
        assertThat(admin("status")).startsWith("0:")
                .containsPattern("lease: +file-connector held by .+, epoch 2,");

        // 5. No container's console output (what a log shipper would
        // collect) holds PHI or filenames.
        for (String log : List.of(splitterLog(), ACTIVE_LOG.toUtf8String(),
                STANDBY_LOG.toUtf8String()))
        {
            assertThat(log).doesNotContain(MARKER).doesNotContain(".csv");
        }
        assertThat(ACTIVE_LOG.toUtf8String())
                .contains("Started LandingConnectorApplication");
        // Native libraries load from /app/native, so nothing was unpacked
        // into /tmp (mounted noexec here). Without a shell, the work itself
        // shows zstd loaded: sections and landed chunks couldn't be
        // compressed otherwise.
        if (hasShell())
        {
            for (GenericContainer<?> container :
                    List.of(app, landingStandby))
            {
                assertThat(exec(container, "sh", "-c",
                        "ls -A /tmp | grep -ci 'zstd\\|snappy\\|hs_err'"))
                        .isEqualTo("1:0");
            }
        }

        // 6. Third-party attribution ships in both images (design §14.2).
        checkAttribution(app);
        checkAttribution(landingStandby);
    }

    /**
     * Our notice, every component with its license and NOTICE, the license
     * texts, the native libraries' sources, and every OS package the base
     * image brings.
     *
     * @param container a running container of the image
     * @throws Exception if a file can't be read from the container
     */
    void checkAttribution(GenericContainer<?> container) throws Exception
    {
        assertThat(read(container, "/app/legal/LICENSE"))
                .contains("Smile Digital Health Inc.");
        assertThat(read(container, "/app/legal/NOTICE"))
                .contains("Smile Digital Health Inc.");
        if (container == app)
        {
            assertThat(read(container, "/app/legal/NOTICE"))
                    .contains("derived from the HL7 CDA Release 2");
        }
        String thirdParty = read(container, "/app/legal/THIRD-PARTY.txt");
        assertThat(thirdParty)
                .contains("Coordinates: com.github.luben:zstd-jni:")
                .contains("Coordinates: org.xerial.snappy:snappy-java:")
                .doesNotContain("PLACEHOLDER");
        // Every component offered under a choice of licenses names the one
        // we distribute it under, and none is a GNU license where something
        // else was offered (design §14.2).
        List<String> lines = thirdParty.lines().toList();
        for (int index = 0; index < lines.size(); index++)
        {
            if (lines.get(index).startsWith("Licenses: ")
                    && lines.get(index).contains(","))
            {
                String elected = lines.get(index + 1);
                assertThat(elected).as("election for " + lines.get(index))
                        .startsWith("Elected license: ")
                        .doesNotContain("NONE").doesNotContain("GPL");
            }
        }
        assertThat(thirdParty).contains("Elected license: Apache-2.0")
                .contains("Elected license: EPL-2.0");
        assertThat(read(container, "/app/legal/licenses/apache-2.0.txt"))
                .contains("Apache License");
        assertThat(read(container, "/app/legal/licenses/mit.txt"))
                .contains("MIT License");

        // What the base image brings is attributed too: every OS package,
        // with its source package and a copyright file that exists in the
        // image.
        List<String[]> packages = read(container, "/app/legal/os-packages.txt")
                .lines().filter(line -> !line.startsWith("#"))
                .map(line -> line.split(" ")).toList();
        assertThat(packages).anySatisfy(fields ->
                assertThat(fields).startsWith("libc6").contains("glibc"));
        for (String[] fields : packages)
        {
            assertThat(fields).as(String.join(" ", fields)).hasSize(5);
            assertThat(read(container, fields[4]))
                    .as("copyright of " + fields[0]).isNotBlank();
        }
        checkBaseImage(container,
                packages.stream().map(fields -> fields[0]).toList());
    }
}
