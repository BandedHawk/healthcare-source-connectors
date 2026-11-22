/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.testcontainers;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Target;
import com.sdh.healthcare.connector.harness.TestIssuer;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The harness's first level (CONTROL-API-DESIGN.md §14, step H1): a whole
 * pipeline (landing node, split stage, convert stage) on the services as
 * the security ITs run them ({@link Services}), each stage run from the
 * jar its build made, as a worker and as its control API, two processes a
 * stage. Its faults are a service's container paused, and back.
 *
 * <p>Chosen here, and listed in the design: the stages run as processes,
 * not images, so the control APIs reach the test issuer on this machine
 * (the sandbox target runs the images); one database user and one S3 key
 * for every stage, and the stages' Kafka users super users (the isolation
 * ITs test the per-stage rights); the compliance profile off, since Vault
 * is reached over HTTP; synthetic C-CDA only.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestcontainersTarget implements Target
{

    /** The pipeline's name: its intake, topics and namespaces. */
    static final String PIPELINE = "harness";

    /** The bucket the landing node writes. */
    static final String BRONZE = "sdh-bronze";

    /** The bucket the stages write. */
    static final String SILVER = "sdh-silver";

    /** The producer the intake lists. */
    static final String PRODUCER = "CN=harness-producer,O=Example";

    /** Each stage's module, by stage. */
    static final Map<String, String> MODULES = Map.of(LANDING,
            "landing-connector", SPLIT, "file-connector", CONVERT,
            "convert-stage");

    /** Each stage's application, as its startup line names it. */
    static final Map<String, String> APPLICATIONS = Map.of(LANDING,
            "LandingConnectorApplication", SPLIT, "FileConnectorApplication",
            CONVERT, "ConvertStageApplication");

    /** How long a stage may take to start. */
    static final Duration START = Duration.ofMinutes(3);

    /** The repository's root, where the jars are. */
    private final Path root;

    /** Where the run's files and logs go. */
    private final Path work;

    /** The certificates and key files. */
    private final Pki pki;

    /** The tokens' issuer. */
    private final TestIssuer issuer;

    /** The services. */
    private final Services services;

    /** Each stage's settings, by stage. */
    private final Map<String, Map<String, String>> settings =
            new LinkedHashMap<>();

    /** Each stage's control port. */
    private final Map<String, Integer> controlPorts = new LinkedHashMap<>();

    /** The running processes, by name. */
    private final Map<String, StageProcess> processes =
            new LinkedHashMap<>();

    /** Every secret configured. */
    private final Set<String> secrets = new HashSet<>();

    /** Trusts the servers. */
    private final SSLContext trust;

    /** The listed producer's client certificate. */
    private final SSLContext producer;

    /** The intake's push port. */
    private final int pushPort;

    /**
     * Starts the services and the pipeline.
     *
     * @param root the repository's root, with the stages built
     * @throws Exception if something can't start
     */
    public TestcontainersTarget(Path root) throws Exception
    {
        this.root = root;
        work = root.resolve("control-harness").resolve("target")
                .resolve("harness").resolve(LocalDateTime.now().format(
                        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")));
        Files.createDirectories(work.resolve("keys"));
        pki = new Pki(work.resolve("keys"));
        issuer = new TestIssuer();
        trust = pki.context(null);
        producer = pki.context(pki.client(PRODUCER));
        Harness.say("Starting the services (Postgres, Kafka, SeaweedFS,"
                + " Vault, the collector)");
        services = new Services(pki, List.of(LANDING, SPLIT, CONVERT),
                List.of(topic("landed"), topic("split"), topic("records"),
                        topic("convert.dlq")));
        secrets.add(services.postgresPassword);
        secrets.add(services.s3Secret);
        secrets.add(services.vaultToken);
        secrets.addAll(services.kafkaPasswords.values());
        try (S3Client s3 = services.s3())
        {
            s3.createBucket(request -> request.bucket(BRONZE));
            s3.createBucket(request -> request.bucket(SILVER));
        }
        for (String kek : List.of(transitKek("landed-items"),
                transitKek("records"), transitKek("landed-items-2"),
                transitKek("records-2")))
        {
            services.transitKey(kek, "aes256-gcm96");
        }
        for (String stage : List.of(LANDING, SPLIT, CONVERT))
        {
            services.transitKey(signingKey(stage), "ecdsa-p256");
        }
        pushPort = freePort();
        configure();
        for (String stage : List.of(CONVERT, SPLIT, LANDING))
        {
            start(stage, false);
        }
        for (String stage : List.of(CONVERT, SPLIT, LANDING))
        {
            start(stage, true);
        }
    }

    /**
     * A topic of the pipeline.
     *
     * @param name its name within the pipeline, such as {@code landed}
     * @return the topic
     */
    static String topic(String name)
    {
        return PIPELINE + "." + name;
    }

    /**
     * A KEK of the pipeline, in Vault's transit engine.
     *
     * @param name its name within the pipeline
     * @return its reference
     */
    static String transitKek(String name)
    {
        return PIPELINE + "-" + name;
    }

    /**
     * A stage's signing key, in Vault's transit engine.
     *
     * @param stage the stage
     * @return its reference
     */
    static String signingKey(String stage)
    {
        return PIPELINE + "-" + stage + "-signing";
    }

    /**
     * Writes the stages' key files and builds their settings, as the
     * sandbox's are (deploy/sandbox/pipeline).
     *
     * @throws Exception if a key can't be made or read
     */
    private void configure() throws Exception
    {
        String passphrase = Services.random();
        secrets.add(passphrase);
        KeyPair splitInbox = Pki.ec();
        KeyPair convertInbox = Pki.ec();
        KeyPair reader = Pki.ec();
        Path splitInboxPem = pki.encryptedKey("split-inbox", splitInbox,
                passphrase);
        Path convertInboxPem = pki.encryptedKey("convert-inbox",
                convertInbox, passphrase);
        Path readerPem = pki.publicKey("reader", reader);
        Path landingSigner = pki.write("landing-signer.pem",
                services.transitPublicKey(signingKey(LANDING)));
        Path splitSigner = pki.write("split-signer.pem",
                services.transitPublicKey(signingKey(SPLIT)));
        Pki.Issued server = pki.server("localhost", "127.0.0.1");
        Path serverCertificate = pki.write("server.crt",
                pki.chainPem(server));
        Path serverKey = pki.write("server.key", Pki.keyPem(server));

        Map<String, String> landing = common(LANDING);
        landing.put("sdh.landing.intake", PIPELINE);
        landing.put("sdh.landing.source.type", "http");
        landing.put("sdh.landing.http.port", Integer.toString(pushPort));
        landing.put("sdh.landing.http.auth", "mtls");
        landing.put("sdh.landing.http.tls.certificate",
                serverCertificate.toString());
        landing.put("sdh.landing.http.tls.private-key", serverKey.toString());
        landing.put("sdh.landing.http.mtls.trusted-cas",
                services.caFile.toString());
        landing.put("sdh.landing.http.mtls.subjects[0]", PRODUCER);
        landing.put("sdh.storage.catalog-name", "sdh");
        landing.put("sdh.storage.namespace", PIPELINE + "_landing");
        landing.putAll(services.catalog("sdh.storage.catalog", BRONZE));
        landing.put("sdh.crypto.keys.landed-items-kek",
                transitKek("landed-items"));
        landing.put("sdh.crypto.keys.signing.kid", LANDING + "-" + PIPELINE);
        landing.put("sdh.crypto.keys.signing.key-ref", signingKey(LANDING));
        landing.put("sdh.crypto.keys.recipient.kid", "split-inbox");
        landing.put("sdh.crypto.keys.recipient.pem",
                splitInboxPem.resolveSibling("split-inbox-public.pem")
                        .toString());
        landing.put("sdh.crypto.keys.recipient.sha256",
                fingerprint(splitInbox));
        landing.put("sdh.queue.type", "kafka");
        landing.put("sdh.queue.uri", services.kafkaUri(topic("landed"),
                LANDING, ""));
        settings.put(LANDING, landing);

        Map<String, String> split = common(SPLIT);
        split.put("sdh.landing.intake", PIPELINE);
        split.put("sdh.landing.storage.catalog-name", "sdh");
        split.put("sdh.landing.storage.namespace", PIPELINE + "_landing");
        split.putAll(services.catalog("sdh.landing.storage.catalog",
                BRONZE));
        split.put("sdh.stage.input.type", "kafka");
        split.put("sdh.stage.input.uri", services.kafkaUri(topic("landed"),
                SPLIT, "&groupId=" + PIPELINE + "-split"
                        + "&groupProtocol=consumer"));
        split.put("sdh.stage.output.type", "kafka");
        split.put("sdh.stage.output.uri", services.kafkaUri(topic("split"),
                SPLIT, ""));
        split.put("sdh.stage.reprocess.type", "kafka");
        split.put("sdh.stage.reprocess.uri", services.kafkaUri(
                topic("landed"), SPLIT, ""));
        split.put("sdh.stage.registry-schema", "split_" + PIPELINE);
        split.put("sdh.stage.next-registry-schema", "convert_" + PIPELINE);
        split.put("sdh.stage.ready-port", "0");
        split.put("sdh.split.format", "ccda");
        split.put("sdh.storage.catalog-name", "sdh");
        split.put("sdh.storage.namespace", PIPELINE);
        split.put("sdh.storage.commit.max-records", "1");
        split.putAll(services.catalog("sdh.storage.catalog", SILVER));
        split.put("sdh.crypto.keys.records-kek", transitKek("records"));
        split.put("sdh.crypto.keys.landed-items-kek",
                transitKek("landed-items"));
        split.put("sdh.crypto.keys.signing.kid", SPLIT + "-" + PIPELINE);
        split.put("sdh.crypto.keys.signing.key-ref", signingKey(SPLIT));
        split.put("sdh.crypto.keys.recipient.kid", "convert-inbox");
        split.put("sdh.crypto.keys.recipient.pem", convertInboxPem
                .resolveSibling("convert-inbox-public.pem").toString());
        split.put("sdh.crypto.keys.recipient.sha256",
                fingerprint(convertInbox));
        inbox(split, "split-inbox", splitInboxPem, passphrase, splitInbox);
        split.put("sdh.crypto.keys.landing-signer.kid",
                LANDING + "-" + PIPELINE);
        split.put("sdh.crypto.keys.landing-signer.pem",
                landingSigner.toString());
        settings.put(SPLIT, split);

        Map<String, String> convert = common(CONVERT);
        convert.put("sdh.stage.input.type", "kafka");
        convert.put("sdh.stage.input.uri", services.kafkaUri(topic("split"),
                CONVERT, "&groupId=" + PIPELINE + "-convert"
                        + "&groupProtocol=consumer"));
        convert.put("sdh.stage.output.type", "kafka");
        convert.put("sdh.stage.output.uri", services.kafkaUri(
                topic("records"), CONVERT, ""));
        convert.put("sdh.stage.dlq.type", "kafka");
        convert.put("sdh.stage.dlq.uri", services.kafkaUri(
                topic("convert.dlq"), CONVERT, ""));
        convert.put("sdh.stage.reprocess.type", "kafka");
        convert.put("sdh.stage.reprocess.uri", services.kafkaUri(
                topic("split"), CONVERT, ""));
        convert.put("sdh.stage.registry-schema", "convert_" + PIPELINE);
        convert.put("sdh.stage.ready-port", "0");
        convert.put("sdh.stage.source.catalog-name", "sdh");
        convert.put("sdh.stage.source.namespace", PIPELINE);
        convert.putAll(services.catalog("sdh.stage.source.catalog", SILVER));
        convert.put("sdh.stage.sink.catalog-name", "sdh");
        convert.put("sdh.stage.sink.namespace", PIPELINE);
        convert.put("sdh.crypto.keys.records-kek", transitKek("records"));
        convert.put("sdh.crypto.keys.signing.kid", CONVERT + "-" + PIPELINE);
        convert.put("sdh.crypto.keys.signing.key-ref", signingKey(CONVERT));
        convert.put("sdh.crypto.keys.recipient.kid", "reader-" + PIPELINE);
        convert.put("sdh.crypto.keys.recipient.pem", readerPem.toString());
        convert.put("sdh.crypto.keys.recipient.sha256", fingerprint(reader));
        inbox(convert, "convert-inbox", convertInboxPem, passphrase,
                convertInbox);
        convert.put("sdh.crypto.keys.landing-signer.kid",
                SPLIT + "-" + PIPELINE);
        convert.put("sdh.crypto.keys.landing-signer.pem",
                splitSigner.toString());
        settings.put(CONVERT, convert);

        for (String stage : List.of(LANDING, SPLIT, CONVERT))
        {
            controlPorts.put(stage, freePort());
            Map<String, String> each = settings.get(stage);
            each.put("sdh.control.tls.certificate",
                    serverCertificate.toString());
            each.put("sdh.control.tls.key", serverKey.toString());
            each.put("sdh.control.auth.jwt.jwk-set-uri", issuer.jwksUri());
            each.put("sdh.control.auth.jwt.issuer-uri", TestIssuer.ISSUER);
            each.put("sdh.control.auth.jwt.audience", TestIssuer.AUDIENCE);
            Target.permissions().forEach((permission, roles) -> each.put(
                    "sdh.control.permissions[" + permission + "]",
                    String.join(",", roles)));
        }
    }

    /**
     * The settings every stage has: its ID, the database, Vault, the
     * collector, maintenance every 20 seconds, and the profile.
     *
     * @param stage the stage
     * @return the settings, to add to
     */
    private Map<String, String> common(String stage)
    {
        Map<String, String> each = new LinkedHashMap<>();
        // Synthetic data, on Vault over HTTP: the profile is off, as in
        // the sandbox (deploy/sandbox/pipeline).
        each.put("sdh.compliance.profile", "none");
        each.put("sdh.connector-id", stage + "-" + PIPELINE);
        each.put("sdh.lease.jdbc.url", services.jdbcUrl());
        each.put("sdh.lease.jdbc.username", "harness");
        each.put("sdh.lease.jdbc.password", services.postgresPassword);
        each.put("sdh.crypto.wrap-provider", "vault-transit");
        each.put("sdh.crypto.sign-provider", "vault-transit");
        each.put("sdh.crypto.vault.address", services.vaultAddress());
        each.put("sdh.crypto.vault.token", services.vaultToken);
        each.put("sdh.telemetry.otlp.endpoint", services.otlpEndpoint());
        // Maintenance carries out re-keys, purges and retention.
        each.put("sdh.maintenance.interval", "20s");
        each.put("logging.level.root", "WARN");
        each.put("logging.level.com.sdh", "INFO");
        each.put("logging.level.sdh", "INFO");
        each.put("logging.level.org.springframework.boot", "INFO");
        return each;
    }

    /**
     * A stage's inbox key: the private half, its passphrase, the public
     * half and its pin.
     *
     * @param settings   the stage's settings
     * @param kid        its key ID
     * @param pem        the private half's file
     * @param passphrase its passphrase
     * @param keys       the pair
     * @throws Exception if the pin can't be made
     */
    private static void inbox(Map<String, String> settings, String kid,
            Path pem, String passphrase, KeyPair keys) throws Exception
    {
        settings.put("sdh.crypto.keys.inbox.kid", kid);
        settings.put("sdh.crypto.keys.inbox.pem", pem.toString());
        settings.put("sdh.crypto.keys.inbox.passphrase", passphrase);
        settings.put("sdh.crypto.keys.inbox.public-pem", pem.resolveSibling(
                pem.getFileName().toString().replace(".pem", "-public.pem"))
                .toString());
        settings.put("sdh.crypto.keys.inbox.sha256", fingerprint(keys));
    }

    /**
     * A public key's pin: the SHA-256 of its SubjectPublicKeyInfo.
     *
     * @param keys the pair
     * @return the pin, in hex
     * @throws Exception if SHA-256 isn't available
     */
    static String fingerprint(KeyPair keys) throws Exception
    {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(keys.getPublic().getEncoded()));
    }

    /**
     * Starts a stage's worker or control API and waits until it is up.
     *
     * @param stage   the stage
     * @param control whether its control API
     * @return whether it started; false if it stopped instead
     * @throws Exception if it neither starts nor stops in time
     */
    private boolean start(String stage, boolean control) throws Exception
    {
        String name = stage + (control ? "-control" : "-worker");
        Map<String, String> each = new LinkedHashMap<>(settings.get(stage));
        if (control)
        {
            each.put("sdh.control.port",
                    Integer.toString(controlPorts.get(stage)));
            each.put("sdh.control.probe-port", Integer.toString(freePort()));
        }
        Harness.say("Starting " + name);
        StageProcess process = new StageProcess(name, StageProcess.jar(root,
                MODULES.get(stage)), control, each, Map.of(
                        Services.JAAS_ENV, services.jaas(stage)),
                work.resolve("logs")
                        .resolve(name + ".log"));
        processes.put(name, process);
        boolean started = process.await("Started "
                + (control ? "\\w+" : APPLICATIONS.get(stage)) + " in",
                START);
        if (!started && !control)
        {
            Harness.say(name + " stopped without starting (log: "
                    + work.resolve("logs").resolve(name + ".log") + ")");
        }
        else if (!started)
        {
            throw new IllegalStateException(name + " stopped; its log: "
                    + work.resolve("logs").resolve(name + ".log"));
        }
        return started;
    }

    /**
     * A free port on the loopback interface.
     *
     * @return the port
     * @throws IOException if none can be had
     */
    static int freePort() throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0))
        {
            return socket.getLocalPort();
        }
    }

    @Override
    public String name()
    {
        return "testcontainers";
    }

    @Override
    public List<String> stages()
    {
        return List.of(LANDING, SPLIT, CONVERT);
    }

    @Override
    public URI control(String stage)
    {
        return URI.create("https://127.0.0.1:" + controlPorts.get(stage)
                + "/api/v1");
    }

    @Override
    public SSLContext trust()
    {
        return trust;
    }

    @Override
    public TestIssuer issuer()
    {
        return issuer;
    }

    @Override
    public Reply push(byte[] body, Map<String, String> headers)
            throws Exception
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                "https://127.0.0.1:" + pushPort + "/intakes/" + PIPELINE
                        + "/items"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/octet-stream");
        headers.forEach(request::header);
        HttpResponse<String> response = HttpClient.newBuilder()
                .sslContext(producer).connectTimeout(Duration.ofSeconds(10))
                .build().send(request.POST(HttpRequest.BodyPublishers
                        .ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofString());
        return new Reply("intake", "POST", "/intakes/" + PIPELINE + "/items",
                PRODUCER, response.statusCode(), response.headers().map(),
                response.body());
    }

    @Override
    public void cut(Dependency dependency)
    {
        Services.pause(container(dependency), true);
    }

    @Override
    public void restore(Dependency dependency)
    {
        Services.pause(container(dependency), false);
    }

    /**
     * A service's container.
     *
     * @param dependency the service
     * @return its container
     */
    private org.testcontainers.containers.GenericContainer<?> container(
            Dependency dependency)
    {
        return switch (dependency)
        {
            case DATABASE -> services.postgres;
            case BROKER -> services.kafka;
            case STORE -> services.seaweed;
            case KMS -> services.vault;
        };
    }

    @Override
    public boolean restartWorkers(String stage) throws Exception
    {
        StageProcess worker = processes.remove(stage + "-worker");
        if (worker != null)
        {
            worker.close();
        }
        return start(stage, false);
    }

    @Override
    public String kek(String stage)
    {
        return transitKek(stage.equals(LANDING) ? "landed-items"
                : "records");
    }

    @Override
    public String otherKek(String stage)
    {
        return transitKek(stage.equals(LANDING) ? "landed-items-2"
                : "records-2");
    }

    @Override
    public Set<String> secrets()
    {
        return Set.copyOf(secrets);
    }

    @Override
    public List<String> stored(String stage)
    {
        String bucket = stage.equals(LANDING) ? BRONZE : SILVER;
        String prefix = switch (stage)
        {
            case LANDING -> "warehouse/" + PIPELINE + "_landing/";
            case SPLIT -> "warehouse/" + PIPELINE + "/split_records";
            default -> "warehouse/" + PIPELINE + "/records";
        };
        List<String> keys = new ArrayList<>();
        try (S3Client s3 = services.s3())
        {
            s3.listObjectsV2Paginator(request -> request.bucket(bucket)
                    .prefix(prefix)).contents().stream().map(S3Object::key)
                    .forEach(keys::add);
        }
        return keys;
    }

    /** Stops the stages, then the services. */
    @Override
    public void close()
    {
        List<StageProcess> running = new ArrayList<>(processes.values());
        java.util.Collections.reverse(running);
        for (StageProcess process : running)
        {
            try
            {
                process.close();
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
        issuer.close();
        services.close();
        Harness.say("Logs and keys of the run: " + work);
    }
}
