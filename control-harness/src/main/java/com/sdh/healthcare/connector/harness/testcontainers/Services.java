/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.testcontainers;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.vault.VaultContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The services the Testcontainers target's pipeline runs on, as the
 * security ITs run them (CONTROL-API.md): Postgres over verified TLS with
 * a password, Kafka over SASL_SSL (SCRAM) with a user per stage, SeaweedFS
 * as S3 with signed requests, Vault's transit engine for the KEKs and
 * signing keys, and an OpenTelemetry Collector. Every password and token
 * is random, made for the run.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Services implements AutoCloseable
{

    /** The environment variable a stage's Kafka login is in. */
    static final String JAAS_ENV = "SDH_KAFKA_JAAS";

    /** The Postgres image, as the ITs use. */
    static final String POSTGRES_IMAGE = "postgres:18.4";

    /** The Kafka image. */
    static final String KAFKA_IMAGE = "apache/kafka:4.2.2";

    /** The object store's image. */
    static final String SEAWEED_IMAGE = "chrislusf/seaweedfs:4.48";

    /** Vault's image. */
    static final String VAULT_IMAGE = "hashicorp/vault:2.1.1";

    /** The collector's image. */
    static final String COLLECTOR_IMAGE =
            "otel/opentelemetry-collector-contrib:0.162.0";

    /** Kafka's SASL mechanism. */
    static final String MECHANISM = "SCRAM-SHA-512";

    /** The S3 access key. */
    static final String ACCESS_KEY = "sdh-harness";

    /** Only TLS with a password reaches Postgres. */
    static final String HBA = """
            local all all trust
            hostssl all all all scram-sha-256
            host all all all reject
            """;

    /** The collector: OTLP in, a file out. */
    static final String COLLECTOR_CONFIG = """
            receivers:
              otlp:
                protocols:
                  http:
                    endpoint: 0.0.0.0:4318
            exporters:
              file:
                path: /tmp/otel-out.json
            service:
              pipelines:
                traces: {receivers: [otlp], exporters: [file]}
                metrics: {receivers: [otlp], exporters: [file]}
                logs: {receivers: [otlp], exporters: [file]}
            """;

    /** Calls Vault's API. */
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    /** Reads Vault's answers. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The Docker host's address, as this JVM reaches the containers. */
    final String host;

    /** The CA every server certificate is from, as a file. */
    final Path caFile;

    /** Postgres's password. */
    final String postgresPassword = random();

    /** The S3 secret key. */
    final String s3Secret = random();

    /** Vault's root token. */
    final String vaultToken = random();

    /** Each Kafka user's password: the stages' and admin's. */
    final Map<String, String> kafkaPasswords = new LinkedHashMap<>();

    /** Kafka's client port, bound to the same port on the host. */
    final int kafkaPort;

    /** Postgres. */
    final PostgreSQLContainer postgres;

    /** Kafka. */
    final GenericContainer<?> kafka;

    /** SeaweedFS. */
    final GenericContainer<?> seaweed;

    /** Vault. */
    final VaultContainer<?> vault;

    /** The collector. */
    final GenericContainer<?> collector;

    /**
     * Starts the services.
     *
     * @param pki    issues their certificates
     * @param users  the Kafka users, one a stage
     * @param topics the topics to create
     * @throws Exception if one can't start
     */
    @SuppressWarnings("resource")
    Services(Pki pki, List<String> users, List<String> topics)
            throws Exception
    {
        host = DockerClientFactory.instance().dockerHostIpAddress();
        caFile = pki.caFile();
        Pki.Issued database = pki.server(host, "localhost");
        postgres = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withDatabaseName("harness").withUsername("harness")
                .withPassword(postgresPassword);
        postgres.withCopyToContainer(Transferable.of(pki.chainPem(database)),
                        "/tmp/sdh/server.crt")
                .withCopyToContainer(Transferable.of(Pki.keyPem(database)),
                        "/tmp/sdh/server.key")
                .withCopyToContainer(Transferable.of(HBA),
                        "/tmp/sdh/pg_hba.conf")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "sh", "-c", "install -o postgres -m 0600"
                                + " /tmp/sdh/server.key /tmp/sdh/server.crt"
                                + " /tmp/sdh/pg_hba.conf /var/lib/postgresql/"
                                + " && exec docker-entrypoint.sh postgres"
                                + " -c ssl=on -c max_connections=300"
                                + " -c ssl_cert_file=/var/lib/postgresql/"
                                + "server.crt"
                                + " -c ssl_key_file=/var/lib/postgresql/"
                                + "server.key"
                                + " -c hba_file=/var/lib/postgresql/"
                                + "pg_hba.conf"));

        kafkaPort = 20_000 + new SecureRandom().nextInt(20_000);
        kafkaPasswords.put("admin", random());
        for (String user : users)
        {
            kafkaPasswords.put(user, random());
        }
        StringBuilder format = new StringBuilder(
                "/opt/kafka/bin/kafka-storage.sh format -t "
                        + Uuid.randomUuid()
                        + " -c /tmp/sdh/server.properties");
        kafkaPasswords.forEach((user, password) -> format
                .append(" --add-scram '").append(MECHANISM)
                .append("=[name=").append(user).append(",password=")
                .append(password).append("]'"));
        String storePassword = random();
        kafka = new GenericContainer<>(KAFKA_IMAGE)
                .withCopyToContainer(Transferable.of(kafkaProperties(users,
                        storePassword)), "/tmp/sdh/server.properties")
                .withCopyToContainer(Transferable.of(pki.keyStore(
                        pki.server(host, "localhost"),
                        storePassword.toCharArray())), "/tmp/sdh/server.p12")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "sh", "-c", format + " && exec"
                                + " /opt/kafka/bin/kafka-server-start.sh"
                                + " /tmp/sdh/server.properties"))
                .withExposedPorts(kafkaPort)
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        kafka.setPortBindings(List.of(kafkaPort + ":" + kafkaPort));

        String s3Config = """
                {"identities":[{"name":"sdh","credentials":\
                [{"accessKey":"%s","secretKey":"%s"}],
                  "actions":["Admin","Read","Write","List","Tagging"]}]}"""
                .formatted(ACCESS_KEY, s3Secret);
        seaweed = new GenericContainer<>(SEAWEED_IMAGE)
                .withCopyToContainer(Transferable.of(s3Config),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        "-ip.bind=0.0.0.0", "-volume.max=20")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        vault = new VaultContainer<>(VAULT_IMAGE).withVaultToken(vaultToken);
        collector = new GenericContainer<>(COLLECTOR_IMAGE)
                .withCopyToContainer(Transferable.of(COLLECTOR_CONFIG),
                        "/etc/otelcol-contrib/config.yaml")
                .withCopyToContainer(Transferable.of("", 0666),
                        "/tmp/otel-out.json")
                .withExposedPorts(4318)
                .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
        for (GenericContainer<?> container : List.of(postgres, kafka,
                seaweed, vault, collector))
        {
            container.start();
        }
        try (Admin admin = Admin.create(kafkaClient("admin")))
        {
            List<NewTopic> created = new ArrayList<>();
            for (String topic : topics)
            {
                created.add(new NewTopic(topic, 4, (short) 1));
            }
            admin.createTopics(created).all().get();
        }
        vault("sys/mounts/transit", "{\"type\":\"transit\"}");
    }

    /**
     * Kafka's settings: SASL_SSL with SCRAM for clients, the stages' users
     * super users (KafkaSecurityIT tests their ACLs), no topic made on
     * demand.
     *
     * @param users         the stages' users
     * @param storePassword the key store's password
     * @return the properties file
     */
    private String kafkaProperties(List<String> users, String storePassword)
    {
        StringBuilder supers = new StringBuilder("User:admin;User:ANONYMOUS");
        users.forEach(user -> supers.append(";User:").append(user));
        return String.join("\n",
                "process.roles=broker,controller",
                "node.id=1",
                "controller.quorum.voters=1@127.0.0.1:9094",
                "listeners=CONTROLLER://127.0.0.1:9094,"
                        + "BROKER://127.0.0.1:9095,CLIENT://0.0.0.0:"
                        + kafkaPort,
                "advertised.listeners=BROKER://127.0.0.1:9095,"
                        + "CLIENT://" + host + ":" + kafkaPort,
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,"
                        + "BROKER:PLAINTEXT,CLIENT:SASL_SSL",
                "controller.listener.names=CONTROLLER",
                "inter.broker.listener.name=BROKER",
                "sasl.enabled.mechanisms=" + MECHANISM,
                "listener.name.client.scram-sha-512.sasl.jaas.config="
                        + "org.apache.kafka.common.security.scram"
                        + ".ScramLoginModule required;",
                "ssl.keystore.type=PKCS12",
                "ssl.keystore.location=/tmp/sdh/server.p12",
                "ssl.keystore.password=" + storePassword,
                "ssl.key.password=" + storePassword,
                "authorizer.class.name=org.apache.kafka.metadata.authorizer"
                        + ".StandardAuthorizer",
                "super.users=" + supers,
                "allow.everyone.if.no.acl.found=false",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "group.initial.rebalance.delay.ms=0",
                "log.dirs=/tmp/kraft-logs", "");
    }

    /**
     * A user's JAAS login.
     *
     * @param user the user
     * @return the login
     */
    String jaas(String user)
    {
        return "org.apache.kafka.common.security.scram.ScramLoginModule"
                + " required username=\"" + user + "\" password=\""
                + kafkaPasswords.get(user) + "\";";
    }

    /**
     * A Camel Kafka endpoint, as a stage's settings name it. Its login is
     * a placeholder, as a stage takes no secret in a URI: the stage's
     * process has it in {@link #JAAS_ENV}, from {@link #jaas(String)}.
     *
     * @param topic   the topic
     * @param user    the stage's user, unused while the login is in the
     *                process's environment
     * @param options further options, such as {@code &groupId=g}; may be
     *                empty
     * @return the URI
     */
    String kafkaUri(String topic, String user, String options)
    {
        return "kafka:" + topic + "?brokers=" + host + ":" + kafkaPort
                + "&securityProtocol=SASL_SSL&saslMechanism=" + MECHANISM
                + "&saslJaasConfig=RAW({{env:" + JAAS_ENV + "}})"
                + "&sslTruststoreType=PEM&sslTruststoreLocation=" + caFile
                + options;
    }

    /**
     * A Kafka client's settings, as a user.
     *
     * @param user the user
     * @return the settings
     */
    Properties kafkaClient(String user)
    {
        Properties client = new Properties();
        client.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                host + ":" + kafkaPort);
        client.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        client.put(SaslConfigs.SASL_MECHANISM, MECHANISM);
        client.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(user));
        client.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        client.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                caFile.toString());
        return client;
    }

    /**
     * Postgres's JDBC URL, over verified TLS.
     *
     * @return the URL
     */
    String jdbcUrl()
    {
        return "jdbc:postgresql://" + host + ":"
                + postgres.getMappedPort(5432) + "/harness?sslmode=verify-full"
                + "&sslrootcert=" + caFile;
    }

    /**
     * The JDBC catalog's settings for a bucket, under a settings prefix.
     *
     * @param prefix such as {@code sdh.storage.catalog}
     * @param bucket the bucket the warehouse is in
     * @return the settings
     */
    Map<String, String> catalog(String prefix, String bucket)
    {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put(prefix + "[type]", "jdbc");
        settings.put(prefix + "[uri]", "jdbc:postgresql://" + host + ":"
                + postgres.getMappedPort(5432) + "/harness");
        settings.put(prefix + "[jdbc.user]", "harness");
        settings.put(prefix + "[jdbc.password]", postgresPassword);
        settings.put(prefix + "[jdbc.sslmode]", "verify-full");
        settings.put(prefix + "[jdbc.sslrootcert]", caFile.toString());
        settings.put(prefix + "[jdbc.schema-version]", "V1");
        settings.put(prefix + "[warehouse]", "s3://" + bucket + "/warehouse");
        settings.put(prefix + "[io-impl]",
                "org.apache.iceberg.aws.s3.S3FileIO");
        settings.put(prefix + "[s3.endpoint]", s3Endpoint());
        settings.put(prefix + "[s3.path-style-access]", "true");
        settings.put(prefix + "[s3.access-key-id]", ACCESS_KEY);
        settings.put(prefix + "[s3.secret-access-key]", s3Secret);
        settings.put(prefix + "[client.region]", "us-east-1");
        return settings;
    }

    /**
     * The object store's endpoint.
     *
     * @return the URL
     */
    String s3Endpoint()
    {
        return "http://" + host + ":" + seaweed.getMappedPort(8333);
    }

    /**
     * An S3 client of the object store.
     *
     * @return the client
     */
    S3Client s3()
    {
        return S3Client.builder().endpointOverride(URI.create(s3Endpoint()))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, s3Secret)))
                .build();
    }

    /**
     * Vault's address.
     *
     * @return the URL
     */
    String vaultAddress()
    {
        return vault.getHttpHostAddress();
    }

    /**
     * The collector's OTLP endpoint.
     *
     * @return the URL
     */
    String otlpEndpoint()
    {
        return "http://" + host + ":" + collector.getMappedPort(4318);
    }

    /**
     * Creates a transit key.
     *
     * @param name its name
     * @param type {@code aes256-gcm96} for a KEK, {@code ecdsa-p256} for a
     *             signing key
     * @throws Exception if Vault refuses
     */
    void transitKey(String name, String type) throws Exception
    {
        vault("transit/keys/" + name, "{\"type\":\"" + type + "\"}");
    }

    /**
     * A transit signing key's public half, as PEM.
     *
     * @param name the key
     * @return its latest version's PEM
     * @throws Exception if Vault refuses
     */
    String transitPublicKey(String name) throws Exception
    {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                URI.create(vaultAddress() + "/v1/transit/keys/" + name))
                .header("X-Vault-Token", vaultToken).GET().build(),
                HttpResponse.BodyHandlers.ofString(UTF_8));
        if (response.statusCode() != 200)
        {
            throw new IllegalStateException("Vault read of " + name + ": "
                    + response.statusCode());
        }
        JsonNode data = JSON.readTree(response.body()).path("data");
        return data.path("keys").path(data.path("latest_version").asText())
                .path("public_key").asText();
    }

    /**
     * Posts to Vault's API as root.
     *
     * @param path the path under {@code /v1}
     * @param body the body
     * @throws Exception if Vault refuses
     */
    private void vault(String path, String body) throws Exception
    {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(
                URI.create(vaultAddress() + "/v1/" + path))
                .header("X-Vault-Token", vaultToken)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString(UTF_8));
        if (response.statusCode() / 100 != 2)
        {
            throw new IllegalStateException("Vault " + path + ": "
                    + response.statusCode());
        }
    }

    /**
     * Pauses or unpauses a container, as an outage of its service.
     *
     * @param container the container
     * @param paused    whether to pause it
     */
    static void pause(GenericContainer<?> container, boolean paused)
    {
        var docker = DockerClientFactory.instance().client();
        if (paused)
        {
            docker.pauseContainerCmd(container.getContainerId()).exec();
        }
        else
        {
            docker.unpauseContainerCmd(container.getContainerId()).exec();
        }
    }

    /**
     * A random secret.
     *
     * @return 32 hex digits
     */
    static String random()
    {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    /** Stops every service. */
    @Override
    public void close()
    {
        for (GenericContainer<?> container : List.of(collector, vault,
                seaweed, kafka, postgres))
        {
            try
            {
                container.stop();
            }
            catch (RuntimeException failure)
            {
                // Stopping the others matters more; Ryuk removes it.
            }
        }
    }
}
