/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.kms.VaultTransitKeyProvider;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.vault.VaultContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Two pipelines, {@code alpha} and {@code beta}, on shared services, laid
 * out as HIPAA-OPERATIONS.md §2.8 says to isolate customers, a hybrid
 * entity's components or Part 2 records (HIPAA.md §8.1 item 9): a
 * database, buckets and keys for each pipeline, and an identity for each
 * stage allowed only its own. Each pipeline's split stage reaches its own
 * data, and is refused the other's, through the stages' own clients: the
 * lease pool, the S3 SDK and the Vault Transit key provider. A data key
 * wrapped under the other pipeline's KEK can't be unwrapped with its own.
 * The broker's side, ACLs per stage, is tested by connector-broker's
 * {@code KafkaSecurityIT}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PipelineIsolationIT
{

    /** The pipelines. */
    static final List<String> PIPELINES = List.of("alpha", "beta");

    /** The object store's administrator. */
    static final String S3_ADMIN = "sdh-admin";

    /** Vault's root token, for setting it up only. */
    static final String ROOT_TOKEN = "root-token";

    /** A token in Vault's answer. */
    static final Pattern CLIENT_TOKEN = Pattern.compile(
            "\"client_token\"\\s*:\\s*\"([^\"]+)\"");

    /** Each identity's secret: database password or S3 secret key. */
    static final Map<String, String> SECRETS = new LinkedHashMap<>();

    /** Each split stage's Vault token. */
    static final Map<String, String> TOKENS = new LinkedHashMap<>();

    /** The shared database server. */
    static PostgreSQLContainer postgres;

    /** The shared object store. */
    static GenericContainer<?> seaweed;

    /** The shared key service. */
    static VaultContainer<?> vault;

    /** Vault's address. */
    static URI vaultAddress;

    /** Creates the tests; JUnit runs each test on a new instance. */
    PipelineIsolationIT()
    {
    }

    /**
     * Starts the services and lays out each pipeline's database, buckets,
     * keys and identities.
     *
     * @throws Exception if a service can't start or be set up
     */
    @BeforeAll
    static void start() throws Exception
    {
        requireDocker();
        SECRETS.put(S3_ADMIN, secret());
        for (String pipeline : PIPELINES)
        {
            SECRETS.put(splitter(pipeline), secret());
        }
        postgres = new PostgreSQLContainer("postgres:18.4");
        seaweed = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withCopyToContainer(Transferable.of(s3Identities()),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        // Each bucket is a collection of volumes of its own.
                        "-ip.bind=0.0.0.0", "-volume.max=40")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        vault = new VaultContainer<>("hashicorp/vault:2.1.1")
                .withVaultToken(ROOT_TOKEN);
        postgres.start();
        seaweed.start();
        vault.start();
        vaultAddress = URI.create(vault.getHttpHostAddress());
        layOutDatabases();
        layOutBuckets();
        layOutKeys();
    }

    /** Stops the services. */
    @AfterAll
    static void stop()
    {
        for (GenericContainer<?> each : new GenericContainer<?>[] {vault,
            seaweed, postgres})
        {
            if (each != null)
            {
                each.stop();
            }
        }
    }

    /**
     * Each split stage uses its own pipeline's database, buckets and keys.
     *
     * @throws Exception if the test fails
     */
    @Test
    void eachStageReachesItsOwnPipelinesData() throws Exception
    {
        for (String pipeline : PIPELINES)
        {
            String stage = splitter(pipeline);
            try (HikariDataSource pool = pool(stage, database(pipeline));
                    Connection connection = pool.getConnection();
                    Statement statement = connection.createStatement())
            {
                statement.execute("CREATE TABLE split_" + pipeline
                        + ".sdh_lease (name text PRIMARY KEY)");
                statement.execute("INSERT INTO split_" + pipeline
                        + ".sdh_lease VALUES ('duties')");
            }
            try (S3Client s3 = s3(stage))
            {
                assertThat(s3.getObjectAsBytes(get -> get.bucket(
                        bucket(pipeline, "bronze")).key("item"))
                        .asString(UTF_8)).isEqualTo(pipeline);
                s3.putObject(put -> put.bucket(bucket(pipeline, "silver"))
                        .key("records"), RequestBody.fromString("split"));
            }
            VaultTransitKeyProvider keys = keys(stage);
            byte[] dataKey = new byte[32];
            new SecureRandom().nextBytes(dataKey);
            WrappedKey wrapped = keys.wrap(pipeline + "-records", dataKey);
            assertThat(keys.unwrap(wrapped)).isEqualTo(dataKey);
            assertThat(keys.sign(pipeline + "-splitter-signing",
                    new byte[32])).isNotEmpty();
        }
    }

    /**
     * Alpha's split stage is refused beta's database, buckets and keys,
     * and can't open a data key wrapped under beta's KEK with its own.
     *
     * @throws Exception if the test fails
     */
    @Test
    void aStageIsRefusedAnotherPipelinesData() throws Exception
    {
        String stage = splitter("alpha");
        assertThatThrownBy(() -> pool(stage, database("beta")).close())
                .as("beta's database")
                .hasStackTraceContaining("permission denied for database");

        try (S3Client s3 = s3(stage))
        {
            assertForbidden(() -> s3.getObjectAsBytes(get -> get.bucket(
                    bucket("beta", "bronze")).key("item")), "reading beta");
            assertForbidden(() -> s3.putObject(put -> put.bucket(
                    bucket("beta", "silver")).key("records"),
                    RequestBody.fromString("x")), "writing beta");
            assertForbidden(() -> s3.listObjectsV2(list -> list.bucket(
                    bucket("beta", "bronze"))), "listing beta");
            assertForbidden(() -> s3.putObject(put -> put.bucket(
                    bucket("alpha", "bronze")).key("item"),
                    RequestBody.fromString("x")),
                    "writing its own input, which it only reads");
        }

        byte[] dataKey = new byte[32];
        new SecureRandom().nextBytes(dataKey);
        WrappedKey betas = keys(splitter("beta")).wrap("beta-records",
                dataKey);
        VaultTransitKeyProvider alpha = keys(stage);
        assertThatThrownBy(() -> alpha.unwrap(betas))
                .as("unwrapping under beta's KEK")
                .hasMessageContaining("403");
        assertThatThrownBy(() -> alpha.wrap("beta-records", dataKey))
                .as("wrapping under beta's KEK")
                .hasMessageContaining("403");
        assertThatThrownBy(() -> alpha.sign("beta-splitter-signing",
                new byte[32])).as("signing as beta")
                .hasMessageContaining("403");
        assertThatThrownBy(() -> alpha.unwrap(new WrappedKey(
                "alpha-records", betas.wrapped())))
                .as("beta's wrapped key, under its own KEK")
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Makes a database for each pipeline, which only its stages may
     * connect to, and a registry schema in it for its split stage.
     *
     * @throws Exception if the database refuses
     */
    static void layOutDatabases() throws Exception
    {
        try (Connection admin = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
                Statement statement = admin.createStatement())
        {
            for (String pipeline : PIPELINES)
            {
                String role = splitter(pipeline).replace('-', '_');
                statement.execute("CREATE ROLE " + role
                        + " LOGIN PASSWORD '"
                        + SECRETS.get(splitter(pipeline)) + "'");
                statement.execute("CREATE DATABASE " + database(pipeline));
                statement.execute("REVOKE ALL ON DATABASE "
                        + database(pipeline) + " FROM PUBLIC");
                statement.execute("GRANT CONNECT ON DATABASE "
                        + database(pipeline) + " TO " + role);
            }
        }
        for (String pipeline : PIPELINES)
        {
            try (Connection admin = DriverManager.getConnection(
                    url(database(pipeline)), postgres.getUsername(),
                    postgres.getPassword());
                    Statement statement = admin.createStatement())
            {
                statement.execute("CREATE SCHEMA split_" + pipeline
                        + " AUTHORIZATION "
                        + splitter(pipeline).replace('-', '_'));
            }
        }
    }

    /**
     * Makes each pipeline's buckets, with an item landed in its bronze.
     */
    static void layOutBuckets()
    {
        try (S3Client s3 = s3(S3_ADMIN))
        {
            for (String pipeline : PIPELINES)
            {
                for (String layer : List.of("bronze", "silver"))
                {
                    s3.createBucket(create -> create.bucket(
                            bucket(pipeline, layer)));
                }
                s3.putObject(put -> put.bucket(bucket(pipeline, "bronze"))
                        .key("item"), RequestBody.fromString(pipeline));
            }
        }
    }

    /**
     * Makes each pipeline's KEK and its split stage's signing key, and a
     * token for each split stage with the sandbox's policy: its own keys
     * only (scripts/sandbox.sh).
     *
     * @throws Exception if Vault refuses
     */
    static void layOutKeys() throws Exception
    {
        vaultPost("sys/mounts/transit", "{\"type\":\"transit\"}");
        for (String pipeline : PIPELINES)
        {
            String kek = pipeline + "-records";
            String signing = pipeline + "-splitter-signing";
            vaultPost("transit/keys/" + kek, "{\"type\":\"aes256-gcm96\"}");
            vaultPost("transit/keys/" + signing,
                    "{\"type\":\"ecdsa-p256\"}");
            String policy = String.join("\n",
                    "path \"transit/encrypt/" + kek + "\""
                            + " { capabilities = [\"update\"] }",
                    "path \"transit/decrypt/" + kek + "\""
                            + " { capabilities = [\"update\"] }",
                    "path \"transit/sign/" + signing + "/*\""
                            + " { capabilities = [\"update\"] }",
                    "path \"transit/keys/" + signing + "\""
                            + " { capabilities = [\"read\"] }");
            vaultPost("sys/policies/acl/" + splitter(pipeline),
                    "{\"policy\":" + quote(policy) + "}");
            Matcher token = CLIENT_TOKEN.matcher(vaultPost(
                    "auth/token/create", "{\"policies\":[\""
                            + splitter(pipeline) + "\"]}"));
            assertThat(token.find()).isTrue();
            TOKENS.put(splitter(pipeline), token.group(1));
        }
    }

    /**
     * The object store's identities: an administrator, and each split
     * stage allowed to read its pipeline's bronze and write its silver.
     *
     * @return SeaweedFS's S3 configuration
     */
    static String s3Identities()
    {
        List<String> identities = new ArrayList<>();
        identities.add(identity(S3_ADMIN, "\"Admin\",\"Read\",\"Write\","
                + "\"List\",\"Tagging\""));
        for (String pipeline : PIPELINES)
        {
            String bronze = bucket(pipeline, "bronze");
            String silver = bucket(pipeline, "silver");
            identities.add(identity(splitter(pipeline), "\"Read:" + bronze
                    + "\",\"List:" + bronze + "\",\"Read:" + silver
                    + "\",\"Write:" + silver + "\",\"List:" + silver
                    + "\""));
        }
        return "{\"identities\":[" + String.join(",", identities) + "]}";
    }

    /**
     * One S3 identity, whose access key is its name.
     *
     * @param name    the name
     * @param actions its actions, as JSON strings
     * @return the identity, as JSON
     */
    static String identity(String name, String actions)
    {
        return "{\"name\":\"" + name
                + "\",\"credentials\":[{\"accessKey\":\"" + name
                + "\",\"secretKey\":\"" + SECRETS.get(name)
                + "\"}],\"actions\":[" + actions + "]}";
    }

    /**
     * A pipeline's split stage, as the sandbox names its identities.
     *
     * @param pipeline the pipeline
     * @return the identity
     */
    static String splitter(String pipeline)
    {
        return "splitter-" + pipeline;
    }

    /**
     * A pipeline's database.
     *
     * @param pipeline the pipeline
     * @return the database's name
     */
    static String database(String pipeline)
    {
        return "sdh_" + pipeline;
    }

    /**
     * A pipeline's bucket for a layer.
     *
     * @param pipeline the pipeline
     * @param layer    the layer, such as {@code bronze}
     * @return the bucket's name
     */
    static String bucket(String pipeline, String layer)
    {
        return "sdh-" + pipeline + "-" + layer;
    }

    /**
     * The lease pool, as a stage opens it, logged in as a stage.
     *
     * @param stage    the stage's identity
     * @param database the database
     * @return the pool
     */
    static HikariDataSource pool(String stage, String database)
    {
        return LeaseDatabase.dataSource(new CommonSettings.Jdbc(
                url(database), stage.replace('-', '_'), SECRETS.get(stage),
                "sdh_lease", 2));
    }

    /**
     * A database's JDBC URL.
     *
     * @param database the database
     * @return the URL
     */
    static String url(String database)
    {
        return "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                + "/" + database;
    }

    /**
     * An S3 client logged in as an identity.
     *
     * @param identity the identity
     * @return the client
     */
    static S3Client s3(String identity)
    {
        return S3Client.builder()
                .endpointOverride(URI.create("http://" + seaweed.getHost()
                        + ":" + seaweed.getMappedPort(8333)))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(identity,
                                SECRETS.get(identity))))
                .build();
    }

    /**
     * The Vault Transit key provider, as a stage makes it, with a stage's
     * token.
     *
     * @param stage the stage's identity
     * @return the provider
     */
    static VaultTransitKeyProvider keys(String stage)
    {
        String token = TOKENS.get(stage);
        return VaultTransitKeyProvider.create(vaultAddress, "transit",
                () -> token, Optional.empty());
    }

    /**
     * Checks that an S3 request is refused as forbidden.
     *
     * @param request the request
     * @param what    what it tries, for the failure
     */
    static void assertForbidden(Runnable request, String what)
    {
        assertThatThrownBy(request::run).as(what)
                .isInstanceOfSatisfying(S3Exception.class,
                        refused -> assertThat(refused.statusCode())
                                .isEqualTo(403));
    }

    /**
     * Posts to Vault's API as root and checks for success.
     *
     * @param path the API path, after {@code /v1/}
     * @param json the body
     * @return the answer's body
     * @throws Exception if the request fails
     */
    static String vaultPost(String path, String json) throws Exception
    {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(vaultAddress.resolve("/v1/" + path))
                        .header("X-Vault-Token", ROOT_TOKEN)
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode() / 100)
                .as(path + ": " + response.body()).isEqualTo(2);
        return response.body();
    }

    /**
     * A JSON string.
     *
     * @param text the text, with no backslashes
     * @return it quoted, with its quotes and line ends escaped
     */
    static String quote(String text)
    {
        return "\"" + text.replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }

    /**
     * A random secret.
     *
     * @return the secret
     */
    static String secret()
    {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
