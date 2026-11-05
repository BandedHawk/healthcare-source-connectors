/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.landing.runtime.LandingRuntime;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The whole landing node on an S3 bucket intake: SeaweedFS in a container,
 * embedded Postgres, a JDBC catalog, key files and a SEDA queue. The store
 * is paused and resumed part-way.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
@ExtendWith(OutputCaptureExtension.class)
class BucketLandingApplicationIT
{

    /** The S3 access key. */
    static final String ACCESS_KEY = "sdh-it";

    /** Its secret. */
    static final String SECRET_KEY = "sdh-it-secret";

    /** The bucket. */
    static final String BUCKET = "drop";

    /** A word in every key, which must never be logged. */
    static final String SECRET_NAME = "patient-doe";

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres POSTGRES;

    /** SeaweedFS. */
    static final GenericContainer<?> SEAWEED;

    /** Writes to the bucket. */
    static final S3Client S3;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-landing-bucket");
            KEYS = new LandingTestKeys(
                    Files.createDirectories(root.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
            SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                    .withCopyToContainer(Transferable.of("""
                            {"identities":[{"name":"sdh","credentials":[\
                            {"accessKey":"%s","secretKey":"%s"}],\
                            "actions":["Admin","Read","Write","List"]}]}"""
                            .formatted(ACCESS_KEY, SECRET_KEY)),
                            "/etc/seaweedfs/s3.json")
                    .withCommand("server", "-s3",
                            "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                            "-ip.bind=0.0.0.0", "-volume.max=10")
                    .withExposedPorts(8333)
                    .waitingFor(Wait.forListeningPort()
                            .withStartupTimeout(Duration.ofMinutes(2)));
            SEAWEED.start();
            S3 = S3Client.builder().endpointOverride(URI.create(endpoint()))
                    .region(Region.US_EAST_1).forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY,
                                    SECRET_KEY)))
                    .build();
            S3.createBucket(request -> request.bucket(BUCKET));
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /**
     * SeaweedFS's S3 address.
     *
     * @return the address
     */
    static String endpoint()
    {
        return "http://" + SEAWEED.getHost() + ":"
                + SEAWEED.getMappedPort(8333);
    }

    /** Stops the store after the tests. */
    @AfterAll
    static void stopStore()
    {
        S3.close();
        SEAWEED.stop();
    }

    /** Puts a recording exit hook in place of the real one. */
    @TestConfiguration
    static class Overrides
    {

        /**
         * The recording hook.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Points the node at the test's database, keys and bucket.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> "claims");
        registry.add("sdh.landing.source.type", () -> "s3");
        registry.add("sdh.landing.source.path", () -> "/inbound");
        registry.add("sdh.landing.source.access", () -> "read-write");
        registry.add("sdh.landing.source.settle", () -> "0s");
        registry.add("sdh.landing.source.poll-interval", () -> "200ms");
        registry.add("sdh.landing.source.bucket.name", () -> BUCKET);
        registry.add("sdh.landing.source.bucket.endpoint",
                BucketLandingApplicationIT::endpoint);
        registry.add("sdh.landing.source.bucket.path-style-access",
                () -> "true");
        registry.add("sdh.landing.source.bucket.access-key-id",
                () -> ACCESS_KEY);
        registry.add("sdh.landing.source.bucket.secret-access-key",
                () -> SECRET_KEY);
        registry.add("sdh.landing.source.bucket.connect-timeout", () -> "2s");
        registry.add("sdh.landing.source.bucket.read-timeout", () -> "2s");
        registry.add("sdh.storage.catalog.type", () -> "jdbc");
        registry.add("sdh.storage.catalog.uri", () -> jdbc);
        registry.add("sdh.storage.catalog.warehouse",
                () -> "memory://wh-" + UUID.randomUUID());
        registry.add("sdh.storage.catalog.io-impl",
                () -> "org.apache.iceberg.inmemory.InMemoryFileIO");
        registry.add("sdh.storage.catalog[jdbc.user]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.password]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.schema-version]", () -> "V1");
        registry.add("sdh.lease.jdbc.url", () -> jdbc);
        registry.add("sdh.lease.jdbc.username", () -> "postgres");
        registry.add("sdh.lease.jdbc.password", () -> "postgres");
        registry.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        registry.add("sdh.crypto.keystore.password",
                () -> LandingTestKeys.KEYSTORE_PASSWORD);
        registry.add("sdh.crypto.keys.landed-items-kek",
                () -> LandingTestKeys.LANDED_ITEMS);
        registry.add("sdh.crypto.keys.signing.kid", () -> "landing-2026");
        registry.add("sdh.crypto.keys.signing.pem",
                KEYS.signingPem::toString);
        registry.add("sdh.crypto.keys.signing.passphrase",
                () -> LandingTestKeys.SIGNING_PASSPHRASE);
        registry.add("sdh.crypto.keys.recipient.kid",
                () -> "downstream-2026");
        registry.add("sdh.crypto.keys.recipient.pem",
                KEYS.recipientPem::toString);
        String pin = KeyMaterial.fingerprint(KEYS.recipient.getPublic());
        registry.add("sdh.crypto.keys.recipient.sha256", () -> pin);
        registry.add("sdh.queue.type", () -> "camel");
        registry.add("sdh.queue.uri", () -> "seda:sdh-out?size=1000");
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /** The application's Camel context. */
    @Autowired
    CamelContext camel;

    /** The node's services. */
    @Autowired
    LandingRuntime runtime;

    /**
     * Objects in the bucket are landed as objects, announced and deleted,
     * and the node claims the bucket; while the store is paused the intake
     * is reported unreachable, and once it is back the node carries on. No
     * key reaches the log.
     *
     * @param output what the node logged
     * @throws Exception if the test fails
     */
    @Test
    void landsFromABucketAndRidesOutAnOutage(CapturedOutput output)
            throws Exception
    {
        drop("inbound/" + SECRET_NAME + "-1.csv", "1,a\n2,b\n");
        drop("inbound/" + SECRET_NAME + "-2.csv", "3,c\n");
        drop("elsewhere/" + SECRET_NAME + "-x.csv", "x\n");
        List<ConnectorMessage.ItemLanded> landed = receive(2);
        assertThat(landed).extracting(ConnectorMessage.ItemLanded::size)
                .containsExactlyInAnyOrder(8L, 4L);
        assertThat(landed).extracting(ConnectorMessage.ItemLanded::kind)
                .containsOnly("object");
        // What is left: the object outside the prefix, and the node's claim.
        awaitTrue(() -> keys().equals(List.of("elsewhere/" + SECRET_NAME
                + "-x.csv", "inbound/" + InboundDirectoryClaim.FILE)));

        SEAWEED.getDockerClient().pauseContainerCmd(SEAWEED.getContainerId())
                .exec();
        try
        {
            awaitTrue(() -> !runtime.service().reachable());
        }
        finally
        {
            SEAWEED.getDockerClient().unpauseContainerCmd(
                    SEAWEED.getContainerId()).exec();
        }
        drop("inbound/" + SECRET_NAME + "-3.csv", "4,d\n5,e\n6,f\n");
        assertThat(receive(1)).singleElement().extracting(
                ConnectorMessage.ItemLanded::size).isEqualTo(12L);
        assertThat(runtime.service().reachable()).isTrue();
        awaitTrue(() -> keys().size() == 2);
        assertThat(FATAL).isEmpty();
        assertThat(output.getAll()).doesNotContain(SECRET_NAME)
                .contains("intake.unreachable");
    }

    /**
     * Writes an object.
     *
     * @param key     its key
     * @param content its content
     */
    static void drop(String key, String content)
    {
        S3.putObject(request -> request.bucket(BUCKET).key(key),
                RequestBody.fromString(content, UTF_8));
    }

    /**
     * The keys in the bucket.
     *
     * @return the keys
     */
    static List<String> keys()
    {
        return S3.listObjectsV2(request -> request.bucket(BUCKET)).contents()
                .stream().map(object -> object.key()).toList();
    }

    /**
     * Waits for item.landed messages.
     *
     * @param count how many
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(int count) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long deadline = System.nanoTime() + 60_000_000_000L;
        while (landed.size() < count && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:sdh-out", 500,
                    String.class);
            if (body != null)
            {
                landed.add((ConnectorMessage.ItemLanded)
                        opener.open(body.getBytes(UTF_8)));
            }
        }
        return landed;
    }

    /**
     * Waits up to 30 seconds for a condition.
     *
     * @param condition the condition
     * @throws Exception if interrupted
     */
    static void awaitTrue(BooleanSupplier condition) throws Exception
    {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
