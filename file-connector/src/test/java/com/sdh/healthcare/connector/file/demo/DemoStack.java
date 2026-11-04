/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * What the demo's connector runs against: Postgres for the lease and the
 * JDBC catalog, somewhere for the Iceberg tables, and a broker.
 *
 * <ul>
 *   <li>{@code local} (default): one JVM, no Docker. Embedded Postgres,
 *       tables on local disk under {@code target/demo/warehouse/}, and a
 *       Camel SEDA queue standing in for the broker.
 *   <li>{@code vm}: as deployed. Postgres, SeaweedFS (through Iceberg's
 *       {@code S3FileIO}) and Kafka as containers in the {@code sdh-it}
 *       minikube VM, which is started if needed and stopped afterwards
 *       ({@code -Ddemo.stop-vm=false} keeps it running).
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
interface DemoStack extends AutoCloseable
{

    /**
     * Starts a stack.
     *
     * @param kind {@code local} or {@code vm}
     * @param work the demo's working directory
     * @return the running stack
     * @throws Exception if the stack can't start
     */
    static DemoStack create(String kind, Path work) throws Exception
    {
        return switch (kind)
        {
            case "local" -> new Local(work);
            case "vm" -> new Vm();
            default -> throw new IllegalArgumentException(
                    "demo.stack must be local or vm, not " + kind);
        };
    }

    /**
     * Describes the stack for the demo's output.
     *
     * @return the description
     */
    String describe();

    /**
     * The Iceberg catalog's properties, shared by the splitter and the
     * landing stand-in.
     *
     * @return the properties
     */
    Map<String, String> catalog();

    /**
     * The catalog, lease and queue settings for the connector.
     *
     * @return the settings
     */
    Map<String, String> properties();

    /**
     * A separate catalog client, as a downstream process would have.
     *
     * @return the catalog
     */
    Catalog newCatalogClient();

    /**
     * Every message the connector published, as a downstream consumer
     * receives them.
     *
     * @param camel the connector's Camel context
     * @param atLeast how many to wait for, where the broker needs waiting
     * @param timeout how long to wait at most
     * @return the messages
     * @throws Exception if the messages can't be received
     */
    List<String> receive(CamelContext camel, int atLeast, Duration timeout)
            throws Exception;

    /**
     * Stops the stack.
     *
     * @throws Exception if it can't be stopped
     */
    @Override
    default void close() throws Exception
    {
    }

    /**
     * One JVM: embedded Postgres, tables on local disk, a SEDA queue for
     * the broker.
     */
    final class Local implements DemoStack
    {

        /** The queue standing in for the broker. */
        static final String QUEUE = "seda:sdh-demo-out?size=100000";

        /** The lease database and catalog. */
        private final EmbeddedPostgres postgres;

        /** Where the tables' files are written. */
        private final Path warehouse;

        /**
         * Starts embedded Postgres.
         *
         * @param work the demo's working directory
         * @throws Exception if Postgres can't start
         */
        Local(Path work) throws Exception
        {
            this.postgres = EmbeddedPostgres.start();
            this.warehouse = work.resolve("warehouse").toAbsolutePath();
        }

        @Override
        public String describe()
        {
            return "in-process: embedded Postgres, Iceberg tables on local"
                    + " disk (" + warehouse
                    + "), a SEDA queue standing in for the broker";
        }

        @Override
        public Map<String, String> catalog()
        {
            Map<String, String> catalog = new LinkedHashMap<>();
            catalog.put("type", "jdbc");
            catalog.put("uri", postgres.getJdbcUrl("postgres", "postgres"));
            catalog.put("jdbc.user", "postgres");
            catalog.put("jdbc.password", "postgres");
            catalog.put("jdbc.schema-version", "V1");
            catalog.put("warehouse", warehouse.toString());
            catalog.put("io-impl", DemoLocalFileIO.class.getName());
            return catalog;
        }

        @Override
        public Map<String, String> properties()
        {
            Map<String, String> properties = new LinkedHashMap<>();
            catalog().forEach((key, value) -> properties.put(
                    "sdh.storage.catalog[" + key + "]", value));
            properties.put("sdh.lease.jdbc.url",
                    postgres.getJdbcUrl("postgres", "postgres"));
            properties.put("sdh.lease.jdbc.username", "postgres");
            properties.put("sdh.lease.jdbc.password", "postgres");
            properties.put("sdh.queue.type", "camel");
            properties.put("sdh.queue.uri", QUEUE);
            return properties;
        }

        @Override
        public Catalog newCatalogClient()
        {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog(), null);
        }

        /**
         * Drains the queue; the connector has published everything by now.
         *
         * @param camel the connector's Camel context
         * @param atLeast unused: the queue is in process
         * @param timeout unused
         * @return the messages
         */
        @Override
        public List<String> receive(CamelContext camel, int atLeast,
                Duration timeout)
        {
            List<String> out = new ArrayList<>();
            ConsumerTemplate consumer = camel.createConsumerTemplate();
            String body;
            while ((body = consumer.receiveBody("seda:sdh-demo-out", 1000,
                    String.class)) != null)
            {
                out.add(body);
            }
            return out;
        }

        @Override
        public void close() throws Exception
        {
            postgres.close();
        }
    }

    /** As deployed: Postgres, SeaweedFS and Kafka in the minikube VM. */
    final class Vm implements DemoStack
    {

        /** The bucket holding the warehouse. */
        static final String BUCKET = "sdh-warehouse";

        /** The S3 access key. */
        static final String ACCESS_KEY = "sdh-demo-access";

        /** The S3 secret key. */
        static final String SECRET_KEY = "sdh-demo-secret-key-0123456789";

        /** The Kafka topic the connector publishes to. */
        static final String TOPIC = "sdh-sections";

        /** The lease database and catalog. */
        private final PostgreSQLContainer postgres =
                new PostgreSQLContainer("postgres:18.4");

        /** The object store. */
        private final GenericContainer<?> seaweed =
                new GenericContainer<>("chrislusf/seaweedfs:4.48")
                        .withCopyToContainer(Transferable.of("""
                                {"identities":[{"name":"sdh","credentials":\
                                [{"accessKey":"%s","secretKey":"%s"}],
                                  "actions":["Admin","Read","Write",\
                                "List","Tagging"]}]}"""
                                .formatted(ACCESS_KEY, SECRET_KEY)),
                                "/etc/seaweedfs/s3.json")
                        .withCommand("server", "-s3",
                                "-s3.config=/etc/seaweedfs/s3.json",
                                "-dir=/data", "-ip.bind=0.0.0.0",
                                "-volume.max=10")
                        .withExposedPorts(8333)
                        .waitingFor(Wait.forListeningPort()
                                .withStartupTimeout(Duration.ofMinutes(2)));

        /** The broker. */
        private final KafkaContainer kafka =
                new KafkaContainer("apache/kafka:4.2.2");

        /**
         * Starts the containers in the VM and makes the bucket.
         *
         * @throws Exception if the VM or a container can't start
         */
        Vm() throws Exception
        {
            MinikubeDocker.connect();
            System.out.println(
                    ">> Starting Postgres, SeaweedFS and Kafka in the VM");
            postgres.start();
            seaweed.start();
            kafka.start();
            try (S3Client s3 = S3Client.builder()
                    .endpointOverride(URI.create(s3Endpoint()))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY,
                                    SECRET_KEY)))
                    .build())
            {
                s3.createBucket(request -> request.bucket(BUCKET));
            }
        }

        /**
         * The object store's S3 endpoint.
         *
         * @return the endpoint URL
         */
        String s3Endpoint()
        {
            return "http://" + seaweed.getHost() + ":"
                    + seaweed.getMappedPort(8333);
        }

        @Override
        public String describe()
        {
            return "full stack in the sdh-it VM: Postgres 18, SeaweedFS"
                    + " through S3FileIO (" + s3Endpoint() + "), Kafka ("
                    + kafka.getBootstrapServers() + ")";
        }

        @Override
        public Map<String, String> catalog()
        {
            Map<String, String> catalog = new LinkedHashMap<>();
            catalog.put("type", "jdbc");
            catalog.put("uri", postgres.getJdbcUrl());
            catalog.put("jdbc.user", postgres.getUsername());
            catalog.put("jdbc.password", postgres.getPassword());
            catalog.put("jdbc.schema-version", "V1");
            catalog.put("warehouse", "s3://" + BUCKET + "/wh");
            catalog.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            catalog.put("s3.endpoint", s3Endpoint());
            catalog.put("s3.path-style-access", "true");
            catalog.put("s3.access-key-id", ACCESS_KEY);
            catalog.put("s3.secret-access-key", SECRET_KEY);
            catalog.put("client.region", "us-east-1");
            return catalog;
        }

        @Override
        public Map<String, String> properties()
        {
            Map<String, String> properties = new LinkedHashMap<>();
            catalog().forEach((key, value) -> properties.put(
                    "sdh.storage.catalog[" + key + "]", value));
            properties.put("sdh.lease.jdbc.url", postgres.getJdbcUrl());
            properties.put("sdh.lease.jdbc.username", postgres.getUsername());
            properties.put("sdh.lease.jdbc.password", postgres.getPassword());
            properties.put("sdh.queue.type", "kafka");
            properties.put("sdh.queue.uri", "kafka:" + TOPIC + "?brokers="
                    + kafka.getBootstrapServers());
            return properties;
        }

        @Override
        public Catalog newCatalogClient()
        {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog(), null);
        }

        /**
         * Reads the topic from the start until enough messages arrive or
         * the time runs out.
         *
         * @param camel unused: Kafka is read directly
         * @param atLeast how many to wait for
         * @param timeout how long to wait at most
         * @return the messages
         */
        @Override
        public List<String> receive(CamelContext camel, int atLeast,
                Duration timeout)
        {
            List<String> out = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                    Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                            "group.id", "sdh-demo-downstream",
                            "auto.offset.reset", "earliest"),
                    new StringDeserializer(), new StringDeserializer()))
            {
                consumer.subscribe(List.of(TOPIC));
                long until = System.currentTimeMillis() + timeout.toMillis();
                while (out.size() < atLeast
                        && System.currentTimeMillis() < until)
                {
                    consumer.poll(Duration.ofMillis(500))
                            .forEach(record -> out.add(record.value()));
                }
            }
            return out;
        }

        /** Stops the containers, and the VM unless asked not to. */
        @Override
        public void close()
        {
            kafka.stop();
            seaweed.stop();
            postgres.stop();
            MinikubeDocker.stopIfAsked();
        }
    }

    /**
     * Points Testcontainers at the Docker daemon inside the {@code sdh-it}
     * minikube VM, as scripts/minikube-env.sh does for the shell scripts,
     * so the demo also runs from NetBeans without a terminal: it asks
     * minikube for the daemon's address and certificates and hands them to
     * {@link MinikubeDockerStrategy}.
     */
    final class MinikubeDocker
    {

        /** The minikube profile. */
        static final String PROFILE = "sdh-it";

        /** Not instantiable. */
        private MinikubeDocker()
        {
        }

        /**
         * Starts or reuses the VM and sets the Docker host properties,
         * unless DOCKER_HOST is already set.
         *
         * @throws Exception if the daemon can't be reached
         */
        static void connect() throws Exception
        {
            if (System.getenv("DOCKER_HOST") != null
                    || System.getProperty("DOCKER_HOST") != null)
            {
                return;
            }
            Path script = Path.of("..", "scripts", "minikube-env.sh")
                    .toAbsolutePath().normalize();
            System.out.println(">> Starting or reusing the minikube profile '"
                    + PROFILE + "' (" + script + ")");
            Process process = new ProcessBuilder("bash", "-c",
                    "source \"$1\" >&2; echo \"host=$DOCKER_HOST\";"
                            + " echo \"certs=$DOCKER_CERT_PATH\"",
                    "bash", script.toString())
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(),
                            StandardCharsets.UTF_8)))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    if (line.startsWith("host=") && line.length() > 5)
                    {
                        System.setProperty(MinikubeDockerStrategy.HOST,
                                line.substring(5));
                    }
                    else if (line.startsWith("certs=") && line.length() > 6)
                    {
                        System.setProperty(MinikubeDockerStrategy.CERTS,
                                line.substring(6));
                    }
                }
            }
            if (process.waitFor() != 0
                    || System.getProperty(MinikubeDockerStrategy.HOST) == null)
            {
                throw new IllegalStateException("Couldn't reach the Docker"
                        + " daemon in the minikube VM " + PROFILE);
            }
        }

        /**
         * Keeps the VM stopped when it isn't in use, unless
         * -Ddemo.stop-vm=false.
         */
        static void stopIfAsked()
        {
            if (!Boolean.parseBoolean(
                    System.getProperty("demo.stop-vm", "true")))
            {
                return;
            }
            try
            {
                System.out.println(">> Stopping the minikube profile '"
                        + PROFILE + "'");
                new ProcessBuilder("minikube", "stop", "-p", PROFILE)
                        .inheritIO().start().waitFor();
            }
            catch (Exception failure)
            {
                System.out.println("   couldn't stop it: "
                        + failure.getMessage());
            }
        }
    }
}
