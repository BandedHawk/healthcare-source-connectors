/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
 * What the demo's two apps run against (design §15.3): Postgres for their
 * leases and the JDBC catalog, somewhere for the Iceberg tables, and a
 * broker between them and on to the downstream consumer. Both apps run as
 * processes of their own, so the broker is a real one in both kinds.
 *
 * <ul>
 *   <li>{@code local} (default): no Docker. Embedded Postgres, tables on
 *       local disk under {@code target/demo/warehouse/}, and an embedded
 *       Apache Artemis broker in the demo's JVM.
 *   <li>{@code vm}: as deployed. Postgres, SeaweedFS (through Iceberg's
 *       {@code S3FileIO}) and Kafka as containers in the {@code sdh-it}
 *       minikube VM, which is started if needed and stopped afterwards
 *       ({@code -Ddemo.stop-vm=false} keeps it running).
 * </ul>
 * Each pipeline has two queues: {@code <pipeline>-landed}, from its landing
 * node to its splitter, and {@code <pipeline>-records}, from the splitter
 * to the downstream consumer.
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
     * Whether the tables are on local disk, so that the apps need the
     * demo's {@link DemoLocalFileIO} on their class path.
     *
     * @return true for {@code local}
     */
    boolean localDisk();

    /**
     * The Iceberg catalog's properties, the same for both apps and the
     * downstream consumer.
     *
     * @return the properties
     */
    Map<String, String> catalog();

    /**
     * The catalog and lease settings both apps get.
     *
     * @return the settings, as flat Spring property names
     */
    default Map<String, String> storage()
    {
        Map<String, String> settings = new LinkedHashMap<>();
        catalog().forEach((key, value) -> settings.put(
                "sdh.storage.catalog[" + key + "]", value));
        settings.putAll(lease());
        return settings;
    }

    /**
     * The lease database's settings.
     *
     * @return the settings, as flat Spring property names
     */
    Map<String, String> lease();

    /**
     * Where a pipeline's landing node announces items.
     *
     * @param pipeline the pipeline
     * @return its {@code sdh.queue} settings
     */
    Map<String, String> landingQueue(String pipeline);

    /**
     * Where a pipeline's splitter takes items from, and publishes to.
     *
     * @param pipeline the pipeline
     * @return its {@code sdh.landing.queue-uri} and {@code sdh.queue}
     *         settings
     */
    Map<String, String> splitterQueues(String pipeline);

    /**
     * A separate catalog client, as a downstream process would have.
     *
     * @return the catalog
     */
    default Catalog newCatalogClient()
    {
        return CatalogUtil.buildIcebergCatalog("sdh", catalog(), null);
    }

    /**
     * Every message a pipeline's splitter published, as a downstream
     * consumer receives them.
     *
     * @param pipeline the pipeline
     * @param atLeast  how many to wait for
     * @param timeout  how long to wait at most
     * @return the messages
     * @throws Exception if the messages can't be received
     */
    List<String> receive(String pipeline, int atLeast, Duration timeout)
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
     * No Docker: embedded Postgres, tables on local disk, and an embedded
     * Artemis broker.
     */
    final class Local implements DemoStack
    {

        /** The lease database and catalog. */
        private final EmbeddedPostgres postgres;

        /** Where the tables' files are written. */
        private final Path warehouse;

        /** The broker. */
        private final EmbeddedActiveMQ broker = new EmbeddedActiveMQ();

        /** The broker's address. */
        private final String brokerUrl;

        /**
         * Starts embedded Postgres and the broker.
         *
         * @param work the demo's working directory
         * @throws Exception if one can't start
         */
        Local(Path work) throws Exception
        {
            this.postgres = EmbeddedPostgres.start();
            this.warehouse = work.resolve("warehouse").toAbsolutePath();
            brokerUrl = "tcp://127.0.0.1:" + freePort();
            broker.setConfiguration(new ConfigurationImpl()
                    .setPersistenceEnabled(false)
                    .setSecurityEnabled(false)
                    .addAcceptorConfiguration("demo", brokerUrl));
            broker.start();
        }

        /**
         * A free local port, for the broker.
         *
         * @return the port
         * @throws IOException if none can be found
         */
        static int freePort() throws IOException
        {
            try (ServerSocket socket = new ServerSocket(0))
            {
                return socket.getLocalPort();
            }
        }

        /**
         * Describes the stack.
         *
         * @return the description
         */
        @Override
        public String describe()
        {
            return "no Docker: embedded Postgres, Iceberg tables on local"
                    + " disk (" + warehouse + "), an embedded Artemis broker"
                    + " (" + brokerUrl + ")";
        }

        /**
         * Whether the tables are on local disk.
         *
         * @return true
         */
        @Override
        public boolean localDisk()
        {
            return true;
        }

        /**
         * The catalog: JDBC on embedded Postgres, data on local disk.
         *
         * @return the properties
         */
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

        /**
         * The lease database: embedded Postgres.
         *
         * @return the settings
         */
        @Override
        public Map<String, String> lease()
        {
            return Map.of("sdh.lease.jdbc.url",
                    postgres.getJdbcUrl("postgres", "postgres"),
                    "sdh.lease.jdbc.username", "postgres",
                    "sdh.lease.jdbc.password", "postgres");
        }

        /**
         * The landing node's queue on the broker.
         *
         * @param pipeline the pipeline
         * @return the settings
         */
        @Override
        public Map<String, String> landingQueue(String pipeline)
        {
            return Map.of("sdh.queue.type", "artemis",
                    "sdh.queue.uri", "jms:queue:" + pipeline + "-landed",
                    "sdh.queue.artemis.broker-url", brokerUrl);
        }

        /**
         * The splitter's queues on the broker. The JMS component the
         * splitter's publisher is given serves its inbox route too.
         *
         * @param pipeline the pipeline
         * @return the settings
         */
        @Override
        public Map<String, String> splitterQueues(String pipeline)
        {
            return Map.of("sdh.landing.queue-uri",
                    "jms:queue:" + pipeline + "-landed",
                    "sdh.queue.type", "artemis",
                    "sdh.queue.uri", "jms:queue:" + pipeline + "-records",
                    "sdh.queue.artemis.broker-url", brokerUrl);
        }

        /**
         * Reads the records queue until enough messages arrive or the
         * time runs out.
         *
         * @param pipeline the pipeline
         * @param atLeast  how many to wait for
         * @param timeout  how long to wait at most
         * @return the messages
         * @throws Exception if a message can't be read
         */
        @Override
        public List<String> receive(String pipeline, int atLeast,
                Duration timeout) throws Exception
        {
            List<String> out = new ArrayList<>();
            try (ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory(brokerUrl);
                    JMSContext context = factory.createContext();
                    JMSConsumer consumer = context.createConsumer(
                            context.createQueue(pipeline + "-records")))
            {
                long until = System.currentTimeMillis() + timeout.toMillis();
                while (System.currentTimeMillis() < until)
                {
                    // Once enough have come, take what else is waiting.
                    Message message = consumer.receive(
                            out.size() < atLeast ? 500 : 200);
                    if (message == null && out.size() >= atLeast)
                    {
                        break;
                    }
                    if (message != null)
                    {
                        out.add(message.getBody(String.class));
                    }
                }
            }
            return out;
        }

        /**
         * Stops the broker and Postgres.
         *
         * @throws Exception if one can't be stopped
         */
        @Override
        public void close() throws Exception
        {
            broker.stop();
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

        /**
         * Describes the stack.
         *
         * @return the description
         */
        @Override
        public String describe()
        {
            return "full stack in the sdh-it VM: Postgres 18, SeaweedFS"
                    + " through S3FileIO (" + s3Endpoint() + "), Kafka ("
                    + kafka.getBootstrapServers() + ")";
        }

        /**
         * Whether the tables are on local disk.
         *
         * @return false: they are in SeaweedFS
         */
        @Override
        public boolean localDisk()
        {
            return false;
        }

        /**
         * The catalog: JDBC on Postgres, data in SeaweedFS.
         *
         * @return the properties
         */
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

        /**
         * The lease database: Postgres in the VM.
         *
         * @return the settings
         */
        @Override
        public Map<String, String> lease()
        {
            return Map.of("sdh.lease.jdbc.url", postgres.getJdbcUrl(),
                    "sdh.lease.jdbc.username", postgres.getUsername(),
                    "sdh.lease.jdbc.password", postgres.getPassword());
        }

        /**
         * The landing node's topic.
         *
         * @param pipeline the pipeline
         * @return the settings
         */
        @Override
        public Map<String, String> landingQueue(String pipeline)
        {
            return Map.of("sdh.queue.type", "kafka",
                    "sdh.queue.uri", "kafka:" + pipeline + "-landed?brokers="
                            + kafka.getBootstrapServers());
        }

        /**
         * The splitter's topics. Offsets are committed automatically, one
         * message per poll, so only for messages already handled;
         * {@code breakOnFirstError} keeps a failed message from being
         * skipped (design §5.3.12).
         *
         * @param pipeline the pipeline
         * @return the settings
         */
        @Override
        public Map<String, String> splitterQueues(String pipeline)
        {
            return Map.of("sdh.landing.queue-uri", "kafka:" + pipeline
                    + "-landed?brokers=" + kafka.getBootstrapServers()
                    + "&groupId=" + pipeline + "-splitter"
                    + "&autoOffsetReset=earliest&maxPollRecords=1"
                    + "&breakOnFirstError=true",
                    "sdh.queue.type", "kafka",
                    "sdh.queue.uri", "kafka:" + pipeline
                            + "-records?brokers="
                            + kafka.getBootstrapServers());
        }

        /**
         * Reads the records topic from the start until enough messages
         * arrive or the time runs out.
         *
         * @param pipeline the pipeline
         * @param atLeast  how many to wait for
         * @param timeout  how long to wait at most
         * @return the messages
         */
        @Override
        public List<String> receive(String pipeline, int atLeast,
                Duration timeout)
        {
            List<String> out = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                    Map.of("bootstrap.servers", kafka.getBootstrapServers(),
                            "group.id", pipeline + "-downstream",
                            "auto.offset.reset", "earliest"),
                    new StringDeserializer(), new StringDeserializer()))
            {
                consumer.subscribe(List.of(pipeline + "-records"));
                long until = System.currentTimeMillis() + timeout.toMillis();
                while (System.currentTimeMillis() < until)
                {
                    // Once enough have come, take what else is waiting.
                    ConsumerRecords<String, String> polled = consumer.poll(
                            Duration.ofMillis(out.size() < atLeast
                                    ? 500 : 1000));
                    if (polled.isEmpty() && out.size() >= atLeast)
                    {
                        break;
                    }
                    polled.forEach(record -> out.add(record.value()));
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
