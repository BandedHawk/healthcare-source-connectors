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
 * What the demo's connector runs against: Postgres for the lease and the JDBC catalog, somewhere for the Iceberg
 * tables, and a broker.
 *
 * <ul>
 *   <li>{@code local} (default): one JVM, no Docker. Embedded Postgres, tables on local disk under
 *       {@code target/demo/warehouse/}, and a Camel SEDA queue standing in for the broker.
 *   <li>{@code vm}: as deployed. Postgres, SeaweedFS (through Iceberg's {@code S3FileIO}) and Kafka as containers in
 *       the {@code sdh-it} minikube VM, which is started if needed and stopped afterwards
 *       ({@code -Ddemo.stop-vm=false} keeps it running).
 * </ul>
 */
interface DemoStack extends AutoCloseable {

    static DemoStack create(String kind, Path work) throws Exception {
        return switch (kind) {
            case "local" -> new Local(work);
            case "vm" -> new Vm();
            default -> throw new IllegalArgumentException("demo.stack must be local or vm, not " + kind);
        };
    }

    String describe();

    /** The catalog, lease and queue settings for the connector. */
    Map<String, String> properties();

    /** A separate catalog client, as a downstream process would have. */
    Catalog newCatalogClient();

    /** Every message the connector published, as a downstream consumer receives them. */
    List<String> receive(CamelContext camel, int atLeast, Duration timeout) throws Exception;

    @Override
    default void close() throws Exception {}

    /** One JVM: embedded Postgres, tables on local disk, a SEDA queue for the broker. */
    final class Local implements DemoStack {
        static final String QUEUE = "seda:sdh-demo-out?size=100000";
        private final EmbeddedPostgres pg;
        private final Path warehouse;

        Local(Path work) throws Exception {
            this.pg = EmbeddedPostgres.start();
            this.warehouse = work.resolve("warehouse").toAbsolutePath();
        }

        @Override
        public String describe() {
            return "in-process: embedded Postgres, Iceberg tables on local disk (" + warehouse
                    + "), a SEDA queue standing in for the broker";
        }

        Map<String, String> catalog() {
            Map<String, String> c = new LinkedHashMap<>();
            c.put("type", "jdbc");
            c.put("uri", pg.getJdbcUrl("postgres", "postgres"));
            c.put("jdbc.user", "postgres");
            c.put("jdbc.password", "postgres");
            c.put("jdbc.schema-version", "V1");
            c.put("warehouse", warehouse.toString());
            c.put("io-impl", DemoLocalFileIO.class.getName());
            return c;
        }

        @Override
        public Map<String, String> properties() {
            Map<String, String> p = new LinkedHashMap<>();
            catalog().forEach((k, v) -> p.put("sdh.storage.catalog[" + k + "]", v));
            p.put("sdh.lease.jdbc.url", pg.getJdbcUrl("postgres", "postgres"));
            p.put("sdh.lease.jdbc.username", "postgres");
            p.put("sdh.lease.jdbc.password", "postgres");
            p.put("sdh.queue.type", "camel");
            p.put("sdh.queue.uri", QUEUE);
            return p;
        }

        @Override
        public Catalog newCatalogClient() {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog(), null);
        }

        @Override
        public List<String> receive(CamelContext camel, int atLeast, Duration timeout) {
            List<String> out = new ArrayList<>();
            ConsumerTemplate consumer = camel.createConsumerTemplate();
            String body;
            while ((body = consumer.receiveBody("seda:sdh-demo-out", 1000, String.class)) != null) {
                out.add(body);
            }
            return out;
        }

        @Override
        public void close() throws Exception {
            pg.close();
        }
    }

    /** As deployed: Postgres, SeaweedFS and Kafka in the minikube VM. */
    final class Vm implements DemoStack {
        static final String BUCKET = "sdh-warehouse";
        static final String ACCESS_KEY = "sdh-demo-access";
        static final String SECRET_KEY = "sdh-demo-secret-key-0123456789";
        static final String TOPIC = "sdh-sections";
        private final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.4");
        private final GenericContainer<?> seaweed = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withCopyToContainer(Transferable.of("""
                        {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
                          "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY)),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-ip.bind=0.0.0.0",
                        "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        private final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2");

        Vm() throws Exception {
            MinikubeDocker.connect();
            System.out.println(">> Starting Postgres, SeaweedFS and Kafka in the VM");
            postgres.start();
            seaweed.start();
            kafka.start();
            try (S3Client s3 = S3Client.builder().endpointOverride(URI.create(s3Endpoint())).region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .build()) {
                s3.createBucket(b -> b.bucket(BUCKET));
            }
        }

        String s3Endpoint() {
            return "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(8333);
        }

        @Override
        public String describe() {
            return "full stack in the sdh-it VM: Postgres 18, SeaweedFS through S3FileIO (" + s3Endpoint()
                    + "), Kafka (" + kafka.getBootstrapServers() + ")";
        }

        Map<String, String> catalog() {
            Map<String, String> c = new LinkedHashMap<>();
            c.put("type", "jdbc");
            c.put("uri", postgres.getJdbcUrl());
            c.put("jdbc.user", postgres.getUsername());
            c.put("jdbc.password", postgres.getPassword());
            c.put("jdbc.schema-version", "V1");
            c.put("warehouse", "s3://" + BUCKET + "/wh");
            c.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            c.put("s3.endpoint", s3Endpoint());
            c.put("s3.path-style-access", "true");
            c.put("s3.access-key-id", ACCESS_KEY);
            c.put("s3.secret-access-key", SECRET_KEY);
            c.put("client.region", "us-east-1");
            return c;
        }

        @Override
        public Map<String, String> properties() {
            Map<String, String> p = new LinkedHashMap<>();
            catalog().forEach((k, v) -> p.put("sdh.storage.catalog[" + k + "]", v));
            p.put("sdh.lease.jdbc.url", postgres.getJdbcUrl());
            p.put("sdh.lease.jdbc.username", postgres.getUsername());
            p.put("sdh.lease.jdbc.password", postgres.getPassword());
            p.put("sdh.queue.type", "kafka");
            p.put("sdh.queue.uri", "kafka:" + TOPIC + "?brokers=" + kafka.getBootstrapServers());
            return p;
        }

        @Override
        public Catalog newCatalogClient() {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog(), null);
        }

        @Override
        public List<String> receive(CamelContext camel, int atLeast, Duration timeout) {
            List<String> out = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    "bootstrap.servers", kafka.getBootstrapServers(), "group.id", "sdh-demo-downstream",
                    "auto.offset.reset", "earliest"), new StringDeserializer(), new StringDeserializer())) {
                consumer.subscribe(List.of(TOPIC));
                long until = System.currentTimeMillis() + timeout.toMillis();
                while (out.size() < atLeast && System.currentTimeMillis() < until) {
                    consumer.poll(Duration.ofMillis(500)).forEach(r -> out.add(r.value()));
                }
            }
            return out;
        }

        @Override
        public void close() {
            kafka.stop();
            seaweed.stop();
            postgres.stop();
            MinikubeDocker.stopIfAsked();
        }
    }

    /**
     * Points Testcontainers at the Docker daemon inside the {@code sdh-it} minikube VM, as scripts/minikube-env.sh
     * does for the shell scripts, so the demo also runs from NetBeans without a terminal: it asks minikube for the
     * daemon's address and certificates and hands them to {@link MinikubeDockerStrategy}.
     */
    final class MinikubeDocker {
        static final String PROFILE = "sdh-it";

        private MinikubeDocker() {}

        static void connect() throws Exception {
            if (System.getenv("DOCKER_HOST") != null || System.getProperty("DOCKER_HOST") != null) {
                return;
            }
            Path script = Path.of("..", "scripts", "minikube-env.sh").toAbsolutePath().normalize();
            System.out.println(">> Starting or reusing the minikube profile '" + PROFILE + "' (" + script + ")");
            Process p = new ProcessBuilder("bash", "-c", "source \"$1\" >&2; echo \"host=$DOCKER_HOST\";"
                    + " echo \"certs=$DOCKER_CERT_PATH\"", "bash", script.toString())
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(),
                    StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.startsWith("host=") && line.length() > 5) {
                        System.setProperty(MinikubeDockerStrategy.HOST, line.substring(5));
                    } else if (line.startsWith("certs=") && line.length() > 6) {
                        System.setProperty(MinikubeDockerStrategy.CERTS, line.substring(6));
                    }
                }
            }
            if (p.waitFor() != 0 || System.getProperty(MinikubeDockerStrategy.HOST) == null) {
                throw new IllegalStateException("Couldn't reach the Docker daemon in the minikube VM " + PROFILE);
            }
        }

        /** Keeps the VM stopped when it isn't in use, unless -Ddemo.stop-vm=false. */
        static void stopIfAsked() {
            if (!Boolean.parseBoolean(System.getProperty("demo.stop-vm", "true"))) {
                return;
            }
            try {
                System.out.println(">> Stopping the minikube profile '" + PROFILE + "'");
                new ProcessBuilder("minikube", "stop", "-p", PROFILE).inheritIO().start().waitFor();
            } catch (Exception e) {
                System.out.println("   couldn't stop it: " + e.getMessage());
            }
        }
    }
}
