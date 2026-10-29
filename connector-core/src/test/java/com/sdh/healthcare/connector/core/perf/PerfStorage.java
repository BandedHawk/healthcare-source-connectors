package com.sdh.healthcare.connector.core.perf;

import com.sdh.healthcare.connector.core.LocalFileIO;
import com.sdh.healthcare.connector.core.TestPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Where throughput runs store their tables: a JDBC catalog on embedded Postgres in both cases, with data either on
 * local disk ({@code local}) or in SeaweedFS through {@code S3FileIO} ({@code s3}, needs Docker; on minikube the
 * store runs in the VM, so uploads cross the VM's virtual network).
 */
abstract class PerfStorage implements AutoCloseable {
    private final Catalog catalog;

    PerfStorage(Map<String, String> props) {
        Map<String, String> p = new HashMap<>(props);
        p.put("type", "jdbc");
        p.put("uri", TestPostgres.jdbcUrl());
        p.put("jdbc.schema-version", "V1");
        this.catalog = CatalogUtil.buildIcebergCatalog("perf", p, null);
    }

    static PerfStorage create(String kind) throws Exception {
        return switch (kind) {
            case "local" -> new Local();
            case "s3" -> new SeaweedFs();
            default -> throw new IllegalArgumentException("perf.storage must be local or s3");
        };
    }

    Catalog catalog() {
        return catalog;
    }

    abstract String describe();

    /** Drops the run's tables and their files, so a multi-GB run doesn't accumulate. */
    void dropNamespace(String ns) {
        for (TableIdentifier t : catalog.listTables(Namespace.of(ns))) {
            catalog.dropTable(t, true);
        }
        ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(ns));
    }

    @Override
    public void close() {
        if (catalog instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // done
            }
        }
    }

    static final class Local extends PerfStorage {
        private static final Path DIR = Path.of("target", "perf", "warehouse").toAbsolutePath();

        Local() {
            super(Map.of("warehouse", DIR.toString(), "io-impl", LocalFileIO.class.getName()));
        }

        @Override
        String describe() {
            return "local disk (" + DIR + ") via a JDBC catalog on embedded Postgres";
        }

        @Override
        public void close() {
            super.close();
            if (Files.exists(DIR)) {
                try (Stream<Path> s = Files.walk(DIR)) {
                    for (Path p : s.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(p);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    static final class SeaweedFs extends PerfStorage {
        static final String ACCESS_KEY = "sdh-perf-access";
        static final String SECRET_KEY = "sdh-perf-secret-key-0123456789";
        static final String BUCKET = "sdh-perf";
        private static GenericContainer<?> container;

        SeaweedFs() {
            super(props());
        }

        private static synchronized Map<String, String> props() {
            if (container == null) {
                String identities = """
                        {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
                          "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);
                container = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                        .withCopyToContainer(Transferable.of(identities), "/etc/seaweedfs/s3.json")
                        .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                                "-ip.bind=0.0.0.0", "-volume.max=50")
                        .withExposedPorts(8333)
                        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
                container.start();
                try (S3Client s3 = S3Client.builder().endpointOverride(URI.create(endpoint())).region(Region.US_EAST_1)
                        .forcePathStyle(true)
                        .credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                        .build()) {
                    s3.createBucket(b -> b.bucket(BUCKET));
                }
            }
            return Map.of("warehouse", "s3://" + BUCKET + "/wh", "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.endpoint", endpoint(), "s3.path-style-access", "true", "s3.access-key-id", ACCESS_KEY,
                    "s3.secret-access-key", SECRET_KEY, "client.region", "us-east-1");
        }

        private static String endpoint() {
            return "http://" + container.getHost() + ":" + container.getMappedPort(8333);
        }

        @Override
        String describe() {
            return "SeaweedFS 4.48 at " + endpoint() + " via S3FileIO, JDBC catalog on embedded Postgres";
        }

        @Override
        public void close() {
            super.close();
            if (container != null) {
                container.stop();
                container = null;
            }
        }
    }
}
