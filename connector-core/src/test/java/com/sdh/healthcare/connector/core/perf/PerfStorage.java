/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * Where throughput runs store their tables: a JDBC catalog on embedded
 * Postgres in both cases, with data either on local disk ({@code local})
 * or in SeaweedFS through {@code S3FileIO} ({@code s3}, needs Docker; on
 * minikube the store runs in the VM, so uploads cross the VM's virtual
 * network).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
abstract class PerfStorage implements AutoCloseable
{

    /** The catalog the run's tables live in. */
    private final Catalog catalog;

    /**
     * Builds a JDBC catalog on the test Postgres.
     *
     * @param properties the storage's catalog properties
     */
    PerfStorage(Map<String, String> properties)
    {
        Map<String, String> all = new HashMap<>(properties);
        all.put("type", "jdbc");
        all.put("uri", TestPostgres.jdbcUrl());
        all.put("jdbc.schema-version", "V1");
        this.catalog = CatalogUtil.buildIcebergCatalog("perf", all, null);
    }

    /**
     * The storage of a kind.
     *
     * @param kind {@code local} or {@code s3}
     * @return the storage
     * @throws Exception if the storage can't start
     */
    static PerfStorage create(String kind) throws Exception
    {
        return switch (kind)
        {
            case "local" -> new Local();
            case "s3" -> new SeaweedFs();
            default -> throw new IllegalArgumentException(
                    "perf.storage must be local or s3");
        };
    }

    /**
     * The catalog.
     *
     * @return the catalog
     */
    Catalog catalog()
    {
        return catalog;
    }

    /**
     * Describes the storage for the run's report.
     *
     * @return the description
     */
    abstract String describe();

    /**
     * Drops the run's tables and their files, so a multi-GB run doesn't
     * accumulate.
     *
     * @param namespace the run's namespace
     */
    void dropNamespace(String namespace)
    {
        for (TableIdentifier table :
                catalog.listTables(Namespace.of(namespace)))
        {
            catalog.dropTable(table, true);
        }
        ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(namespace));
    }

    /** Closes the catalog, ignoring any failure. */
    @Override
    public void close()
    {
        if (catalog instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // done
            }
        }
    }

    /** Data on local disk, under {@code target/perf/warehouse}. */
    static final class Local extends PerfStorage
    {

        /** The warehouse directory. */
        private static final Path DIR =
                Path.of("target", "perf", "warehouse").toAbsolutePath();

        /** Creates the storage. */
        Local()
        {
            super(Map.of("warehouse", DIR.toString(), "io-impl",
                    LocalFileIO.class.getName()));
        }

        /**
         * Describes the storage.
         *
         * @return the description
         */
        @Override
        String describe()
        {
            return "local disk (" + DIR
                    + ") via a JDBC catalog on embedded Postgres";
        }

        /** Closes the catalog and deletes the warehouse directory. */
        @Override
        public void close()
        {
            super.close();
            if (Files.exists(DIR))
            {
                try (Stream<Path> paths = Files.walk(DIR))
                {
                    for (Path path :
                            paths.sorted(Comparator.reverseOrder()).toList())
                    {
                        Files.deleteIfExists(path);
                    }
                }
                catch (IOException failure)
                {
                    throw new UncheckedIOException(failure);
                }
            }
        }
    }

    /** Data in a SeaweedFS container, through {@code S3FileIO}. */
    static final class SeaweedFs extends PerfStorage
    {

        /** The S3 access key. */
        static final String ACCESS_KEY = "sdh-perf-access";

        /** The S3 secret key. */
        static final String SECRET_KEY = "sdh-perf-secret-key-0123456789";

        /** The bucket holding the warehouse. */
        static final String BUCKET = "sdh-perf";

        /** The running container, or null before the first use. */
        private static GenericContainer<?> container;

        /** Creates the storage, starting the container if needed. */
        SeaweedFs()
        {
            super(properties());
        }

        /**
         * Starts the container and makes the bucket on first use.
         *
         * @return the catalog properties for the container
         */
        private static synchronized Map<String, String> properties()
        {
            if (container == null)
            {
                String identities = """
                        {"identities":[{"name":"sdh","credentials":\
                        [{"accessKey":"%s","secretKey":"%s"}],
                          "actions":["Admin","Read","Write",\
                        "List","Tagging"]}]}"""
                        .formatted(ACCESS_KEY, SECRET_KEY);
                container = new GenericContainer<>(
                        "chrislusf/seaweedfs:4.48")
                        .withCopyToContainer(Transferable.of(identities),
                                "/etc/seaweedfs/s3.json")
                        .withCommand("server", "-s3",
                                "-s3.config=/etc/seaweedfs/s3.json",
                                "-dir=/data", "-ip.bind=0.0.0.0",
                                "-volume.max=50")
                        .withExposedPorts(8333)
                        .waitingFor(Wait.forListeningPort()
                                .withStartupTimeout(Duration.ofMinutes(2)));
                container.start();
                try (S3Client s3 = S3Client.builder()
                        .endpointOverride(URI.create(endpoint()))
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
            return Map.of("warehouse", "s3://" + BUCKET + "/wh",
                    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.endpoint", endpoint(),
                    "s3.path-style-access", "true",
                    "s3.access-key-id", ACCESS_KEY,
                    "s3.secret-access-key", SECRET_KEY,
                    "client.region", "us-east-1");
        }

        /**
         * The container's S3 endpoint.
         *
         * @return the endpoint URL
         */
        private static String endpoint()
        {
            return "http://" + container.getHost() + ":"
                    + container.getMappedPort(8333);
        }

        /**
         * Describes the storage.
         *
         * @return the description
         */
        @Override
        String describe()
        {
            return "SeaweedFS 4.48 at " + endpoint()
                    + " via S3FileIO, JDBC catalog on embedded Postgres";
        }

        /** Closes the catalog and stops the container. */
        @Override
        public void close()
        {
            super.close();
            if (container != null)
            {
                container.stop();
                container = null;
            }
        }
    }
}
