/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Postgres as production runs it (HIPAA.md §8.1 item 7): TLS only, the
 * server's certificate from the customer's CA, and the stages connecting
 * with {@code sslmode=verify-full}, which the compliance profile requires.
 * The lease database's pool, which the registries and the audit journal
 * also use, and the Iceberg JDBC catalog, with its {@code jdbc.*}
 * properties, each connect over TLS; a server whose certificate isn't from
 * the CA given is refused, and so is a connection without TLS.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PostgresTlsIT
{

    /** Every connection over TLS, by password; anything else refused. */
    static final String HBA = """
            local all all trust
            hostssl all all all scram-sha-256
            host all all all reject
            """;

    /** The test CA. */
    static ItPki pki;

    /** The CA's certificate, which the stages trust. */
    static Path trustedCa;

    /** The database. */
    static PostgreSQLContainer postgres;

    /** Creates the tests; JUnit runs each test on a new instance. */
    PostgresTlsIT()
    {
    }

    /**
     * Starts Postgres with TLS on, and a certificate naming the address
     * the tests reach it at.
     *
     * @throws Exception if it can't start
     */
    @BeforeAll
    static void start() throws Exception
    {
        requireDocker();
        pki = new ItPki("sdh-postgres-ca");
        trustedCa = Files.writeString(Files.createTempFile("postgres-ca",
                ".pem"), pki.caPem());
        ItPki.Issued server = pki.server(
                DockerClientFactory.instance().dockerHostIpAddress(),
                "localhost");
        // The key must belong to the server's user, with no one else able
        // to read it, or Postgres won't start.
        postgres = new PostgreSQLContainer("postgres:18.4");
        postgres.withCopyToContainer(Transferable.of(pki.chainPem(server)),
                        "/tmp/sdh/server.crt")
                .withCopyToContainer(Transferable.of(ItPki.keyPem(server)),
                        "/tmp/sdh/server.key")
                .withCopyToContainer(Transferable.of(HBA),
                        "/tmp/sdh/pg_hba.conf")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "sh", "-c", "install -o postgres -m 0600"
                                + " /tmp/sdh/server.key /tmp/sdh/server.crt"
                                + " /tmp/sdh/pg_hba.conf /var/lib/postgresql/"
                                + " && exec docker-entrypoint.sh postgres"
                                + " -c ssl=on"
                                + " -c ssl_cert_file=/var/lib/postgresql/"
                                + "server.crt"
                                + " -c ssl_key_file=/var/lib/postgresql/"
                                + "server.key"
                                + " -c hba_file=/var/lib/postgresql/"
                                + "pg_hba.conf"));
        postgres.start();
    }

    /** Stops Postgres. */
    @AfterAll
    static void stop()
    {
        if (postgres != null)
        {
            postgres.stop();
        }
    }

    /**
     * The lease pool connects with {@code verify-full} over TLS, and the
     * Iceberg JDBC catalog does, given its {@code jdbc.*} properties.
     *
     * @throws Exception if the test fails
     */
    @Test
    void theLeasePoolAndTheCatalogConnectOverVerifiedTls() throws Exception
    {
        try (HikariDataSource pool = LeaseDatabase.dataSource(jdbc(
                url("sslmode=verify-full&sslrootcert=" + trustedCa)));
                Connection connection = pool.getConnection();
                Statement statement = connection.createStatement();
                ResultSet ssl = statement.executeQuery("SELECT ssl, version"
                        + " FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
        {
            assertThat(ssl.next()).isTrue();
            assertThat(ssl.getBoolean(1)).as("over TLS").isTrue();
            assertThat(ssl.getString(2)).startsWith("TLSv1.");
        }

        Map<String, String> properties = new HashMap<>();
        properties.put("type", "jdbc");
        properties.put("uri", url(""));
        properties.put("jdbc.user", postgres.getUsername());
        properties.put("jdbc.password", postgres.getPassword());
        properties.put("jdbc.sslmode", "verify-full");
        properties.put("jdbc.sslrootcert", trustedCa.toString());
        properties.put("jdbc.schema-version", "V1");
        properties.put("warehouse", Files.createTempDirectory("warehouse")
                .toUri().toString());
        Catalog catalog = CatalogUtil.buildIcebergCatalog("sdh", properties,
                null);
        try
        {
            SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
            namespaces.createNamespace(Namespace.of("sdh_tls"));
            assertThat(namespaces.listNamespaces())
                    .contains(Namespace.of("sdh_tls"));
        }
        finally
        {
            ((AutoCloseable) catalog).close();
        }
    }

    /**
     * A server whose certificate isn't from the CA given is refused, and so
     * is a connection without TLS.
     *
     * @throws Exception if the test fails
     */
    @Test
    void anUntrustedServerAndAPlaintextConnectionAreRefused()
            throws Exception
    {
        Path otherCa = Files.writeString(Files.createTempFile("other-ca",
                ".pem"), new ItPki("sdh-other-ca").caPem());
        assertThatThrownBy(() -> LeaseDatabase.dataSource(jdbc(url(
                "sslmode=verify-full&sslrootcert=" + otherCa))).close())
                .as("an untrusted certificate")
                .hasStackTraceContaining("PKIX");
        assertThatThrownBy(() -> LeaseDatabase.dataSource(jdbc(url(
                "sslmode=disable"))).close())
                .as("no TLS")
                .hasStackTraceContaining("pg_hba.conf");
    }

    /**
     * The database's JDBC URL with options.
     *
     * @param options the URL's query, without its {@code ?}; may be empty
     * @return the URL
     */
    static String url(String options)
    {
        return "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
                + "/" + postgres.getDatabaseName()
                + (options.isEmpty() ? "" : "?" + options);
    }

    /**
     * The lease database's settings for a URL.
     *
     * @param url the URL
     * @return the settings
     */
    static CommonSettings.Jdbc jdbc(String url)
    {
        return new CommonSettings.Jdbc(url, postgres.getUsername(),
                postgres.getPassword(), "sdh_lease", 2);
    }
}
