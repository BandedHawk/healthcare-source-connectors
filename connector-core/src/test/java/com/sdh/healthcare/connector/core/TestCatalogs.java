package com.sdh.healthcare.connector.core;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;

/**
 * Catalog fixtures. Each {@link Kind} can hand out several independent clients for the same catalog,
 * which stand in for separate connector processes.
 */
public final class TestCatalogs {

    public enum Kind { JDBC, REST }

    /** A shared catalog backend; {@link #newClient()} gives a fresh client with its own table cache. */
    public interface Backend {
        Catalog newClient();
    }

    private TestCatalogs() {}

    public static Backend create(Kind kind) {
        return switch (kind) {
            case JDBC -> jdbc();
            case REST -> rest();
        };
    }

    /** JDBC catalog on embedded Postgres. Metadata commits are a CAS on metadata_location. */
    public static Backend jdbc() {
        String name = "jdbc_" + UUID.randomUUID().toString().replace("-", "");
        return () -> {
            JdbcCatalog c = new JdbcCatalog();
            c.initialize(name, Map.of(
                    CatalogProperties.URI, TestPostgres.jdbcUrl(),
                    CatalogProperties.WAREHOUSE_LOCATION, "memory://" + name,
                    CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName(),
                    "jdbc.schema-version", "V1"));
            return c;
        };
    }

    /**
     * REST catalog client talking to Iceberg's reference server logic in-process. The adapter applies
     * the same UpdateRequirements checks a REST catalog server does.
     */
    public static Backend rest() {
        InMemoryCatalog server = new InMemoryCatalog();
        server.initialize("server", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://rest-" + UUID.randomUUID()));
        RESTCatalogAdapter adapter = new RESTCatalogAdapter(server);
        return () -> {
            RESTCatalog c = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), cfg -> adapter);
            c.initialize("rest", Map.of(
                    CatalogProperties.URI, "http://in-process",
                    CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
            return c;
        };
    }
}
