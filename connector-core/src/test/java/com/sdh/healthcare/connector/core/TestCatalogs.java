/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * Catalog fixtures. Each {@link Kind} can hand out several independent
 * clients for the same catalog, which stand in for separate connector
 * processes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestCatalogs
{

    /** The kinds of catalog. */
    public enum Kind
    {
        /** A JDBC catalog. */
        JDBC,

        /** A REST catalog. */
        REST
    }

    /**
     * A shared catalog backend.
     */
    public interface Backend
    {

        /**
         * A fresh client with its own table cache.
         *
         * @return the client
         */
        Catalog newClient();
    }

    /** Not instantiable. */
    private TestCatalogs()
    {
    }

    /**
     * A new backend of a kind.
     *
     * @param kind the kind
     * @return the backend
     */
    public static Backend create(Kind kind)
    {
        return switch (kind)
        {
            case JDBC ->
                jdbc();
            case REST ->
                rest();
        };
    }

    /**
     * A JDBC catalog on embedded Postgres. Metadata commits are a CAS on
     * metadata_location.
     *
     * @return the backend
     */
    public static Backend jdbc()
    {
        String name = "jdbc_" + UUID.randomUUID().toString().replace("-", "");
        return () ->
        {
            JdbcCatalog catalog = new JdbcCatalog();
            catalog.initialize(name, Map.of(
                    CatalogProperties.URI, TestPostgres.jdbcUrl(),
                    CatalogProperties.WAREHOUSE_LOCATION, "memory://" + name,
                    CatalogProperties.FILE_IO_IMPL,
                    InMemoryFileIO.class.getName(),
                    "jdbc.schema-version", "V1"));
            return catalog;
        };
    }

    /**
     * A REST catalog client talking to Iceberg's reference server logic
     * in-process. The adapter applies the same UpdateRequirements checks a
     * REST catalog server does.
     *
     * @return the backend
     */
    public static Backend rest()
    {
        InMemoryCatalog server = new InMemoryCatalog();
        server.initialize("server", Map.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                "memory://rest-" + UUID.randomUUID()));
        RESTCatalogAdapter adapter = new RESTCatalogAdapter(server);
        return () ->
        {
            RESTCatalog catalog = new RESTCatalog(
                    SessionCatalog.SessionContext.createEmpty(),
                    properties -> adapter);
            catalog.initialize("rest", Map.of(
                    CatalogProperties.URI, "http://in-process",
                    CatalogProperties.FILE_IO_IMPL,
                    InMemoryFileIO.class.getName()));
            return catalog;
        };
    }
}
