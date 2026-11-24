/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IngestTables}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IngestTablesTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    IngestTablesTest()
    {
    }

    /**
     * A records table made before contract 2.1, without the line and column
     * columns, gets them when the connector starts; its other columns are
     * kept.
     */
    @Test
    void aTableFromAnEarlierContractGetsTheNewOptionalColumns()
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        catalog.createNamespace(Namespace.of("sdh_ingest"));
        // The 2.0 layout: the columns up to error_code.
        Schema earlier = new Schema(RecordsTable.SCHEMA.columns().stream()
                .filter(field -> field.fieldId() <= 17).toList());
        catalog.createTable(TableIdentifier.of("sdh_ingest",
                RecordsTable.DEFAULT_NAME), earlier);

        Table table = IngestTables.records(catalog, "sdh_ingest");

        for (Types.NestedField field : RecordsTable.SCHEMA.columns())
        {
            assertThat(table.schema().findField(field.name()))
                    .as(field.name()).isNotNull();
        }
        assertThat(table.schema().findField(RecordsTable.SOURCE_LINE).type())
                .isEqualTo(Types.LongType.get());
        // A second start finds nothing to add.
        int schemaId = table.schema().schemaId();
        assertThat(IngestTables.records(catalog, "sdh_ingest").schema()
                .schemaId()).isEqualTo(schemaId);
    }

    /**
     * The listener is told of each table as it is created or loaded, and a
     * refusal from it reaches the caller.
     */
    @Test
    void theListenerIsToldOfEachTableAndMayRefuseIt()
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        List<String> told = new ArrayList<>();
        try
        {
            IngestTables.whenOpened(table -> told.add(table.name()));
            IngestTables.records(catalog, "sdh_ingest");
            IngestTables.records(catalog, "sdh_ingest");
            assertThat(told).containsExactly("prod.sdh_ingest.records",
                    "prod.sdh_ingest.records");

            IngestTables.whenOpened(table ->
            {
                throw new IllegalStateException("refused");
            });
            assertThatThrownBy(() -> IngestTables.records(catalog,
                    "sdh_ingest")).hasMessage("refused");
        }
        finally
        {
            IngestTables.whenOpened(table ->
            {
            });
        }
    }
}
