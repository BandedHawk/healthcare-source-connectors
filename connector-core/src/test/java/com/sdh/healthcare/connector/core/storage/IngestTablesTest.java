/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
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
     * A sections table made before contract 2.1, without the line and column
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
        Schema earlier = new Schema(SectionsTable.SCHEMA.columns().stream()
                .filter(field -> field.fieldId() <= 17).toList());
        catalog.createTable(TableIdentifier.of("sdh_ingest",
                SectionsTable.DEFAULT_NAME), earlier);

        Table table = IngestTables.sections(catalog, "sdh_ingest");

        for (Types.NestedField field : SectionsTable.SCHEMA.columns())
        {
            assertThat(table.schema().findField(field.name()))
                    .as(field.name()).isNotNull();
        }
        assertThat(table.schema().findField(SectionsTable.SOURCE_LINE).type())
                .isEqualTo(Types.LongType.get());
        // A second start finds nothing to add.
        int schemaId = table.schema().schemaId();
        assertThat(IngestTables.sections(catalog, "sdh_ingest").schema()
                .schemaId()).isEqualTo(schemaId);
    }
}
