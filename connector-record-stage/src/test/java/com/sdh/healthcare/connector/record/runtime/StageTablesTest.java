/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StageTables}: each stage's defaults, what must be set,
 * and the sink's KEK.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageTablesTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageTablesTest()
    {
    }

    /**
     * An in-memory catalog's properties.
     *
     * @return them
     */
    static Map<String, String> inMemory()
    {
        return Map.of("catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog", "warehouse",
                "memory://wh-" + UUID.randomUUID());
    }

    /**
     * A stage's settings with only its tables and records KEK set.
     *
     * @param source     the source's settings
     * @param sink       the sink's settings
     * @param recordsKek the records KEK; null if unset
     * @return the settings
     */
    static RecordStageSettings settings(StageProperties.TableSettings source,
            StageProperties.TableSettings sink, String recordsKek)
    {
        StageProperties stage = new StageProperties(null, null, null, 4,
                null, "sdh_stage", Duration.ofSeconds(30), null, -1,
                Duration.ofMinutes(1), source, sink, true);
        CommonSettings.Crypto crypto = new CommonSettings.Crypto("local",
                "local", null, new CommonSettings.Keys(recordsKek, null, null,
                        null, null, null), null, null, null, null);
        return new RecordStageSettings()
        {
            @Override
            public StageProperties stage()
            {
                return stage;
            }

            @Override
            public CommonSettings.Crypto crypto()
            {
                return crypto;
            }

            @Override
            public Results results()
            {
                return null;
            }

            @Override
            public Reconcile reconcile()
            {
                return null;
            }

            @Override
            public Maintenance maintenance()
            {
                return null;
            }

            @Override
            public String connectorId()
            {
                return "test";
            }

            @Override
            public CommonSettings.Lease lease()
            {
                return null;
            }

            @Override
            public CommonSettings.Retry retry()
            {
                return null;
            }

            @Override
            public CommonSettings.Telemetry telemetry()
            {
                return null;
            }
        };
    }

    /**
     * Table settings.
     *
     * @param catalog its catalog's properties; null for none
     * @param table   the table; null for the default
     * @param kek     its KEK; null for the default
     * @return the settings
     */
    static StageProperties.TableSettings table(Map<String, String> catalog,
            String table, String kek)
    {
        return new StageProperties.TableSettings(catalog, null, null, table,
                kek);
    }

    /**
     * The convert stage reads {@code split_records} and stores in
     * {@code records}; the transform stage reads {@code records}, its sink
     * named; both default to catalog {@code sdh}, namespace
     * {@code sdh_ingest}, and the sink shares the source's catalog.
     *
     * @throws Exception if a catalog can't be closed
     */
    @Test
    void givesEachStageItsDefaults() throws Exception
    {
        try (StageTables convert = new StageTables(settings(
                table(inMemory(), null, null), table(null, null, null),
                "records-kek/1"), StageMessages.CONVERT))
        {
            assertThat(convert.resolved()).containsEntry("source",
                    new StageTables.Resolved("sdh", "sdh_ingest",
                            "split_records"))
                    .containsEntry("sink", new StageTables.Resolved("sdh",
                            "sdh_ingest", "records"));
            assertThat(convert.sinkCatalog())
                    .isSameAs(convert.sourceCatalog());
            assertThat(convert.sink().table().name()).endsWith("records");
            assertThatThrownBy(convert::existingSource)
                    .hasMessageContaining("the stage before has never run");
            convert.source();
            assertThat(convert.existingSource().name())
                    .isEqualTo("split_records");
        }
        try (StageTables transform = new StageTables(settings(
                table(inMemory(), null, null), table(inMemory(), "bundles",
                        null), "records-kek/1"), StageMessages.TRANSFORM))
        {
            assertThat(transform.resolved()).containsEntry("source",
                    new StageTables.Resolved("sdh", "sdh_ingest", "records"))
                    .containsEntry("sink", new StageTables.Resolved("sdh",
                            "sdh_ingest", "bundles"));
            assertThat(transform.sinkCatalog())
                    .isNotSameAs(transform.sourceCatalog());
            assertThatThrownBy(transform::existingSink)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bundles");
        }
    }

    /**
     * The source's catalog is required, as is the transform stage's sink.
     */
    @Test
    void requiresASourceCatalogAndTheTransformsSink()
    {
        assertThatThrownBy(() -> new StageTables(settings(
                table(null, null, null), table(null, null, null), null),
                StageMessages.CONVERT))
                .hasMessage("sdh.stage.source.catalog is required");
        assertThatThrownBy(() -> new StageTables(settings(
                table(inMemory(), null, null), table(null, null, null), null),
                StageMessages.TRANSFORM))
                .hasMessage("sdh.stage.sink.table is required");
    }

    /**
     * The sink's KEK is its own if set, else the records KEK; one is
     * required.
     */
    @Test
    void takesTheSinksKekOrTheRecordsKek()
    {
        StageProperties.TableSettings source = table(inMemory(), null, null);
        assertThat(StageTables.sinkKek(settings(source,
                table(null, null, "gold-kek/1"), "records-kek/1")))
                .isEqualTo("gold-kek/1");
        assertThat(StageTables.sinkKek(settings(source,
                table(null, null, null), "records-kek/1")))
                .isEqualTo("records-kek/1");
        assertThatThrownBy(() -> StageTables.sinkKek(settings(source,
                table(null, null, null), null)))
                .hasMessage("sdh.stage.sink.kek is required");
    }
}
