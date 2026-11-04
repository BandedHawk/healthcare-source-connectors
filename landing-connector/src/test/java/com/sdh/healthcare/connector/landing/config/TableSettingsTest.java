/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableSettings}, and Iceberg sources in
 * {@link SourceSettings}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class TableSettingsTest
{

    /** A REST catalog's settings, with a secret. */
    static final Map<String, String> CATALOG = Map.of("type", "rest",
            "uri", "https://catalog", "credential", "id:s3cret");

    /**
     * Table settings as bound.
     *
     * @param catalog    the catalog's properties
     * @param identifier the table
     * @param start      where to start
     * @return the settings
     */
    static LandingProperties.Table table(Map<String, String> catalog,
            String identifier, String start)
    {
        return new LandingProperties.Table(catalog, "source", identifier,
                start);
    }

    /**
     * A source's settings, with a table.
     *
     * @param access the access
     * @param table  the table
     * @return the settings as bound
     */
    static LandingProperties.Source source(String access,
            LandingProperties.Table table)
    {
        return new LandingProperties.Source(null, access, null, null,
                ".landed", "settled", Duration.ofMinutes(2), "*", true,
                Duration.ofSeconds(30), "iceberg", Duration.ofMinutes(5),
                null, null, null, table);
    }

    /**
     * An iceberg source reads its table, settles nothing and leaves no
     * marker; its catalog's secrets are never shown.
     */
    @Test
    void readsAnIcebergSource()
    {
        SourceSettings settings = SourceSettings.of(source("read-only",
                table(CATALOG, "claims.raw", "latest")));
        assertThat(settings.type()).isEqualTo(SourceSettings.Type.ICEBERG);
        assertThat(settings.remote()).isTrue();
        assertThat(settings.settle()).isZero();
        assertThat(settings.marker()).isFalse();
        assertThat(settings.table().identifier())
                .isEqualTo(TableIdentifier.of("claims", "raw"));
        assertThat(settings.table().earliest()).isFalse();
        assertThat(settings.table().toString()).doesNotContain("s3cret");
        assertThat(table(CATALOG, "a.b", "earliest").toString())
                .doesNotContain("s3cret");
        assertThat(TableSettings.of(table(CATALOG, " a.b.c ", "Earliest")))
                .satisfies(t -> assertThat(t.earliest()).isTrue())
                .satisfies(t -> assertThat(t.identifier().namespace()
                        .levels()).containsExactly("a", "b"));
    }

    /** A table's files are never changed, so the source is read-only. */
    @Test
    void mustBeReadOnly()
    {
        assertThatThrownBy(() -> SourceSettings.of(source("read-write",
                table(CATALOG, "a.b", "earliest"))))
                .hasMessageContaining("read-only");
    }

    /** The catalog, its name, the table and the start are checked. */
    @Test
    void checksTheTable()
    {
        assertThatThrownBy(() -> TableSettings.of(null))
                .hasMessageContaining("catalog is required");
        assertThatThrownBy(() -> TableSettings.of(table(Map.of(), "a.b",
                "earliest"))).hasMessageContaining("catalog is required");
        assertThatThrownBy(() -> TableSettings.of(table(
                Map.of("uri", "x"), "a.b", "earliest")))
                .hasMessageContaining("type or a catalog-impl");
        assertThatThrownBy(() -> TableSettings.of(table(CATALOG, null,
                "earliest"))).hasMessageContaining("identifier");
        assertThatThrownBy(() -> TableSettings.of(table(CATALOG, "b",
                "earliest"))).hasMessageContaining("namespace.table");
        assertThatThrownBy(() -> TableSettings.of(table(CATALOG, "a.b",
                "now"))).hasMessageContaining("earliest or latest");
        assertThatThrownBy(() -> TableSettings.of(
                new LandingProperties.Table(CATALOG, " ", "a.b", "latest")))
                .hasMessageContaining("catalog-name");
    }
}
