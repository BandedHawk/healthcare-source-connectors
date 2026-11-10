/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.runtime;

import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.record.RecordStageProcessor;
import com.sdh.healthcare.connector.record.RecordTable;
import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * A record stage's source and sink, as {@code sdh.stage.source} and
 * {@code sdh.stage.sink} give them (STAGES.md §17.7), with each stage's
 * defaults: the convert stage reads {@code split_records} and stores in
 * {@code records}; the transform stage reads {@code records}, and its sink
 * must be named. Both are in catalog {@code sdh}, namespace
 * {@code sdh_ingest}, unless set. The sink is in the source's catalog
 * unless it has catalog properties of its own.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageTables implements AutoCloseable
{

    /** The catalog name, unless set. */
    static final String DEFAULT_CATALOG = "sdh";

    /** The namespace, unless set. */
    static final String DEFAULT_NAMESPACE = "sdh_ingest";

    /** Where the settings are, to name them in errors. */
    static final String SOURCE = StageProperties.PREFIX + ".source";

    /** Where the settings are, to name them in errors. */
    static final String SINK = StageProperties.PREFIX + ".sink";

    /** The source's catalog. */
    private final Catalog sourceCatalog;

    /** The sink's catalog: the source's, or one of its own. */
    private final Catalog sinkCatalog;

    /** The source's settings, its defaults filled in. */
    private final Resolved source;

    /** The sink's settings, its defaults filled in. */
    private final Resolved sink;

    /**
     * A table's settings, its defaults filled in.
     *
     * @param catalogName the catalog's name
     * @param namespace   the namespace
     * @param table       the table's name
     */
    record Resolved(String catalogName, String namespace, String table)
    {
    }

    /**
     * Builds the catalogs; no table is loaded yet.
     *
     * @param settings the stage's settings
     * @param messages the stage's messages, for its defaults
     * @throws IllegalArgumentException if no catalog is set, or the
     *                                  transform stage's sink isn't named
     */
    public StageTables(RecordStageSettings settings, StageMessages messages)
    {
        StageProperties.TableSettings from = settings.stage().source();
        StageProperties.TableSettings to = settings.stage().sink();
        this.source = resolve(from, defaultSource(messages), SOURCE);
        this.sink = resolve(to, defaultSink(messages), SINK);
        if (from.catalog().isEmpty())
        {
            throw new IllegalArgumentException(SOURCE
                    + ".catalog is required");
        }
        this.sourceCatalog = CatalogUtil.buildIcebergCatalog(
                source.catalogName(), from.catalog(), null);
        this.sinkCatalog = to.catalog().isEmpty() ? sourceCatalog
                : CatalogUtil.buildIcebergCatalog(sink.catalogName(),
                        to.catalog(), null);
    }

    /**
     * The source table a stage reads by default.
     *
     * @param messages the stage's messages
     * @return the table's name
     */
    static String defaultSource(StageMessages messages)
    {
        return switch (messages)
        {
            case CONVERT -> IngestTables.SPLIT_RECORDS;
            case TRANSFORM -> RecordsTable.DEFAULT_NAME;
        };
    }

    /**
     * The sink table a stage stores in by default.
     *
     * @param messages the stage's messages
     * @return the table's name; null if it has none and must be set
     */
    static String defaultSink(StageMessages messages)
    {
        return switch (messages)
        {
            case CONVERT -> RecordsTable.DEFAULT_NAME;
            case TRANSFORM -> null;
        };
    }

    /**
     * Fills a table's settings in with the defaults.
     *
     * @param settings     the settings
     * @param defaultTable the table's name, unless set; null if it must be
     * @param prefix       where the settings are, for errors
     * @return the settings, filled in
     * @throws IllegalArgumentException if the table must be set and isn't
     */
    private static Resolved resolve(StageProperties.TableSettings settings,
            String defaultTable, String prefix)
    {
        String table = orDefault(settings.table(), defaultTable);
        if (table == null)
        {
            throw new IllegalArgumentException(prefix + ".table is required");
        }
        return new Resolved(orDefault(settings.catalogName(),
                DEFAULT_CATALOG), orDefault(settings.namespace(),
                DEFAULT_NAMESPACE), table);
    }

    /**
     * A setting, or its default if unset.
     *
     * @param value        the setting
     * @param defaultValue its default
     * @return the setting, if it isn't null or blank
     */
    private static String orDefault(String value, String defaultValue)
    {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * Opens the source, created if missing (the stage before may not have
     * run yet), with the same settings the stage before would give it.
     *
     * @return the source
     */
    public RecordTable source()
    {
        return new RecordTable(source.catalogName(), source.namespace(),
                source.table(), IngestTables.shared(sourceCatalog,
                        source.namespace(), source.table()));
    }

    /**
     * Opens the sink, created if missing, shared by every pod (§9).
     *
     * @return the sink
     */
    public RecordTable sink()
    {
        return new RecordTable(sink.catalogName(), sink.namespace(),
                sink.table(), IngestTables.shared(sinkCatalog,
                        sink.namespace(), sink.table()));
    }

    /**
     * Loads the source, which must exist: for an operator command, which
     * never creates a table.
     *
     * @return the source
     * @throws IllegalStateException if it doesn't exist
     */
    public RecordTable existingSource()
    {
        return existing(sourceCatalog, source, "the stage before");
    }

    /**
     * Loads the sink, which must exist: for an operator command.
     *
     * @return the sink
     * @throws IllegalStateException if it doesn't exist
     */
    public RecordTable existingSink()
    {
        return existing(sinkCatalog, sink, "the stage");
    }

    /**
     * Loads a table that must exist.
     *
     * @param catalog its catalog
     * @param table   its settings
     * @param owner   whose table it is, for the error
     * @return the table
     * @throws IllegalStateException if it doesn't exist
     */
    private static RecordTable existing(Catalog catalog, Resolved table,
            String owner)
    {
        try
        {
            return new RecordTable(table.catalogName(), table.namespace(),
                    table.table(), catalog.loadTable(TableIdentifier.of(
                            table.namespace(), table.table())));
        }
        catch (NoSuchTableException missing)
        {
            throw new IllegalStateException("Table " + table.table()
                    + " does not exist: " + owner + " has never run here");
        }
    }

    /**
     * The tables before the source whose failures results report
     * ({@code sdh.results.upstream}, STAGES.md §17.5), each in the
     * source's catalog, created if missing.
     *
     * @param upstream the settings
     * @return the tables, earliest first
     * @throws IllegalArgumentException if one has no stage or table named
     */
    public List<RecordStageProcessor.Earlier> earlier(
            List<RecordStageSettings.Upstream> upstream)
    {
        List<RecordStageProcessor.Earlier> tables = new ArrayList<>();
        for (RecordStageSettings.Upstream table : upstream)
        {
            if (orDefault(table.stage(), null) == null
                    || orDefault(table.table(), null) == null)
            {
                throw new IllegalArgumentException(
                        "sdh.results.upstream: each needs a stage and a"
                                + " table");
            }
            tables.add(new RecordStageProcessor.Earlier(table.stage(),
                    IngestTables.shared(sourceCatalog, orDefault(
                            table.namespace(), source.namespace()),
                            table.table())));
        }
        return tables;
    }

    /**
     * The source's catalog, for the reader.
     *
     * @return the catalog
     */
    public Catalog sourceCatalog()
    {
        return sourceCatalog;
    }

    /**
     * The sink's catalog: the source's, unless the sink has its own.
     *
     * @return the catalog
     */
    public Catalog sinkCatalog()
    {
        return sinkCatalog;
    }

    /**
     * The sink's KEK: {@code sdh.stage.sink.kek}, or the records KEK
     * ({@code sdh.crypto.keys.records-kek}) if unset.
     *
     * @param settings the stage's settings
     * @return the KEK's reference
     * @throws IllegalArgumentException if neither is set
     */
    public static String sinkKek(RecordStageSettings settings)
    {
        String kek = orDefault(settings.stage().sink().kek(),
                settings.crypto().keys().recordsKek());
        if (kek == null)
        {
            throw new IllegalArgumentException(SINK + ".kek is required");
        }
        return kek;
    }

    /**
     * Closes the catalogs, if they hold anything to close.
     *
     * @throws Exception if one can't be closed
     */
    @Override
    public void close() throws Exception
    {
        if (sinkCatalog != sourceCatalog
                && sinkCatalog instanceof AutoCloseable closeable)
        {
            closeable.close();
        }
        if (sourceCatalog instanceof AutoCloseable closeable)
        {
            closeable.close();
        }
    }

    /**
     * Each setting's value, for a test.
     *
     * @return the source's and the sink's settings, filled in
     */
    Map<String, Resolved> resolved()
    {
        return Map.of("source", source, "sink", sink);
    }
}
