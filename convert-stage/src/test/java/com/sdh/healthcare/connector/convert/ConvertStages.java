/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.record.BacklogWatch;
import com.sdh.healthcare.connector.record.RecordStageProcessor;
import com.sdh.healthcare.connector.record.RecordTable;
import com.sdh.healthcare.connector.record.ResultReporter;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;

/**
 * Builds the convert stage as it runs, for the tests: the shared record
 * stage's processor with the {@link ConvertTransform}, reading
 * {@code split_records} and storing in {@code records}, both in catalog
 * {@code prod}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ConvertStages
{

    /** The catalog's name in the tests. */
    static final String CATALOG = "prod";

    /** Not instantiated. */
    private ConvertStages()
    {
    }

    /**
     * The convert stage's transform.
     *
     * @param options  format options
     * @param maxBytes the largest record converted
     * @param build    the build named in versions
     * @return the transform
     */
    static ConvertTransform transform(Map<String, String> options,
            long maxBytes, String build)
    {
        return new ConvertTransform(ConverterRegistry.load(),
                new ConvertTransform.Settings(options, maxBytes, build));
    }

    /**
     * The tables, as the stage names them.
     *
     * @param namespace    their namespace
     * @param splitRecords the source
     * @param records      the sink
     * @param keys         wraps the sink's data keys
     * @param kek          the sink's KEK
     * @return the tables, none earlier than the source
     */
    static RecordStageProcessor.Tables tables(String namespace,
            Table splitRecords, Table records, KeyProvider keys, String kek)
    {
        return new RecordStageProcessor.Tables(
                source(namespace, splitRecords),
                new RecordStageProcessor.Sink(new RecordTable(CATALOG,
                        namespace, "records", records), keys, kek,
                        new RecordIds(new byte[32])), List.of());
    }

    /**
     * The source, as the stage names it.
     *
     * @param namespace    its namespace
     * @param splitRecords the table
     * @return it
     */
    static RecordTable source(String namespace, Table splitRecords)
    {
        return new RecordTable(CATALOG, namespace,
                IngestTables.SPLIT_RECORDS, splitRecords);
    }

    /**
     * The convert stage's processor.
     *
     * @param transform its transform
     * @param reader    reads split records
     * @param tables    its tables
     * @param results   sends results; null if off
     * @param clock     stamps batches
     * @param backlog   the backlog watch; null if none
     * @return the processor
     */
    static RecordStageProcessor processor(ConvertTransform transform,
            QueueReader reader, RecordStageProcessor.Tables tables,
            ResultReporter results, Clock clock, BacklogWatch backlog)
    {
        return new RecordStageProcessor(transform, reader, tables, 64L << 20,
                results, clock, backlog);
    }
}
