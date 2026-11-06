/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;

/**
 * Labels the records of a Parquet file (design §6.4). The Parquet
 * splitter already makes each row a JSON object
 * ({@link ParquetRowJson}), since a row isn't a stretch of the file's
 * bytes; this converter passes it on unchanged, so it is stored and
 * announced as JSON with the mapping {@value ParquetRowJson#MAPPING}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ParquetConverter implements RecordConverter
{

    /**
     * Creates the converter; the service loader calls this.
     */
    public ParquetConverter()
    {
    }

    @Override
    public String format()
    {
        return "parquet";
    }

    @Override
    public String mapping()
    {
        return ParquetRowJson.MAPPING;
    }

    /**
     * A copy needs the record twice.
     *
     * @return 2
     */
    @Override
    public double memoryFactor()
    {
        return 2;
    }

    /**
     * Returns the record's JSON as it is, in a new array.
     *
     * @param record the row as JSON
     * @param config not used
     * @return a copy of it
     */
    @Override
    public byte[] convert(byte[] record, SplitConfig config)
    {
        return record.clone();
    }
}
