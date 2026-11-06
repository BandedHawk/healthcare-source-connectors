/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.RecordConverter;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * Settings for {@link IngestPipeline} (design §8.3, §13, §14).
 *
 * @param connectorId        names the checkpoint property, e.g.
 *                           {@code file-connector}
 * @param format             split format, e.g. {@code ccda}
 * @param split              how the format is split
 * @param catalogName        catalog name put in messages, so readers can
 *                           check they look in the right place
 * @param kekRef             KEK for batch data keys of the records table
 * @param commitMaxRecords   commit after this many records (default
 *                           10,000)
 * @param commitMaxBytes     … or this many plaintext bytes (default
 *                           512 MiB)
 * @param commitMaxInterval  … or when a batch has been open this long
 *                           (default 30 s)
 * @param encryptWorkers     threads for hashing, compressing, encrypting
 *                           and sealing (default: CPU cores)
 * @param retry              how transient errors are retried
 * @param converters         JSON converters by format (design §6.4);
 *                           {@link ConverterRegistry#none()} stores every
 *                           record as split
 * @param convertMaxBytes    records larger than this aren't converted
 *                           (they'd need too much memory) and are stored in
 *                           their original form as failed,
 *                           {@code CONVERT_TOO_LARGE} (default 64 MiB)
 * @param convertMemoryBytes heap that conversions running at once may use
 *                           between them, by each converter's
 *                           {@link RecordConverter#memoryFactor()}; a
 *                           conversion that doesn't fit waits for others to
 *                           finish. 0 (the default) means
 *                           {@link #DEFAULT_CONVERT_MEMORY_SHARE} of the
 *                           maximum heap
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record PipelineSettings(
        String connectorId,
        String format,
        SplitConfig split,
        String catalogName,
        String kekRef,
        int commitMaxRecords,
        long commitMaxBytes,
        Duration commitMaxInterval,
        int encryptWorkers,
        RetryPolicy retry,
        ConverterRegistry converters,
        long convertMaxBytes,
        long convertMemoryBytes)
{

    /**
     * The share of the maximum heap that conversions may use when
     * {@code convertMemoryBytes} is 0.
     */
    public static final double DEFAULT_CONVERT_MEMORY_SHARE = 0.4;

    /**
     * Checks the settings.
     *
     * @param connectorId        names the checkpoint property
     * @param format             split format
     * @param split              how the format is split
     * @param catalogName        catalog name put in messages
     * @param kekRef             KEK for batch data keys
     * @param commitMaxRecords   records per commit
     * @param commitMaxBytes     plaintext bytes per commit
     * @param commitMaxInterval  how long a batch may stay open
     * @param encryptWorkers     worker threads
     * @param retry              how transient errors are retried
     * @param converters         JSON converters by format
     * @param convertMaxBytes    largest record converted
     * @param convertMemoryBytes heap for conversions; 0 for the default
     * @throws NullPointerException     if a required setting is null
     * @throws IllegalArgumentException if a threshold, the worker count or
     *                                  a conversion limit isn't positive
     */
    public PipelineSettings
    {
        Objects.requireNonNull(connectorId);
        Objects.requireNonNull(format);
        Objects.requireNonNull(split);
        Objects.requireNonNull(catalogName);
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(commitMaxInterval);
        Objects.requireNonNull(retry);
        Objects.requireNonNull(converters);
        if (commitMaxRecords < 1 || commitMaxBytes < 1 || encryptWorkers < 1
                || convertMaxBytes < 1 || convertMemoryBytes < 0)
        {
            throw new IllegalArgumentException("Commit thresholds, worker"
                    + " count and conversion limits must be positive");
        }
    }

    /**
     * The default settings, with the converters found on the class path.
     *
     * @param connectorId names the checkpoint property
     * @param format      split format
     * @param catalogName catalog name put in messages
     * @param kekRef      KEK for batch data keys
     * @return the settings
     */
    public static PipelineSettings defaults(String connectorId,
            String format, String catalogName, String kekRef)
    {
        return new PipelineSettings(connectorId, format,
                SplitConfig.defaults(format), catalogName, kekRef, 10_000,
                512L << 20, Duration.ofSeconds(30),
                Runtime.getRuntime().availableProcessors(),
                RetryPolicy.DEFAULT, ConverterRegistry.load(), 64L << 20, 0);
    }

    /**
     * A copy with other split settings.
     *
     * @param splitConfig how the format is split
     * @return the copy
     */
    public PipelineSettings withSplit(SplitConfig splitConfig)
    {
        return new PipelineSettings(connectorId, format, splitConfig,
                catalogName, kekRef, commitMaxRecords, commitMaxBytes,
                commitMaxInterval, encryptWorkers, retry, converters,
                convertMaxBytes, convertMemoryBytes);
    }

    /**
     * A copy with other commit thresholds.
     *
     * @param maxRecords  records per commit
     * @param maxBytes    plaintext bytes per commit
     * @param maxInterval how long a batch may stay open
     * @return the copy
     */
    public PipelineSettings withCommit(int maxRecords, long maxBytes,
            Duration maxInterval)
    {
        return new PipelineSettings(connectorId, format, split, catalogName,
                kekRef, maxRecords, maxBytes, maxInterval, encryptWorkers,
                retry, converters, convertMaxBytes, convertMemoryBytes);
    }

    /**
     * A copy with another worker count.
     *
     * @param workers worker threads
     * @return the copy
     */
    public PipelineSettings withWorkers(int workers)
    {
        return new PipelineSettings(connectorId, format, split, catalogName,
                kekRef, commitMaxRecords, commitMaxBytes, commitMaxInterval,
                workers, retry, converters, convertMaxBytes,
                convertMemoryBytes);
    }

    /**
     * A copy with another retry policy.
     *
     * @param policy how transient errors are retried
     * @return the copy
     */
    public PipelineSettings withRetry(RetryPolicy policy)
    {
        return new PipelineSettings(connectorId, format, split, catalogName,
                kekRef, commitMaxRecords, commitMaxBytes, commitMaxInterval,
                encryptWorkers, policy, converters, convertMaxBytes,
                convertMemoryBytes);
    }

    /**
     * A copy with other converters.
     *
     * @param registry JSON converters by format
     * @param maxBytes largest record converted
     * @return the copy
     */
    public PipelineSettings withConversion(ConverterRegistry registry,
            long maxBytes)
    {
        return new PipelineSettings(connectorId, format, split, catalogName,
                kekRef, commitMaxRecords, commitMaxBytes, commitMaxInterval,
                encryptWorkers, retry, registry, maxBytes,
                convertMemoryBytes);
    }

    /**
     * A copy with another conversion memory budget.
     *
     * @param bytes heap for conversions running at once; 0 for the default
     *              share of the heap
     * @return the copy
     */
    public PipelineSettings withConvertMemory(long bytes)
    {
        return new PipelineSettings(connectorId, format, split, catalogName,
                kekRef, commitMaxRecords, commitMaxBytes, commitMaxInterval,
                encryptWorkers, retry, converters, convertMaxBytes, bytes);
    }

    /**
     * The conversion memory budget in force.
     *
     * @return {@code convertMemoryBytes}, or the default share of the
     *         maximum heap when it is 0
     */
    public long effectiveConvertMemoryBytes()
    {
        return convertMemoryBytes > 0 ? convertMemoryBytes
                : (long) (Runtime.getRuntime().maxMemory()
                        * DEFAULT_CONVERT_MEMORY_SHARE);
    }
}
