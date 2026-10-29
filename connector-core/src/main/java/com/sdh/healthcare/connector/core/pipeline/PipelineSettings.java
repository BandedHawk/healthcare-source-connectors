package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * Settings for {@link IngestPipeline} (design §8.3, §13, §14).
 *
 * @param connectorId       names the checkpoint property, e.g. {@code file-connector}
 * @param format            split format, e.g. {@code ccda}
 * @param catalogName       catalog name put in messages, so readers can check they look in the right place
 * @param kekRef            KEK for batch data keys of the sections table
 * @param commitMaxSections commit after this many sections (default 10,000)
 * @param commitMaxBytes    … or this many plaintext bytes (default 512 MiB)
 * @param commitMaxInterval … or when a batch has been open this long (default 30 s)
 * @param encryptWorkers    threads for hashing, compressing, encrypting and sealing (default: CPU cores)
 * @param converters        JSON converters by format (design §6.4); {@link ConverterRegistry#none()} stores every
 *                          section as split
 * @param convertMaxBytes   sections larger than this aren't converted (they'd need too much memory) and are stored
 *                          in their original form as failed, {@code CONVERT_TOO_LARGE} (default 64 MiB)
 */
public record PipelineSettings(
        String connectorId,
        String format,
        SplitConfig split,
        String catalogName,
        String kekRef,
        int commitMaxSections,
        long commitMaxBytes,
        Duration commitMaxInterval,
        int encryptWorkers,
        RetryPolicy retry,
        ConverterRegistry converters,
        long convertMaxBytes) {

    public PipelineSettings {
        Objects.requireNonNull(connectorId);
        Objects.requireNonNull(format);
        Objects.requireNonNull(split);
        Objects.requireNonNull(catalogName);
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(commitMaxInterval);
        Objects.requireNonNull(retry);
        Objects.requireNonNull(converters);
        if (commitMaxSections < 1 || commitMaxBytes < 1 || encryptWorkers < 1 || convertMaxBytes < 1) {
            throw new IllegalArgumentException("Commit thresholds and worker count must be positive");
        }
    }

    public static PipelineSettings defaults(String connectorId, String format, String catalogName, String kekRef) {
        return new PipelineSettings(connectorId, format, SplitConfig.defaults(format), catalogName, kekRef, 10_000,
                512L << 20, Duration.ofSeconds(30), Runtime.getRuntime().availableProcessors(), RetryPolicy.DEFAULT,
                ConverterRegistry.load(), 64L << 20);
    }

    public PipelineSettings withSplit(SplitConfig s) {
        return new PipelineSettings(connectorId, format, s, catalogName, kekRef, commitMaxSections, commitMaxBytes,
                commitMaxInterval, encryptWorkers, retry, converters, convertMaxBytes);
    }

    public PipelineSettings withCommit(int maxSections, long maxBytes, Duration maxInterval) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, maxSections, maxBytes,
                maxInterval, encryptWorkers, retry, converters, convertMaxBytes);
    }

    public PipelineSettings withWorkers(int n) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, commitMaxSections,
                commitMaxBytes, commitMaxInterval, n, retry, converters, convertMaxBytes);
    }

    public PipelineSettings withRetry(RetryPolicy r) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, commitMaxSections,
                commitMaxBytes, commitMaxInterval, encryptWorkers, r, converters, convertMaxBytes);
    }

    public PipelineSettings withConversion(ConverterRegistry c, long maxBytes) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, commitMaxSections,
                commitMaxBytes, commitMaxInterval, encryptWorkers, retry, c, maxBytes);
    }
}
