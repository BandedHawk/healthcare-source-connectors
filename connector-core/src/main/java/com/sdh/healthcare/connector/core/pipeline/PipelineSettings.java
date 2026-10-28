package com.sdh.healthcare.connector.core.pipeline;

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
        RetryPolicy retry) {

    public PipelineSettings {
        Objects.requireNonNull(connectorId);
        Objects.requireNonNull(format);
        Objects.requireNonNull(split);
        Objects.requireNonNull(catalogName);
        Objects.requireNonNull(kekRef);
        Objects.requireNonNull(commitMaxInterval);
        Objects.requireNonNull(retry);
        if (commitMaxSections < 1 || commitMaxBytes < 1 || encryptWorkers < 1) {
            throw new IllegalArgumentException("Commit thresholds and worker count must be positive");
        }
    }

    public static PipelineSettings defaults(String connectorId, String format, String catalogName, String kekRef) {
        return new PipelineSettings(connectorId, format, SplitConfig.defaults(format), catalogName, kekRef, 10_000,
                512L << 20, Duration.ofSeconds(30), Runtime.getRuntime().availableProcessors(), RetryPolicy.DEFAULT);
    }

    public PipelineSettings withSplit(SplitConfig s) {
        return new PipelineSettings(connectorId, format, s, catalogName, kekRef, commitMaxSections, commitMaxBytes,
                commitMaxInterval, encryptWorkers, retry);
    }

    public PipelineSettings withCommit(int maxSections, long maxBytes, Duration maxInterval) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, maxSections, maxBytes,
                maxInterval, encryptWorkers, retry);
    }

    public PipelineSettings withWorkers(int n) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, commitMaxSections,
                commitMaxBytes, commitMaxInterval, n, retry);
    }

    public PipelineSettings withRetry(RetryPolicy r) {
        return new PipelineSettings(connectorId, format, split, catalogName, kekRef, commitMaxSections,
                commitMaxBytes, commitMaxInterval, encryptWorkers, r);
    }
}
