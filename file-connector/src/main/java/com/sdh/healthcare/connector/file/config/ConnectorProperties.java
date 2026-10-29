package com.sdh.healthcare.connector.file.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting (design §14). Secrets (passwords, passphrases) are normally supplied through
 * environment variables, e.g. {@code SDH_CRYPTO_KEYSTORE_PASSWORD}, or a Spring {@code configtree:} mount.
 */
@ConfigurationProperties("sdh")
public record ConnectorProperties(
        @DefaultValue("file-connector") String connectorId,
        @DefaultValue File file,
        @DefaultValue Split split,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue Queue queue,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry,
        /** Hash/encrypt/seal threads; 0 = one per CPU core. */
        @DefaultValue("0") int encryptWorkers) {

    /**
     * @param readLock {@code changed} (size and mtime unchanged for {@code minAge}) or {@code marker}
     *                 (wait for {@code <name>.done})
     * @param include  Ant-style pattern of files to pick up
     */
    public record File(Path inboundDir, @DefaultValue("changed") String readLock, @DefaultValue("30s") Duration minAge,
            @DefaultValue("5s") Duration pollInterval, @DefaultValue("*") String include) {}

    /** @param options format options as in design §6.2, e.g. {@code csv.header}, {@code namespace} */
    /**
     * @param convert          convert sections to JSON where the format has a converter (design §6.4): csv, ccda, x12,
     *                         edifact. FHIR, generic XML and delimited sections are always stored as split.
     * @param convertMaxBytes  larger sections aren't converted and are stored in their original form as failed
     */
    public record Split(String format, String boundary, Map<String, String> options, @DefaultValue Limits limits,
            @DefaultValue("true") boolean convert, @DefaultValue("64MB") DataSize convertMaxBytes) {}

    /** Null values keep the format's defaults (design §6.3). */
    public record Limits(DataSize maxFileBytes, DataSize maxSectionBytes, Long maxSectionsPerFile) {}

    /**
     * @param catalog     Iceberg catalog properties, passed through as-is: {@code type}, {@code uri},
     *                    {@code warehouse}, {@code io-impl}, {@code s3.endpoint}, …
     * @param catalogName name of the catalog, also put in messages for readers to check
     */
    public record Storage(Map<String, String> catalog, @DefaultValue("sdh") String catalogName,
            @DefaultValue("sdh_ingest") String namespace, @DefaultValue Commit commit,
            @DefaultValue Archive archive) {}

    public record Commit(@DefaultValue("10000") int maxSections, @DefaultValue("512MB") DataSize maxBytes,
            @DefaultValue("30s") Duration maxInterval) {}

    public record Archive(@DefaultValue("512MB") DataSize commitBytes) {}

    public record Lease(@DefaultValue Jdbc jdbc, @DefaultValue("file-connector") String name,
            @DefaultValue("60s") Duration ttl, @DefaultValue("15s") Duration renewInterval,
            @DefaultValue("10s") Duration safetyMargin) {}

    public record Jdbc(String url, String username, String password, @DefaultValue("sdh_lease") String table) {}

    /**
     * @param wrapProvider who wraps batch data keys with the KEKs: {@code local}, {@code aws-kms}, {@code gcp-kms},
     *                     {@code azure-keyvault} or {@code vault-transit}
     * @param signProvider who holds the message signing key, same choices
     */
    public record Crypto(@DefaultValue("local") String wrapProvider, @DefaultValue("local") String signProvider,
            @DefaultValue Keystore keystore, @DefaultValue Keys keys, @DefaultValue Aws aws, @DefaultValue Gcp gcp,
            @DefaultValue Vault vault) {}

    /** AWS KMS. Credentials from the default AWS chain. @param endpoint override, e.g. a VPC endpoint */
    public record Aws(String region, java.net.URI endpoint) {}

    /** Google Cloud KMS. Credentials from Application Default Credentials. @param endpoint e.g. a regional endpoint */
    public record Gcp(String endpoint) {}

    /**
     * Vault (or OpenBao) Transit.
     *
     * @param token     a token (from the environment), or
     * @param tokenFile a file holding one, re-read on every request (e.g. written by Vault Agent)
     */
    public record Vault(java.net.URI address, String token, Path tokenFile, @DefaultValue("transit") String mount,
            String namespace) {}

    /** PKCS#12 keystore holding the AES-256 KEKs; entry aliases are the KEK refs. */
    public record Keystore(Path path, String password) {}

    public record Keys(String sectionsKek, String sourceFilesKek, String failedFilesKek, @DefaultValue Signing signing,
            @DefaultValue Recipient recipient, Path recoveryPublic) {}

    /**
     * The ES256 signing key: for {@code local}, an encrypted PKCS#8 PEM ({@code pem}, {@code passphrase}); for a KMS,
     * the key's reference ({@code keyRef}: AWS key ID/ARN/alias, GCP key version name, Azure key ID, Vault key name).
     */
    public record Signing(@DefaultValue("connector") String kid, Path pem, String passphrase, String keyRef) {}

    /** Recipient public key PEM, pinned by the SHA-256 of its SubjectPublicKeyInfo (hex). */
    public record Recipient(String kid, Path pem, String sha256) {}

    /**
     * @param type {@code kafka}, {@code sqs}, {@code pubsub}, {@code servicebus}, {@code eventhubs}, {@code artemis},
     *             {@code rabbitmq}, or {@code camel} (any endpoint, no broker-specific headers)
     * @param uri  full Camel endpoint URI (design §10.3)
     */
    public record Queue(String type, String uri, @DefaultValue Artemis artemis) {}

    public record Artemis(String brokerUrl, String username, String password) {}

    /**
     * Table maintenance (design §8.4), run between files on the single writer.
     *
     * @param interval how often to try; a run is skipped while a file is being processed
     */
    public record Maintenance(@DefaultValue("true") boolean enabled, @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention, @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction, @DefaultValue Orphans orphans, @DefaultValue Retention retention) {}

    public record Compaction(@DefaultValue("true") boolean enabled, @DefaultValue("0.75") double smallFileFraction,
            @DefaultValue("5") int minInputFiles, @DefaultValue("8GB") DataSize maxBytesPerRun) {}

    /** @param minAge never delete an unreferenced file younger than this (at least 1 hour) */
    public record Orphans(@DefaultValue("true") boolean enabled, @DefaultValue("24h") Duration interval,
            @DefaultValue("3d") Duration minAge) {}

    /** How long to keep each table's rows, in whole days; unset = forever (the default). */
    public record Retention(Duration sections, Duration sourceFiles, Duration failedFiles) {}

    /**
     * OpenTelemetry export (design §12). With no endpoint, nothing is exported (audit events still go to the log).
     *
     * @param metricInterval how often metrics are exported
     */
    public record Telemetry(@DefaultValue Otlp otlp, @DefaultValue("60s") Duration metricInterval) {}

    /**
     * @param endpoint base URL of an OTLP/HTTP collector, e.g. {@code https://otel-collector:4318}; {@code /v1/traces},
     *                 {@code /v1/metrics} and {@code /v1/logs} are appended
     * @param headers  extra request headers, e.g. an authorization token (supply secrets from the environment)
     */
    public record Otlp(String endpoint, Map<String, String> headers, @DefaultValue("10s") Duration timeout) {}

    public record Retry(@DefaultValue("8") int maxAttempts, @DefaultValue("200ms") Duration initialBackoff,
            @DefaultValue("30s") Duration maxBackoff) {}
}
