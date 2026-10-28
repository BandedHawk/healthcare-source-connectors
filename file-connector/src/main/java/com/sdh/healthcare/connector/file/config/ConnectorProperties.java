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
    public record Split(String format, String boundary, Map<String, String> options, @DefaultValue Limits limits) {}

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

    public record Crypto(@DefaultValue("local") String wrapProvider, @DefaultValue("local") String signProvider,
            @DefaultValue Keystore keystore, @DefaultValue Keys keys) {}

    /** PKCS#12 keystore holding the AES-256 KEKs; entry aliases are the KEK refs. */
    public record Keystore(Path path, String password) {}

    public record Keys(String sectionsKek, String sourceFilesKek, String failedFilesKek, @DefaultValue Signing signing,
            @DefaultValue Recipient recipient, Path recoveryPublic) {}

    /** EC P-256 private key as an encrypted PKCS#8 PEM. */
    public record Signing(@DefaultValue("connector") String kid, Path pem, String passphrase) {}

    /** Recipient public key PEM, pinned by the SHA-256 of its SubjectPublicKeyInfo (hex). */
    public record Recipient(String kid, Path pem, String sha256) {}

    /**
     * @param type {@code kafka}, {@code sqs}, {@code pubsub}, {@code servicebus}, {@code eventhubs}, {@code artemis},
     *             {@code rabbitmq}, or {@code camel} (any endpoint, no broker-specific headers)
     * @param uri  full Camel endpoint URI (design §10.3)
     */
    public record Queue(String type, String uri, @DefaultValue Artemis artemis) {}

    public record Artemis(String brokerUrl, String username, String password) {}

    public record Retry(@DefaultValue("8") int maxAttempts, @DefaultValue("200ms") Duration initialBackoff,
            @DefaultValue("30s") Duration maxBackoff) {}
}
