/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The {@code sdh.*} settings both connector applications have (design §14):
 * the lease, keys, queue, retries and telemetry export. Each application's
 * own settings record holds these, under the same names, beside the settings
 * only it has. Secrets (passwords, passphrases) normally come from
 * environment variables or a Spring {@code configtree:} mount.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CommonSettings
{

    /** Not used: this class only holds the settings records. */
    private CommonSettings()
    {
    }

    /**
     * The single-instance lease (design §9.3).
     *
     * @param jdbc          the database the lease is kept in
     * @param name          the lease's name, unique to one connector or
     *                      intake
     * @param ttl           how long the lease lasts without a renewal
     * @param renewInterval how often it is renewed
     * @param safetyMargin  taken off the TTL locally, for clock drift
     */
    public record Lease(@DefaultValue Jdbc jdbc,
            @DefaultValue("file-connector") String name,
            @DefaultValue("60s") Duration ttl,
            @DefaultValue("15s") Duration renewInterval,
            @DefaultValue("10s") Duration safetyMargin)
    {
    }

    /**
     * The lease database.
     *
     * @param url      the JDBC URL
     * @param username the database user
     * @param password the user's password, from the environment
     * @param table    the lease table
     */
    public record Jdbc(String url, String username, String password,
            @DefaultValue("sdh_lease") String table)
    {
    }

    /**
     * Keys and who holds them (design §7.4).
     *
     * @param wrapProvider who wraps batch data keys with the KEKs:
     *                     {@code local}, {@code aws-kms}, {@code gcp-kms},
     *                     {@code azure-keyvault} or {@code vault-transit}
     * @param signProvider who holds the message signing key, with the same
     *                     choices
     * @param keystore     the local keystore, for {@code local}
     * @param keys         which keys to use
     * @param aws          AWS KMS settings
     * @param gcp          Google Cloud KMS settings
     * @param vault        Vault Transit settings
     */
    public record Crypto(@DefaultValue("local") String wrapProvider,
            @DefaultValue("local") String signProvider,
            @DefaultValue Keystore keystore, @DefaultValue Keys keys,
            @DefaultValue Aws aws, @DefaultValue Gcp gcp,
            @DefaultValue Vault vault)
    {
    }

    /**
     * AWS KMS. Credentials come from the default AWS chain.
     *
     * @param region   the region
     * @param endpoint an endpoint override, such as a VPC endpoint
     */
    public record Aws(String region, URI endpoint)
    {
    }

    /**
     * Google Cloud KMS. Credentials come from Application Default
     * Credentials.
     *
     * @param endpoint an endpoint override, such as a regional endpoint
     */
    public record Gcp(String endpoint)
    {
    }

    /**
     * Vault (or OpenBao) Transit.
     *
     * @param address   the server's address
     * @param token     a token, from the environment; or
     * @param tokenFile a file holding one, read again for every request
     *                  (as Vault Agent writes it)
     * @param mount     where the Transit engine is mounted
     * @param namespace the Vault namespace, if any
     */
    public record Vault(URI address, String token, Path tokenFile,
            @DefaultValue("transit") String mount, String namespace)
    {
    }

    /**
     * The PKCS#12 keystore holding the AES-256 KEKs, for {@code local}. Its
     * entries' aliases are the KEK references.
     *
     * @param path     the keystore file
     * @param password its password, from the environment
     */
    public record Keystore(Path path, String password)
    {
    }

    /**
     * Which keys to use. Each application requires the KEKs of its own
     * tables only.
     *
     * @param sectionsKek    the KEK of the {@code sections} table
     * @param sourceFilesKek the KEK of {@code source_files}
     * @param failedFilesKek the KEK of {@code failed_files}
     * @param landedItemsKek the KEK of {@code landed_items} (design §5.3)
     * @param signing        the message signing key
     * @param recipient      the public key messages are encrypted to
     * @param recoveryPublic the public key of the local fallback
     *                       (design §11.3); the file connector only
     */
    public record Keys(String sectionsKek, String sourceFilesKek,
            String failedFilesKek, String landedItemsKek,
            @DefaultValue Signing signing, @DefaultValue Recipient recipient,
            Path recoveryPublic)
    {
    }

    /**
     * The ES256 signing key: for {@code local}, an encrypted PKCS#8 PEM; for
     * a KMS, the key's reference (an AWS key ID, ARN or alias, a GCP key
     * version name, an Azure key ID, or a Vault key name).
     *
     * @param kid        the key ID put in message headers
     * @param pem        the PEM file, for {@code local}
     * @param passphrase its passphrase, from the environment
     * @param keyRef     the key's reference, for a KMS
     */
    public record Signing(@DefaultValue("connector") String kid, Path pem,
            String passphrase, String keyRef)
    {
    }

    /**
     * The recipient's public key, pinned by the SHA-256 of its
     * SubjectPublicKeyInfo.
     *
     * @param kid    the key ID put in message headers
     * @param pem    the PEM file
     * @param sha256 the pinned fingerprint, in hex
     */
    public record Recipient(String kid, Path pem, String sha256)
    {
    }

    /**
     * The queue messages are published to (design §10.3).
     *
     * @param type    {@code kafka}, {@code sqs}, {@code pubsub},
     *                {@code servicebus}, {@code eventhubs},
     *                {@code artemis}, {@code rabbitmq}, or {@code camel}
     *                (any endpoint, with no broker-specific headers)
     * @param uri     the full Camel endpoint URI
     * @param artemis the Artemis connection, for {@code artemis}
     */
    public record Queue(String type, String uri,
            @DefaultValue Artemis artemis)
    {
    }

    /**
     * An Artemis broker connection.
     *
     * @param brokerUrl the broker's URL
     * @param username  the user
     * @param password  the user's password, from the environment
     */
    public record Artemis(String brokerUrl, String username, String password)
    {
    }

    /**
     * OpenTelemetry export (design §12). With no endpoint, nothing is
     * exported, and audit events still go to the log.
     *
     * @param otlp           the collector
     * @param metricInterval how often metrics are exported
     */
    public record Telemetry(@DefaultValue Otlp otlp,
            @DefaultValue("60s") Duration metricInterval)
    {
    }

    /**
     * An OTLP/HTTP collector.
     *
     * @param endpoint its base URL, such as
     *                 {@code https://otel-collector:4318};
     *                 {@code /v1/traces}, {@code /v1/metrics} and
     *                 {@code /v1/logs} are appended
     * @param headers  extra request headers, such as an authorization
     *                 token from the environment
     * @param timeout  how long an export may take
     */
    public record Otlp(String endpoint, Map<String, String> headers,
            @DefaultValue("10s") Duration timeout)
    {
    }

    /**
     * How transient failures are retried.
     *
     * @param maxAttempts    how many attempts in all
     * @param initialBackoff the wait after the first failure
     * @param maxBackoff     the longest wait
     */
    public record Retry(@DefaultValue("8") int maxAttempts,
            @DefaultValue("200ms") Duration initialBackoff,
            @DefaultValue("30s") Duration maxBackoff)
    {
    }
}
