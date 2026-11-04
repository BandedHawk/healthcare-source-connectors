/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.google.cloud.kms.v1.AsymmetricSignRequest;
import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.Digest;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PublicKey;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Google Cloud KMS (design §7.4).
 *
 * <ul>
 *   <li>wrap/unwrap: {@code Encrypt}/{@code Decrypt} on a symmetric key
 *       ({@code projects/…/cryptoKeys/<key>}), with additional
 *       authenticated data {@code sdh-batch-data-key}. The wrapped key
 *       records the key version that encrypted it.</li>
 *   <li>sign: {@code AsymmetricSign} on an {@code EC_SIGN_P256_SHA256} key
 *       version ({@code …/cryptoKeyVersions/<n>}); DER → raw.</li>
 * </ul>
 * Every request and response is checked with CRC32C, as Google recommends,
 * to catch corruption in transit. Credentials come from Application
 * Default Credentials.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class GcpKmsKeyProvider implements KeyProvider, AutoCloseable
{

    /** The Cloud KMS client. */
    private final KeyManagementServiceClient kms;

    /**
     * Creates a provider on a client.
     *
     * @param kms the Cloud KMS client, closed with the provider
     */
    public GcpKmsKeyProvider(KeyManagementServiceClient kms)
    {
        this.kms = kms;
    }

    /**
     * Creates a provider with Application Default Credentials.
     *
     * @param endpoint override, e.g. a regional endpoint
     *        {@code europe-west1-kms.googleapis.com:443}
     * @return the provider
     * @throws UncheckedIOException if the client can't be created
     */
    public static GcpKmsKeyProvider create(Optional<String> endpoint)
    {
        try
        {
            KeyManagementServiceSettings.Builder settings =
                    KeyManagementServiceSettings.newBuilder();
            endpoint.ifPresent(settings::setEndpoint);
            return new GcpKmsKeyProvider(
                    KeyManagementServiceClient.create(settings.build()));
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }

    /**
     * The CRC32C checksum of some bytes.
     *
     * @param bytes the bytes
     * @return the checksum
     */
    static long crc32c(byte[] bytes)
    {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes);
        return checksum.getValue();
    }

    /**
     * The CRC32C checksum of some bytes, as a request field.
     *
     * @param bytes the bytes
     * @return the checksum
     */
    private static Int64Value crc(byte[] bytes)
    {
        return Int64Value.of(crc32c(bytes));
    }

    /**
     * {@code …/cryptoKeys/k/cryptoKeyVersions/3} → {@code …/cryptoKeys/k}.
     * Decrypt takes the key, not a version.
     *
     * @param ref a key or key version name
     * @return the key name
     */
    static String keyName(String ref)
    {
        int versions = ref.indexOf("/cryptoKeyVersions/");
        return versions < 0 ? ref : ref.substring(0, versions);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Cloud KMS fails, or the response
     *         fails an integrity check
     */
    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        EncryptResponse response;
        try
        {
            response = kms.encrypt(EncryptRequest.newBuilder()
                    .setName(keyName(keyRef))
                    .setPlaintext(ByteString.copyFrom(dataKey))
                    .setPlaintextCrc32C(crc(dataKey))
                    .setAdditionalAuthenticatedData(
                            ByteString.copyFrom(KmsSupport.PURPOSE_BYTES))
                    .setAdditionalAuthenticatedDataCrc32C(
                            crc(KmsSupport.PURPOSE_BYTES))
                    .build());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Cloud KMS Encrypt", failure);
        }
        byte[] ciphertext = response.getCiphertext().toByteArray();
        if (!response.getVerifiedPlaintextCrc32C()
                || !response.getVerifiedAdditionalAuthenticatedDataCrc32C()
                || response.getCiphertextCrc32C().getValue()
                        != crc32c(ciphertext))
        {
            throw new IllegalStateException(
                    "Cloud KMS Encrypt failed an integrity check");
        }
        return new WrappedKey(response.getName(), ciphertext);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Cloud KMS fails, or the response
     *         fails an integrity check
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        DecryptResponse response;
        try
        {
            response = kms.decrypt(DecryptRequest.newBuilder()
                    .setName(keyName(wrapped.keyRef()))
                    .setCiphertext(ByteString.copyFrom(wrapped.wrapped()))
                    .setCiphertextCrc32C(crc(wrapped.wrapped()))
                    .setAdditionalAuthenticatedData(
                            ByteString.copyFrom(KmsSupport.PURPOSE_BYTES))
                    .setAdditionalAuthenticatedDataCrc32C(
                            crc(KmsSupport.PURPOSE_BYTES))
                    .build());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Cloud KMS Decrypt", failure);
        }
        byte[] plain = response.getPlaintext().toByteArray();
        if (response.getPlaintextCrc32C().getValue() != crc32c(plain))
        {
            throw new IllegalStateException(
                    "Cloud KMS Decrypt failed an integrity check");
        }
        return plain;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Cloud KMS fails, or the response
     *         fails an integrity check
     */
    @Override
    public byte[] sign(String keyRef, byte[] digest)
    {
        AsymmetricSignResponse response;
        try
        {
            response = kms.asymmetricSign(AsymmetricSignRequest.newBuilder()
                    .setName(keyRef)
                    .setDigest(Digest.newBuilder()
                            .setSha256(ByteString.copyFrom(digest)))
                    .setDigestCrc32C(crc(digest))
                    .build());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Cloud KMS AsymmetricSign", failure);
        }
        byte[] der = response.getSignature().toByteArray();
        if (!response.getVerifiedDigestCrc32C()
                || response.getSignatureCrc32C().getValue() != crc32c(der)
                || !response.getName().equals(keyRef))
        {
            throw new IllegalStateException(
                    "Cloud KMS AsymmetricSign failed an integrity check");
        }
        return KmsSupport.derToRaw(der);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the ref isn't a key version name, or
     *         Cloud KMS fails
     */
    @Override
    public PublicKey publicKey(String keyRef)
    {
        try
        {
            CryptoKeyVersionName.parse(keyRef);
            return KmsSupport.publicKeyFromPem(
                    kms.getPublicKey(keyRef).getPem());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Cloud KMS GetPublicKey", failure);
        }
    }

    /**
     * Closes the Cloud KMS client.
     */
    @Override
    public void close()
    {
        kms.close();
    }
}
