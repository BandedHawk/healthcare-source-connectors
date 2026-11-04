/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.net.URI;
import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.MessageType;
import software.amazon.awssdk.services.kms.model.SigningAlgorithmSpec;

/**
 * AWS KMS (design §7.4). Key refs are key IDs, ARNs or aliases.
 *
 * <ul>
 *   <li>wrap/unwrap: {@code Encrypt}/{@code Decrypt} on a symmetric key,
 *       with encryption context {@code sdh:purpose=sdh-batch-data-key}. The
 *       wrapped key records the key ARN KMS actually used, so rotation and
 *       alias changes stay traceable in {@code kek_ref}.</li>
 *   <li>sign: {@code Sign} on an {@code ECC_NIST_P256} key,
 *       {@code ECDSA_SHA_256} over the digest; DER → raw.</li>
 * </ul>
 * Credentials and region come from the default AWS provider chain unless
 * configured.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AwsKmsKeyProvider implements KeyProvider, AutoCloseable
{

    /** The encryption context every data key is bound to. */
    private static final Map<String, String> CONTEXT =
            Map.of("sdh:purpose", KmsSupport.PURPOSE);

    /** The KMS client. */
    private final KmsClient kms;

    /**
     * Creates a provider on a client.
     *
     * @param kms the KMS client, closed with the provider
     */
    public AwsKmsKeyProvider(KmsClient kms)
    {
        this.kms = kms;
    }

    /**
     * Creates a provider with the default credentials.
     *
     * @param region the region, if not the default chain's
     * @param endpoint override for testing (e.g. local-kms)
     * @return the provider
     */
    public static AwsKmsKeyProvider create(Optional<String> region,
            Optional<URI> endpoint)
    {
        KmsClientBuilder builder = KmsClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder());
        region.map(Region::of).ifPresent(builder::region);
        endpoint.ifPresent(builder::endpointOverride);
        return new AwsKmsKeyProvider(builder.build());
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if KMS fails
     */
    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        try
        {
            EncryptResponse response = kms.encrypt(request -> request
                    .keyId(keyRef).plaintext(SdkBytes.fromByteArray(dataKey))
                    .encryptionContext(CONTEXT));
            return new WrappedKey(response.keyId(),
                    response.ciphertextBlob().asByteArray());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("AWS KMS Encrypt", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if KMS fails
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        try
        {
            return kms.decrypt(request -> request.keyId(wrapped.keyRef())
                            .ciphertextBlob(
                                    SdkBytes.fromByteArray(wrapped.wrapped()))
                            .encryptionContext(CONTEXT))
                    .plaintext().asByteArray();
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("AWS KMS Decrypt", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if KMS fails
     */
    @Override
    public byte[] sign(String keyRef, byte[] digest)
    {
        try
        {
            byte[] der = kms.sign(request -> request.keyId(keyRef)
                            .message(SdkBytes.fromByteArray(digest))
                            .messageType(MessageType.DIGEST)
                            .signingAlgorithm(
                                    SigningAlgorithmSpec.ECDSA_SHA_256))
                    .signature().asByteArray();
            return KmsSupport.derToRaw(der);
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("AWS KMS Sign", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if KMS fails
     */
    @Override
    public PublicKey publicKey(String keyRef)
    {
        try
        {
            return KmsSupport.publicKeyFromDer(kms
                    .getPublicKey(request -> request.keyId(keyRef))
                    .publicKey().asByteArray());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("AWS KMS GetPublicKey", failure);
        }
    }

    /**
     * Closes the KMS client.
     */
    @Override
    public void close()
    {
        kms.close();
    }
}
