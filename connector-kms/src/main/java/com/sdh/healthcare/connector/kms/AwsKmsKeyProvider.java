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
 *   <li>wrap/unwrap: {@code Encrypt}/{@code Decrypt} on a symmetric key, with encryption context
 *       {@code sdh:purpose=sdh-batch-data-key}. The wrapped key records the key ARN KMS actually used, so rotation and
 *       alias changes stay traceable in {@code kek_ref}.</li>
 *   <li>sign: {@code Sign} on an {@code ECC_NIST_P256} key, {@code ECDSA_SHA_256} over the digest; DER → raw.</li>
 * </ul>
 * Credentials and region come from the default AWS provider chain unless configured.
 */
public final class AwsKmsKeyProvider implements KeyProvider, AutoCloseable {
    private static final Map<String, String> CONTEXT = Map.of("sdh:purpose", KmsSupport.PURPOSE);

    private final KmsClient kms;

    public AwsKmsKeyProvider(KmsClient kms) {
        this.kms = kms;
    }

    /** @param endpoint override for testing (e.g. local-kms) */
    public static AwsKmsKeyProvider create(Optional<String> region, Optional<URI> endpoint) {
        KmsClientBuilder b = KmsClient.builder().httpClientBuilder(ApacheHttpClient.builder());
        region.map(Region::of).ifPresent(b::region);
        endpoint.ifPresent(b::endpointOverride);
        return new AwsKmsKeyProvider(b.build());
    }

    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey) {
        try {
            EncryptResponse r = kms.encrypt(b -> b.keyId(keyRef).plaintext(SdkBytes.fromByteArray(dataKey))
                    .encryptionContext(CONTEXT));
            return new WrappedKey(r.keyId(), r.ciphertextBlob().asByteArray());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("AWS KMS Encrypt", e);
        }
    }

    @Override
    public byte[] unwrap(WrappedKey wrapped) {
        try {
            return kms.decrypt(b -> b.keyId(wrapped.keyRef())
                    .ciphertextBlob(SdkBytes.fromByteArray(wrapped.wrapped())).encryptionContext(CONTEXT))
                    .plaintext().asByteArray();
        } catch (RuntimeException e) {
            throw KmsSupport.failed("AWS KMS Decrypt", e);
        }
    }

    @Override
    public byte[] sign(String keyRef, byte[] digest) {
        try {
            byte[] der = kms.sign(b -> b.keyId(keyRef).message(SdkBytes.fromByteArray(digest))
                    .messageType(MessageType.DIGEST).signingAlgorithm(SigningAlgorithmSpec.ECDSA_SHA_256))
                    .signature().asByteArray();
            return KmsSupport.derToRaw(der);
        } catch (RuntimeException e) {
            throw KmsSupport.failed("AWS KMS Sign", e);
        }
    }

    @Override
    public PublicKey publicKey(String keyRef) {
        try {
            return KmsSupport.publicKeyFromDer(kms.getPublicKey(b -> b.keyId(keyRef)).publicKey().asByteArray());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("AWS KMS GetPublicKey", e);
        }
    }

    @Override
    public void close() {
        kms.close();
    }
}
