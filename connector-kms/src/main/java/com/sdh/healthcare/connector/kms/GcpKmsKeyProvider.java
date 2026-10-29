package com.sdh.healthcare.connector.kms;

import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.Digest;
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
 *   <li>wrap/unwrap: {@code Encrypt}/{@code Decrypt} on a symmetric key ({@code projects/…/cryptoKeys/<key>}), with
 *       additional authenticated data {@code sdh-batch-data-key}. The wrapped key records the key version that
 *       encrypted it.</li>
 *   <li>sign: {@code AsymmetricSign} on an {@code EC_SIGN_P256_SHA256} key version
 *       ({@code …/cryptoKeyVersions/<n>}); DER → raw.</li>
 * </ul>
 * Every request and response is checked with CRC32C, as Google recommends, to catch corruption in transit.
 * Credentials come from Application Default Credentials.
 */
public final class GcpKmsKeyProvider implements KeyProvider, AutoCloseable {
    private final KeyManagementServiceClient kms;

    public GcpKmsKeyProvider(KeyManagementServiceClient kms) {
        this.kms = kms;
    }

    /** @param endpoint override, e.g. a regional endpoint {@code europe-west1-kms.googleapis.com:443} */
    public static GcpKmsKeyProvider create(Optional<String> endpoint) {
        try {
            KeyManagementServiceSettings.Builder b = KeyManagementServiceSettings.newBuilder();
            endpoint.ifPresent(b::setEndpoint);
            return new GcpKmsKeyProvider(KeyManagementServiceClient.create(b.build()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static long crc32c(byte[] b) {
        CRC32C c = new CRC32C();
        c.update(b);
        return c.getValue();
    }

    private static Int64Value crc(byte[] b) {
        return Int64Value.of(crc32c(b));
    }

    /** {@code …/cryptoKeys/k/cryptoKeyVersions/3} → {@code …/cryptoKeys/k}. Decrypt takes the key, not a version. */
    static String keyName(String ref) {
        int i = ref.indexOf("/cryptoKeyVersions/");
        return i < 0 ? ref : ref.substring(0, i);
    }

    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey) {
        EncryptResponse r;
        try {
            r = kms.encrypt(com.google.cloud.kms.v1.EncryptRequest.newBuilder()
                    .setName(keyName(keyRef))
                    .setPlaintext(ByteString.copyFrom(dataKey)).setPlaintextCrc32C(crc(dataKey))
                    .setAdditionalAuthenticatedData(ByteString.copyFrom(KmsSupport.PURPOSE_BYTES))
                    .setAdditionalAuthenticatedDataCrc32C(crc(KmsSupport.PURPOSE_BYTES))
                    .build());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("Cloud KMS Encrypt", e);
        }
        byte[] ct = r.getCiphertext().toByteArray();
        if (!r.getVerifiedPlaintextCrc32C() || !r.getVerifiedAdditionalAuthenticatedDataCrc32C()
                || r.getCiphertextCrc32C().getValue() != crc32c(ct)) {
            throw new IllegalStateException("Cloud KMS Encrypt failed an integrity check");
        }
        return new WrappedKey(r.getName(), ct);
    }

    @Override
    public byte[] unwrap(WrappedKey wrapped) {
        DecryptResponse r;
        try {
            r = kms.decrypt(com.google.cloud.kms.v1.DecryptRequest.newBuilder()
                    .setName(keyName(wrapped.keyRef()))
                    .setCiphertext(ByteString.copyFrom(wrapped.wrapped())).setCiphertextCrc32C(crc(wrapped.wrapped()))
                    .setAdditionalAuthenticatedData(ByteString.copyFrom(KmsSupport.PURPOSE_BYTES))
                    .setAdditionalAuthenticatedDataCrc32C(crc(KmsSupport.PURPOSE_BYTES))
                    .build());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("Cloud KMS Decrypt", e);
        }
        byte[] plain = r.getPlaintext().toByteArray();
        if (r.getPlaintextCrc32C().getValue() != crc32c(plain)) {
            throw new IllegalStateException("Cloud KMS Decrypt failed an integrity check");
        }
        return plain;
    }

    @Override
    public byte[] sign(String keyRef, byte[] digest) {
        com.google.cloud.kms.v1.AsymmetricSignResponse r;
        try {
            r = kms.asymmetricSign(com.google.cloud.kms.v1.AsymmetricSignRequest.newBuilder()
                    .setName(keyRef)
                    .setDigest(Digest.newBuilder().setSha256(ByteString.copyFrom(digest)))
                    .setDigestCrc32C(crc(digest))
                    .build());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("Cloud KMS AsymmetricSign", e);
        }
        byte[] der = r.getSignature().toByteArray();
        if (!r.getVerifiedDigestCrc32C() || r.getSignatureCrc32C().getValue() != crc32c(der)
                || !r.getName().equals(keyRef)) {
            throw new IllegalStateException("Cloud KMS AsymmetricSign failed an integrity check");
        }
        return KmsSupport.derToRaw(der);
    }

    @Override
    public PublicKey publicKey(String keyRef) {
        try {
            CryptoKeyVersionName.parse(keyRef);
            return KmsSupport.publicKeyFromPem(kms.getPublicKey(keyRef).getPem());
        } catch (RuntimeException e) {
            throw KmsSupport.failed("Cloud KMS GetPublicKey", e);
        }
    }

    @Override
    public void close() {
        kms.close();
    }
}
