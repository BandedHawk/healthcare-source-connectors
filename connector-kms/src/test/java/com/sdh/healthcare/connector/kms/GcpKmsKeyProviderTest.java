package com.sdh.healthcare.connector.kms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.kms.v1.AsymmetricSignRequest;
import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.GetPublicKeyRequest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceGrpc;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Cloud KMS has no emulator, so this runs the provider against an in-process fake of the KMS gRPC service: real
 * AES-GCM with the AAD enforced, real ECDSA, and the CRC32C integrity fields. It checks our side of the protocol, not
 * Google's service.
 */
class GcpKmsKeyProviderTest {
    static final String KEY = "projects/p/locations/global/keyRings/r/cryptoKeys/kek";
    static final String SIGN = "projects/p/locations/global/keyRings/r/cryptoKeys/sig/cryptoKeyVersions/1";

    final FakeKms fake = new FakeKms();
    Server server;
    GcpKmsKeyProvider provider;

    @BeforeEach
    void start() throws Exception {
        String name = "kms-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name).addService(fake).build().start();
        provider = new GcpKmsKeyProvider(KeyManagementServiceClient.create(KeyManagementServiceSettings.newBuilder()
                .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(
                        InProcessChannelBuilder.forName(name).usePlaintext().build())))
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build()));
    }

    @AfterEach
    void stop() {
        provider.close();
        server.shutdownNow();
    }

    @Test
    void wrapsAndUnwrapsBoundToThePurposeAndRecordsTheVersion() {
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);
        WrappedKey w = provider.wrap(KEY, dek);
        assertThat(w.keyRef()).isEqualTo(KEY + "/cryptoKeyVersions/3");
        assertThat(provider.unwrap(w)).isEqualTo(dek);
        assertThat(fake.lastAad).isEqualTo("sdh-batch-data-key");
        // Tampered ciphertext is refused by the KMS.
        byte[] bad = w.wrapped().clone();
        bad[bad.length - 1] ^= 1;
        assertThatThrownBy(() -> provider.unwrap(new WrappedKey(w.keyRef(), bad))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void corruptedResponsesFailTheIntegrityCheck() {
        fake.corruptCrc = true;
        assertThatThrownBy(() -> provider.wrap(KEY, new byte[32])).hasMessageContaining("integrity");
    }

    @Test
    void signsEs256ThatVerifiesWithTheKmsPublicKey() throws Exception {
        ECPublicKey pub = (ECPublicKey) provider.publicKey(SIGN);
        JWSObject jws = new JWSObject(new JWSHeader(JWSAlgorithm.ES256), new Payload("hello"));
        jws.sign(new KmsJwsSigner(provider, SIGN));
        assertThat(jws.verify(new ECDSAVerifier(pub))).isTrue();
        assertThat(jws.getSignature().decode()).hasSize(64);
    }

    /** Just enough of Cloud KMS. */
    static final class FakeKms extends KeyManagementServiceGrpc.KeyManagementServiceImplBase {
        final SecretKey aes;
        final KeyPair ec;
        volatile boolean corruptCrc;
        volatile String lastAad;

        FakeKms() {
            try {
                KeyGenerator kg = KeyGenerator.getInstance("AES");
                kg.init(256);
                aes = kg.generateKey();
                KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
                g.initialize(new ECGenParameterSpec("secp256r1"));
                ec = g.generateKeyPair();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        static Int64Value crc(ByteString b) {
            return Int64Value.of(GcpKmsKeyProvider.crc32c(b.toByteArray()));
        }

        @Override
        public void encrypt(EncryptRequest req, StreamObserver<EncryptResponse> out) {
            try {
                boolean ok = req.getPlaintextCrc32C().getValue() == crc(req.getPlaintext()).getValue();
                lastAad = req.getAdditionalAuthenticatedData().toStringUtf8();
                byte[] nonce = new byte[12];
                new SecureRandom().nextBytes(nonce);
                Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
                c.init(Cipher.ENCRYPT_MODE, aes, new GCMParameterSpec(128, nonce));
                c.updateAAD(req.getAdditionalAuthenticatedData().toByteArray());
                byte[] ct = c.doFinal(req.getPlaintext().toByteArray());
                ByteString blob = ByteString.copyFrom(ByteBuffer.allocate(12 + ct.length).put(nonce).put(ct).array());
                out.onNext(EncryptResponse.newBuilder().setName(req.getName() + "/cryptoKeyVersions/3")
                        .setCiphertext(blob)
                        .setCiphertextCrc32C(Int64Value.of(crc(blob).getValue() + (corruptCrc ? 1 : 0)))
                        .setVerifiedPlaintextCrc32C(ok).setVerifiedAdditionalAuthenticatedDataCrc32C(true).build());
                out.onCompleted();
            } catch (Exception e) {
                out.onError(Status.INTERNAL.asRuntimeException());
            }
        }

        @Override
        public void decrypt(DecryptRequest req, StreamObserver<DecryptResponse> out) {
            try {
                byte[] blob = req.getCiphertext().toByteArray();
                Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
                c.init(Cipher.DECRYPT_MODE, aes, new GCMParameterSpec(128, blob, 0, 12));
                c.updateAAD(req.getAdditionalAuthenticatedData().toByteArray());
                ByteString plain = ByteString.copyFrom(c.doFinal(blob, 12, blob.length - 12));
                out.onNext(DecryptResponse.newBuilder().setPlaintext(plain).setPlaintextCrc32C(crc(plain)).build());
                out.onCompleted();
            } catch (Exception e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("Decryption failed").asRuntimeException());
            }
        }

        @Override
        public void asymmetricSign(AsymmetricSignRequest req, StreamObserver<AsymmetricSignResponse> out) {
            try {
                Signature s = Signature.getInstance("NONEwithECDSA");
                s.initSign(ec.getPrivate());
                s.update(req.getDigest().getSha256().toByteArray());
                ByteString sig = ByteString.copyFrom(s.sign());
                out.onNext(AsymmetricSignResponse.newBuilder().setName(req.getName()).setSignature(sig)
                        .setSignatureCrc32C(crc(sig)).setVerifiedDigestCrc32C(true).build());
                out.onCompleted();
            } catch (Exception e) {
                out.onError(Status.INTERNAL.asRuntimeException());
            }
        }

        @Override
        public void getPublicKey(GetPublicKeyRequest req, StreamObserver<com.google.cloud.kms.v1.PublicKey> out) {
            String pem = "-----BEGIN PUBLIC KEY-----\n"
                    + Base64.getMimeEncoder().encodeToString(ec.getPublic().getEncoded()) + "\n-----END PUBLIC KEY-----\n";
            out.onNext(com.google.cloud.kms.v1.PublicKey.newBuilder().setName(req.getName()).setPem(pem).build());
            out.onCompleted();
        }
    }

    static byte[] copy(byte[] b) {
        return Arrays.copyOf(b, b.length);
    }
}
