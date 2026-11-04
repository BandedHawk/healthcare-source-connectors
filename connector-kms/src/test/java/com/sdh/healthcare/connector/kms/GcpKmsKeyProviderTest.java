/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
import com.google.cloud.kms.v1.PublicKey;
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
 * Cloud KMS has no emulator, so this runs the provider against an
 * in-process fake of the KMS gRPC service: real AES-GCM with the AAD
 * enforced, real ECDSA, and the CRC32C integrity fields. It checks our side
 * of the protocol, not Google's service.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class GcpKmsKeyProviderTest
{

    /** The wrapping key's name. */
    static final String KEY =
            "projects/p/locations/global/keyRings/r/cryptoKeys/kek";

    /** The signing key version's name. */
    static final String SIGN = "projects/p/locations/global/keyRings/r"
            + "/cryptoKeys/sig/cryptoKeyVersions/1";

    /** The fake KMS. */
    final FakeKms fake = new FakeKms();

    /** The in-process server running the fake. */
    Server server;

    /** The provider under test, talking to the fake. */
    GcpKmsKeyProvider provider;

    /**
     * Starts the fake and a provider connected to it.
     *
     * @throws Exception if the server or client can't start
     */
    @BeforeEach
    void start() throws Exception
    {
        String name = "kms-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name).addService(fake).build()
                .start();
        provider = new GcpKmsKeyProvider(KeyManagementServiceClient.create(
                KeyManagementServiceSettings.newBuilder()
                        .setTransportChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(
                                                InProcessChannelBuilder
                                                        .forName(name)
                                                        .usePlaintext()
                                                        .build())))
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build()));
    }

    /**
     * Closes the provider and stops the fake.
     */
    @AfterEach
    void stop()
    {
        provider.close();
        server.shutdownNow();
    }

    /**
     * A data key wraps and unwraps, bound to the purpose, and the wrapped
     * key records the version; tampered ciphertext is refused.
     */
    @Test
    void wrapsAndUnwrapsBoundToThePurposeAndRecordsTheVersion()
    {
        byte[] dataKey = new byte[32];
        new SecureRandom().nextBytes(dataKey);
        WrappedKey wrapped = provider.wrap(KEY, dataKey);
        assertThat(wrapped.keyRef()).isEqualTo(KEY + "/cryptoKeyVersions/3");
        assertThat(provider.unwrap(wrapped)).isEqualTo(dataKey);
        assertThat(fake.lastAad).isEqualTo("sdh-batch-data-key");
        // Tampered ciphertext is refused by the KMS.
        byte[] bad = wrapped.wrapped().clone();
        bad[bad.length - 1] ^= 1;
        assertThatThrownBy(() -> provider.unwrap(
                new WrappedKey(wrapped.keyRef(), bad)))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * A response whose checksum doesn't match is refused.
     */
    @Test
    void corruptedResponsesFailTheIntegrityCheck()
    {
        fake.corruptCrc = true;
        assertThatThrownBy(() -> provider.wrap(KEY, new byte[32]))
                .hasMessageContaining("integrity");
    }

    /**
     * An ES256 JWS signed through the KMS verifies with its public key.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void signsEs256ThatVerifiesWithTheKmsPublicKey() throws Exception
    {
        ECPublicKey publicKey = (ECPublicKey) provider.publicKey(SIGN);
        JWSObject jws = new JWSObject(new JWSHeader(JWSAlgorithm.ES256),
                new Payload("hello"));
        jws.sign(new KmsJwsSigner(provider, SIGN));
        assertThat(jws.verify(new ECDSAVerifier(publicKey))).isTrue();
        assertThat(jws.getSignature().decode()).hasSize(64);
    }

    /**
     * Just enough of Cloud KMS.
     */
    static final class FakeKms
            extends KeyManagementServiceGrpc.KeyManagementServiceImplBase
    {

        /** The symmetric key encrypt and decrypt use. */
        final SecretKey aes;

        /** The signing key pair. */
        final KeyPair ec;

        /** Whether to send a wrong ciphertext checksum. */
        volatile boolean corruptCrc;

        /** The additional authenticated data last encrypted with. */
        volatile String lastAad;

        /**
         * Creates the fake with new keys.
         */
        FakeKms()
        {
            try
            {
                KeyGenerator aesGenerator = KeyGenerator.getInstance("AES");
                aesGenerator.init(256);
                aes = aesGenerator.generateKey();
                KeyPairGenerator ecGenerator =
                        KeyPairGenerator.getInstance("EC");
                ecGenerator.initialize(new ECGenParameterSpec("secp256r1"));
                ec = ecGenerator.generateKeyPair();
            }
            catch (Exception failure)
            {
                throw new IllegalStateException(failure);
            }
        }

        /**
         * The CRC32C checksum of some bytes, as a response field.
         *
         * @param bytes the bytes
         * @return the checksum
         */
        static Int64Value crc(ByteString bytes)
        {
            return Int64Value.of(GcpKmsKeyProvider.crc32c(bytes.toByteArray()));
        }

        /**
         * AES-GCM with the request's AAD; the ciphertext is the nonce then
         * the sealed key.
         *
         * @param request the request
         * @param observer receives the response
         */
        @Override
        public void encrypt(EncryptRequest request,
                StreamObserver<EncryptResponse> observer)
        {
            try
            {
                boolean verified = request.getPlaintextCrc32C().getValue()
                        == crc(request.getPlaintext()).getValue();
                lastAad = request.getAdditionalAuthenticatedData()
                        .toStringUtf8();
                byte[] nonce = new byte[12];
                new SecureRandom().nextBytes(nonce);
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.ENCRYPT_MODE, aes,
                        new GCMParameterSpec(128, nonce));
                cipher.updateAAD(
                        request.getAdditionalAuthenticatedData().toByteArray());
                byte[] ciphertext =
                        cipher.doFinal(request.getPlaintext().toByteArray());
                ByteString blob = ByteString.copyFrom(ByteBuffer
                        .allocate(12 + ciphertext.length).put(nonce)
                        .put(ciphertext).array());
                observer.onNext(EncryptResponse.newBuilder()
                        .setName(request.getName() + "/cryptoKeyVersions/3")
                        .setCiphertext(blob)
                        .setCiphertextCrc32C(Int64Value.of(
                                crc(blob).getValue() + (corruptCrc ? 1 : 0)))
                        .setVerifiedPlaintextCrc32C(verified)
                        .setVerifiedAdditionalAuthenticatedDataCrc32C(true)
                        .build());
                observer.onCompleted();
            }
            catch (Exception failure)
            {
                observer.onError(Status.INTERNAL.asRuntimeException());
            }
        }

        /**
         * Opens what {@link #encrypt} sealed, with the request's AAD.
         *
         * @param request the request
         * @param observer receives the response
         */
        @Override
        public void decrypt(DecryptRequest request,
                StreamObserver<DecryptResponse> observer)
        {
            try
            {
                byte[] blob = request.getCiphertext().toByteArray();
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, aes,
                        new GCMParameterSpec(128, blob, 0, 12));
                cipher.updateAAD(
                        request.getAdditionalAuthenticatedData().toByteArray());
                ByteString plain = ByteString.copyFrom(
                        cipher.doFinal(blob, 12, blob.length - 12));
                observer.onNext(DecryptResponse.newBuilder()
                        .setPlaintext(plain).setPlaintextCrc32C(crc(plain))
                        .build());
                observer.onCompleted();
            }
            catch (Exception failure)
            {
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription("Decryption failed")
                        .asRuntimeException());
            }
        }

        /**
         * ECDSA over the request's digest, DER-encoded as Cloud KMS
         * returns it.
         *
         * @param request the request
         * @param observer receives the response
         */
        @Override
        public void asymmetricSign(AsymmetricSignRequest request,
                StreamObserver<AsymmetricSignResponse> observer)
        {
            try
            {
                Signature signer = Signature.getInstance("NONEwithECDSA");
                signer.initSign(ec.getPrivate());
                signer.update(request.getDigest().getSha256().toByteArray());
                ByteString signature = ByteString.copyFrom(signer.sign());
                observer.onNext(AsymmetricSignResponse.newBuilder()
                        .setName(request.getName()).setSignature(signature)
                        .setSignatureCrc32C(crc(signature))
                        .setVerifiedDigestCrc32C(true).build());
                observer.onCompleted();
            }
            catch (Exception failure)
            {
                observer.onError(Status.INTERNAL.asRuntimeException());
            }
        }

        /**
         * The signing key's public half, as PEM.
         *
         * @param request the request
         * @param observer receives the response
         */
        @Override
        public void getPublicKey(GetPublicKeyRequest request,
                StreamObserver<PublicKey> observer)
        {
            String pem = "-----BEGIN PUBLIC KEY-----\n"
                    + Base64.getMimeEncoder()
                            .encodeToString(ec.getPublic().getEncoded())
                    + "\n-----END PUBLIC KEY-----\n";
            observer.onNext(PublicKey.newBuilder().setName(request.getName())
                    .setPem(pem).build());
            observer.onCompleted();
        }
    }
}
