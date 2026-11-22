/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.SignedJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link TestIssuer#signingWith}: an issuer whose key is kept in
 * files, as the sandbox keeps it, signs tokens its key set verifies, under
 * that key set's key ID, and serves nothing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class TestIssuerTest
{

    /** Where the key files go. */
    @TempDir
    Path dir;

    /** Creates the test. */
    TestIssuerTest()
    {
    }

    /**
     * A kept key signs tokens its key set verifies.
     *
     * @throws Exception if the key can't be made
     */
    @Test
    void signsWithTheKeptKey() throws Exception
    {
        ECKey key = new ECKeyGenerator(Curve.P_256).keyID("kept").generate();
        Path pem = dir.resolve("issuer.pem");
        Files.writeString(pem, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(key.toECPrivateKey()
                        .getEncoded())
                + "\n-----END PRIVATE KEY-----\n", UTF_8);
        Path jwks = dir.resolve("jwks.json");
        Files.writeString(jwks, new JWKSet(key.toPublicJWK()).toString(),
                UTF_8);

        TestIssuer issuer = TestIssuer.signingWith(pem, jwks);
        SignedJWT token = SignedJWT.parse(issuer.token("alice",
                List.of("pipeline.view")));
        assertThat(token.getHeader().getKeyID()).isEqualTo("kept");
        assertThat(token.verify(new ECDSAVerifier(key.toECPublicKey())))
                .isTrue();
        assertThat(token.getJWTClaimsSet().getSubject()).isEqualTo("alice");
        assertThatThrownBy(issuer::jwksUri)
                .isInstanceOf(IllegalStateException.class);
        issuer.close();
    }

    /**
     * A file that holds no key is refused.
     *
     * @throws Exception if the files can't be written
     */
    @Test
    void refusesNoKey() throws Exception
    {
        Path pem = Files.writeString(dir.resolve("issuer.pem"),
                "-----BEGIN PRIVATE KEY-----\nAAAA\n"
                        + "-----END PRIVATE KEY-----\n", UTF_8);
        Path jwks = Files.writeString(dir.resolve("jwks.json"),
                "{\"keys\":[]}", UTF_8);
        assertThatThrownBy(() -> TestIssuer.signingWith(pem, jwks))
                .isInstanceOf(java.io.IOException.class);
    }
}
