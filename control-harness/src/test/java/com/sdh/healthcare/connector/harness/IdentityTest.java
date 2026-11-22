/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.SignedJWT;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link TestIssuer} and {@link Operator}: tokens a stage can
 * verify from the served key set, and an operator's sealing key named
 * as {@code sealTo} names it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IdentityTest
{

    /** Creates the test. */
    IdentityTest()
    {
    }

    /**
     * A token verifies with the served key and carries its claims.
     *
     * @throws Exception if the key set can't be read
     */
    @Test
    void tokenVerifiesFromTheKeySet() throws Exception
    {
        try (TestIssuer issuer = new TestIssuer())
        {
            SignedJWT token = SignedJWT.parse(issuer.token("vic-viewer",
                    List.of("view")));
            JWKSet keys = JWKSet.load(URI.create(issuer.jwksUri()).toURL());
            ECKey key = (ECKey) keys.getKeyByKeyId(token.getHeader()
                    .getKeyID());

            assertThat(token.verify(new ECDSAVerifier(key))).isTrue();
            assertThat(key.isPrivate()).isFalse();
            assertThat(token.getJWTClaimsSet().getSubject())
                    .isEqualTo("vic-viewer");
            assertThat(token.getJWTClaimsSet().getAudience())
                    .containsExactly(TestIssuer.AUDIENCE);
            assertThat(token.getJWTClaimsSet().getStringListClaim("groups"))
                    .containsExactly("view");
        }
    }

    /** A fingerprint is lower-case hex SHA-256; the PEM a public key. */
    @Test
    void operatorNamesItsKey()
    {
        Operator operator = Operator.of("erin-exporter", "view");

        assertThat(operator.fingerprint()).matches("[0-9a-f]{64}");
        assertThat(operator.publicPem()).startsWith(
                "-----BEGIN PUBLIC KEY-----");
        assertThat(operator.toString()).doesNotContain("PRIVATE");
        assertThat(Operator.of("erin-exporter", "view").fingerprint())
                .isNotEqualTo(operator.fingerprint());
    }
}
