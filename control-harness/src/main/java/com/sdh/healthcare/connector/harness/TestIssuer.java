/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Signs short-lived tokens for test principals, with the roles a scenario
 * gives them, and serves its key set (CONTROL-API-DESIGN.md §14). For
 * tests only: never part of a customer deployment, and the reference
 * settings never name it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestIssuer implements AutoCloseable
{

    /** The tokens' issuer. */
    public static final String ISSUER = "https://sdh-test-issuer.invalid";

    /** The tokens' audience: the control API. */
    public static final String AUDIENCE = "sdh-control";

    /** How long a token is good for. */
    static final Duration LIFETIME = Duration.ofMinutes(5);

    /** The signing key. */
    private final ECKey key;

    /** Serves the key set. */
    private final HttpServer server;

    /**
     * Creates a key and serves its public half at {@link #jwksUri()}.
     *
     * @throws IOException if the server can't start
     */
    public TestIssuer() throws IOException
    {
        try
        {
            key = new ECKeyGenerator(Curve.P_256)
                    .keyID("sdh-test-issuer-" + UUID.randomUUID())
                    .generate();
        }
        catch (JOSEException failure)
        {
            throw new IOException("No key for the test issuer", failure);
        }
        byte[] jwks = new JWKSet(key.toPublicJWK()).toString().getBytes(UTF_8);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/jwks", exchange ->
        {
            exchange.getResponseHeaders().add("Content-Type",
                    "application/json");
            exchange.sendResponseHeaders(200, jwks.length);
            exchange.getResponseBody().write(jwks);
            exchange.close();
        });
        server.start();
    }

    /**
     * Where the key set is served, for {@code sdh.control.auth.jwt
     * .jwk-set-uri}.
     *
     * @return the URI
     */
    public String jwksUri()
    {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/jwks";
    }

    /**
     * A token for a principal, good for five minutes.
     *
     * @param principal the principal, as {@code sub}
     * @param roles     its roles, as {@code groups}
     * @return the token
     */
    public String token(String principal, List<String> roles)
    {
        Instant now = Instant.now();
        return token(principal, roles, now, now.plus(LIFETIME));
    }

    /**
     * A token for a principal, with the times given.
     *
     * @param principal the principal
     * @param roles     its roles
     * @param issued    when it was issued
     * @param expires   when it expires
     * @return the token
     */
    public String token(String principal, List<String> roles, Instant issued,
            Instant expires)
    {
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256)
                .keyID(key.getKeyID()).build(),
                new JWTClaimsSet.Builder().issuer(ISSUER).subject(principal)
                        .audience(AUDIENCE).claim("groups", roles)
                        .jwtID(UUID.randomUUID().toString())
                        .issueTime(Date.from(issued))
                        .expirationTime(Date.from(expires)).build());
        try
        {
            jwt.sign(new ECDSASigner(key));
        }
        catch (JOSEException failure)
        {
            throw new UncheckedIOException(new IOException(
                    "Couldn't sign a test token", failure));
        }
        return jwt.serialize();
    }

    /** Stops serving the key set. */
    @Override
    public void close()
    {
        server.stop(0);
    }
}
