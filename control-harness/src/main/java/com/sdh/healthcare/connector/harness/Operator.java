/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * A test principal: its name, its roles, and the sealing key pair its PHI
 * results are sealed to, once someone else registers the public half.
 *
 * @param name    the principal, as its tokens name it
 * @param roles   its roles, as its tokens' {@code groups}
 * @param sealing its sealing key pair, EC P-256
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Operator(String name, List<String> roles, KeyPair sealing)
{

    /**
     * Copies the roles.
     *
     * @param name    the name
     * @param roles   the roles
     * @param sealing the key pair
     */
    public Operator
    {
        roles = List.copyOf(roles);
    }

    /**
     * A principal with a new sealing key pair.
     *
     * @param name  its name
     * @param roles its roles
     * @return the principal
     */
    public static Operator of(String name, String... roles)
    {
        try
        {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            return new Operator(name, List.of(roles),
                    generator.generateKeyPair());
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("No EC key pair", failure);
        }
    }

    /**
     * The sealing key's public half, as PEM, for {@code /operator-keys}.
     *
     * @return the PEM
     */
    public String publicPem()
    {
        return "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(sealing.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
    }

    /**
     * The sealing key's fingerprint: the SHA-256 of its
     * SubjectPublicKeyInfo, as {@code sealTo} names it.
     *
     * @return the fingerprint, in lower-case hex
     */
    public String fingerprint()
    {
        try
        {
            return HexFormat.of().formatHex(MessageDigest.getInstance(
                    "SHA-256").digest(sealing.getPublic().getEncoded()));
        }
        catch (NoSuchAlgorithmException failure)
        {
            throw new IllegalStateException("No SHA-256", failure);
        }
    }

    /**
     * The principal without its keys, for messages.
     *
     * @return its name and roles
     */
    @Override
    public String toString()
    {
        return name + " " + roles;
    }
}
