/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import com.nimbusds.jose.crypto.RSADecrypter;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.util.HexFormat;

/**
 * Opens a sealed result (CONTROL-API-DESIGN.md §7) with a test
 * principal's private key: the header line, a compact JWE a part, and the
 * trailer, whose count and SHA-256 of the parts it checks before it
 * decrypts. A stream without its trailer, or with an incomplete one, was
 * cut off and is refused.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Unsealer
{

    /** Reads the lines. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Not instantiated. */
    private Unsealer()
    {
    }

    /**
     * Opens a sealed result.
     *
     * @param ndjson the answer's body
     * @param key    the private half of the key it was sealed to
     * @return the parts' plaintext, in order
     * @throws HarnessFailure if it is cut off, its trailer doesn't match,
     *                        or a part can't be opened with the key
     */
    public static byte[] unseal(String ndjson, PrivateKey key)
    {
        String[] lines = ndjson.strip().split("\n");
        try
        {
            HarnessFailure.check(lines.length >= 2
                    && JSON.readTree(lines[0]).has("sealedTo"),
                    "A sealed result without its header");
            JsonNode trailer = JSON.readTree(lines[lines.length - 1]);
            HarnessFailure.check(trailer.path("end").asBoolean(false),
                    "A sealed result cut off: no trailer");
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            ByteArrayOutputStream plain = new ByteArrayOutputStream();
            JWEDecrypter decrypter = key instanceof ECPrivateKey ec
                    ? new ECDHDecrypter(ec) : new RSADecrypter(key);
            int parts = 0;
            for (int line = 1; line < lines.length - 1; line++)
            {
                JsonNode part = JSON.readTree(lines[line]);
                HarnessFailure.check(part.path("part").asInt() == parts + 1,
                        "A sealed result's parts out of order");
                String jwe = part.path("jwe").asText();
                digest.update(jwe.getBytes(US_ASCII));
                JWEObject object = JWEObject.parse(jwe);
                object.decrypt(decrypter);
                plain.writeBytes(object.getPayload().toBytes());
                parts++;
            }
            HarnessFailure.check(trailer.path("parts").asInt() == parts,
                    "A sealed result's trailer counts "
                            + trailer.path("parts").asInt() + " parts, not "
                            + parts);
            HarnessFailure.check(HexFormat.of().formatHex(digest.digest())
                    .equals(trailer.path("sha256").asText()),
                    "A sealed result's parts don't match its trailer");
            return plain.toByteArray();
        }
        catch (HarnessFailure failure)
        {
            throw failure;
        }
        catch (Exception failure)
        {
            throw new HarnessFailure("A sealed result couldn't be opened: "
                    + failure.getClass().getSimpleName(), failure);
        }
    }
}
