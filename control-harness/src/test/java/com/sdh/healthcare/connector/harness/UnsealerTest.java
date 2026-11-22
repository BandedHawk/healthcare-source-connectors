/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import java.security.MessageDigest;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link Unsealer}: a sealed export opened, and one tampered with
 * or cut off refused.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class UnsealerTest
{

    /** Who the parts are sealed to. */
    static final Operator TO = Operator.of("erin-exporter", "view");

    /** Creates the test. */
    UnsealerTest()
    {
    }

    /**
     * Seals parts as a stage does: a header, a line a part, a trailer.
     *
     * @param parts the plaintext parts
     * @return the lines
     * @throws Exception if one can't be sealed
     */
    static List<String> seal(String... parts) throws Exception
    {
        List<String> lines = new ArrayList<>();
        lines.add("{\"sealedTo\":\"" + TO.fingerprint() + "\"}");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        int part = 0;
        for (String each : parts)
        {
            JWEObject jwe = new JWEObject(new JWEHeader(
                    JWEAlgorithm.ECDH_ES_A256KW, EncryptionMethod.A256GCM),
                    new Payload(each.getBytes(UTF_8)));
            jwe.encrypt(new ECDHEncrypter((ECPublicKey) TO.sealing()
                    .getPublic()));
            String text = jwe.serialize();
            digest.update(text.getBytes(US_ASCII));
            lines.add("{\"part\":" + ++part + ",\"jwe\":\"" + text + "\"}");
        }
        lines.add("{\"end\":true,\"parts\":" + part + ",\"sha256\":\""
                + HexFormat.of().formatHex(digest.digest()) + "\"}");
        return lines;
    }

    /**
     * Opens lines.
     *
     * @param lines the lines
     * @return the plaintext
     */
    static String open(List<String> lines)
    {
        return new String(Unsealer.unseal(String.join("\n", lines),
                TO.sealing().getPrivate()), UTF_8);
    }

    /**
     * The parts open, in order.
     *
     * @throws Exception if they can't be sealed
     */
    @Test
    void opensTheParts() throws Exception
    {
        assertThat(open(seal("one ", "two"))).isEqualTo("one two");
    }

    /**
     * An export without its trailer is refused.
     *
     * @throws Exception if it can't be sealed
     */
    @Test
    void refusesOneCutOff() throws Exception
    {
        List<String> lines = seal("one", "two");
        lines.remove(lines.size() - 1);

        assertThatThrownBy(() -> open(lines))
                .isInstanceOf(HarnessFailure.class);
    }

    /**
     * An export missing a part is refused.
     *
     * @throws Exception if it can't be sealed
     */
    @Test
    void refusesOneMissingAPart() throws Exception
    {
        List<String> lines = seal("one", "two", "three");
        lines.remove(2);

        assertThatThrownBy(() -> open(lines))
                .isInstanceOf(HarnessFailure.class);
    }

    /**
     * Parts that don't match the trailer's digest are refused.
     *
     * @throws Exception if they can't be sealed
     */
    @Test
    void refusesPartsReplaced() throws Exception
    {
        List<String> lines = seal("one");
        List<String> other = seal("uno");
        lines.set(1, other.get(1));

        assertThatThrownBy(() -> open(lines))
                .isInstanceOf(HarnessFailure.class)
                .hasMessageContaining("trailer");
    }
}
