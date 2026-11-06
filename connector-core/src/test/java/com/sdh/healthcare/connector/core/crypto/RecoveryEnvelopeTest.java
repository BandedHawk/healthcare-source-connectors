/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RecoveryEnvelope}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RecoveryEnvelopeTest
{

    /**
     * Makes a P-256 key pair.
     *
     * @return the pair
     * @throws Exception if it can't be made
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Seals data in an envelope, with a fixed original name.
     *
     * @param data   the data
     * @param itemId the file's ID
     * @param keys   the recipient's key pair
     * @return the envelope
     * @throws Exception if it can't be sealed
     */
    static byte[] seal(byte[] data, UUID itemId, KeyPair keys)
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RecoveryEnvelope.encrypt(SeekableByteSource.of(data), itemId,
                Optional.of("smith-ccd.xml"), (ECPublicKey) keys.getPublic(),
                out);
        return out.toByteArray();
    }

    /**
     * Data over several records and the original name come back, and the
     * name isn't readable in the envelope.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void roundTripsMultipleRecordsAndTheName() throws Exception
    {
        KeyPair keys = ec();
        byte[] data = new byte[(8 << 20) + 777];
        new Random(3).nextBytes(data);
        UUID itemId = UUID.randomUUID();
        byte[] sealed = seal(data, itemId, keys);
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        RecoveryEnvelope.Metadata metadata = RecoveryEnvelope.decrypt(
                new ByteArrayInputStream(sealed), keys.getPrivate(), plain);
        assertThat(plain.toByteArray()).isEqualTo(data);
        assertThat(metadata.itemId()).isEqualTo(itemId);
        assertThat(metadata.originalName()).contains("smith-ccd.xml");
        assertThat(new String(sealed, StandardCharsets.ISO_8859_1))
                .doesNotContain("smith");
    }

    /**
     * An empty file seals and opens.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void emptyFile() throws Exception
    {
        KeyPair keys = ec();
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        RecoveryEnvelope.decrypt(new ByteArrayInputStream(
                seal(new byte[0], UUID.randomUUID(), keys)),
                keys.getPrivate(), plain);
        assertThat(plain.size()).isZero();
    }

    /**
     * Opening fails with the wrong key, a missing last record, or a changed
     * byte.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wrongKeyTruncationAndTamperingAreDetected() throws Exception
    {
        KeyPair keys = ec();
        byte[] data = new byte[(8 << 20) + 10];
        byte[] sealed = seal(data, UUID.randomUUID(), keys);
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(
                new ByteArrayInputStream(sealed), ec().getPrivate(),
                new ByteArrayOutputStream()))
                .isInstanceOf(IOException.class);
        // Drop the last record: the one before it isn't flagged last, so
        // this is caught.
        byte[] truncated = Arrays.copyOf(sealed, sealed.length - 40);
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(
                new ByteArrayInputStream(truncated), keys.getPrivate(),
                new ByteArrayOutputStream()))
                .isInstanceOf(IOException.class);
        byte[] flipped = sealed.clone();
        flipped[flipped.length / 2] ^= 1;
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(
                new ByteArrayInputStream(flipped), keys.getPrivate(),
                new ByteArrayOutputStream()))
                .isInstanceOf(IOException.class);
    }
}
