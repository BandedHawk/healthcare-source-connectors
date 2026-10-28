package com.sdh.healthcare.connector.core.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecoveryEnvelopeTest {
    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }

    static byte[] seal(byte[] data, UUID id, KeyPair k) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RecoveryEnvelope.encrypt(SeekableByteSource.of(data), id, Optional.of("smith-ccd.xml"),
                (ECPublicKey) k.getPublic(), out);
        return out.toByteArray();
    }

    @Test
    void roundTripsMultipleRecordsAndTheName() throws Exception {
        KeyPair k = ec();
        byte[] data = new byte[(8 << 20) + 777];
        new Random(3).nextBytes(data);
        UUID id = UUID.randomUUID();
        byte[] sealed = seal(data, id, k);
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        RecoveryEnvelope.Metadata meta = RecoveryEnvelope.decrypt(new ByteArrayInputStream(sealed), k.getPrivate(), plain);
        assertThat(plain.toByteArray()).isEqualTo(data);
        assertThat(meta.fileId()).isEqualTo(id);
        assertThat(meta.originalName()).contains("smith-ccd.xml");
        assertThat(new String(sealed, java.nio.charset.StandardCharsets.ISO_8859_1)).doesNotContain("smith");
    }

    @Test
    void emptyFile() throws Exception {
        KeyPair k = ec();
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        RecoveryEnvelope.decrypt(new ByteArrayInputStream(seal(new byte[0], UUID.randomUUID(), k)), k.getPrivate(), plain);
        assertThat(plain.size()).isZero();
    }

    @Test
    void wrongKeyTruncationAndTamperingAreDetected() throws Exception {
        KeyPair k = ec();
        byte[] data = new byte[(8 << 20) + 10];
        byte[] sealed = seal(data, UUID.randomUUID(), k);
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(new ByteArrayInputStream(sealed), ec().getPrivate(),
                new ByteArrayOutputStream())).isInstanceOf(IOException.class);
        // Drop the last record: the one before it isn't flagged last, so this is caught.
        byte[] truncated = Arrays.copyOf(sealed, sealed.length - 40);
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(new ByteArrayInputStream(truncated), k.getPrivate(),
                new ByteArrayOutputStream())).isInstanceOf(IOException.class);
        byte[] flipped = sealed.clone();
        flipped[flipped.length / 2] ^= 1;
        assertThatThrownBy(() -> RecoveryEnvelope.decrypt(new ByteArrayInputStream(flipped), k.getPrivate(),
                new ByteArrayOutputStream())).isInstanceOf(IOException.class);
    }
}
