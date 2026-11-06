/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link RecordCrypto} decrypts chunks and small values, and
 * what it refuses.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RecordCryptoTest
{

    /** The file. */
    static final UUID FILE = UUID.randomUUID();

    /** The record. */
    static final long SEQ = 4;

    /** The plaintext. */
    static final byte[] TEXT =
            "a,b,c\n1,2,3\n".getBytes(StandardCharsets.US_ASCII);

    /** The record's key. */
    static final SecretKey KEY =
            RecordCrypto.recordKey(new byte[32], FILE, SEQ);

    /** The chunk, as chunk 1 of 3. */
    static final byte[] STORED =
            RecordCrypto.encryptChunk(KEY, FILE, SEQ, 1, 3, TEXT);

    /** A chunk and a small value read back as written. */
    @Test
    void readsBackWhatWasWritten()
    {
        assertThat(RecordCrypto.decryptChunk(KEY, FILE, SEQ, 1, 3,
                TEXT.length, STORED)).isEqualTo(TEXT);
        assertThat(RecordCrypto.decryptSmall(KEY, FILE, SEQ, 1, 3,
                TEXT.length, STORED)).isEqualTo(TEXT);
        assertThat(RecordCrypto.decryptSmall(KEY, FILE, SEQ, 1, 3,
                1_000, STORED)).isEqualTo(TEXT);
    }

    /** An altered byte, anywhere, fails authentication. */
    @Test
    void refusesAnAlteredChunk()
    {
        for (int at : new int[] {0, 12, STORED.length - 1})
        {
            byte[] altered = STORED.clone();
            altered[at] ^= 1;
            assertThatThrownBy(() -> RecordCrypto.decryptChunk(KEY, FILE,
                    SEQ, 1, 3, TEXT.length, altered))
                    .isInstanceOf(ChunkIntegrityException.class)
                    .hasMessage("Chunk authentication failed");
            assertThatThrownBy(() -> RecordCrypto.decryptSmall(KEY, FILE,
                    SEQ, 1, 3, TEXT.length, altered))
                    .isInstanceOf(ChunkIntegrityException.class)
                    .hasMessage("Chunk authentication failed");
        }
    }

    /**
     * A chunk moved to another file, record, position or count, or read
     * with another key, fails authentication.
     */
    @Test
    void refusesAChunkMovedOrReadWithAnotherKey()
    {
        SecretKey other = RecordCrypto.recordKey(new byte[32], FILE, 5);
        Runnable[] reads = {
            () -> RecordCrypto.decryptChunk(KEY, UUID.randomUUID(), SEQ, 1,
                    3, TEXT.length, STORED),
            () -> RecordCrypto.decryptChunk(KEY, FILE, SEQ + 1, 1, 3,
                    TEXT.length, STORED),
            () -> RecordCrypto.decryptChunk(KEY, FILE, SEQ, 2, 3,
                    TEXT.length, STORED),
            () -> RecordCrypto.decryptChunk(KEY, FILE, SEQ, 1, 4,
                    TEXT.length, STORED),
            () -> RecordCrypto.decryptChunk(other, FILE, SEQ, 1, 3,
                    TEXT.length, STORED),
            () -> RecordCrypto.decryptSmall(KEY, FILE, SEQ, 0, 3,
                    TEXT.length, STORED),
            () -> RecordCrypto.decryptSmall(other, FILE, SEQ, 1, 3,
                    TEXT.length, STORED)};
        for (Runnable read : reads)
        {
            assertThatThrownBy(read::run)
                    .isInstanceOf(ChunkIntegrityException.class)
                    .hasMessage("Chunk authentication failed");
        }
    }

    /** A chunk shorter than a nonce and tag is refused before decrypting. */
    @Test
    void refusesAChunkTooShort()
    {
        byte[] tooShort = new byte[27];
        assertThatThrownBy(() -> RecordCrypto.decryptChunk(KEY, FILE, SEQ,
                1, 3, TEXT.length, tooShort))
                .isInstanceOf(ChunkIntegrityException.class)
                .hasMessage("Chunk too short");
        assertThatThrownBy(() -> RecordCrypto.decryptSmall(KEY, FILE, SEQ,
                1, 3, TEXT.length, tooShort))
                .isInstanceOf(ChunkIntegrityException.class)
                .hasMessage("Chunk too short");
    }

    /**
     * A chunk must be exactly its expected length; a small value at most
     * its limit.
     */
    @Test
    void refusesTheWrongLength()
    {
        assertThatThrownBy(() -> RecordCrypto.decryptChunk(KEY, FILE, SEQ,
                1, 3, TEXT.length + 1, STORED))
                .isInstanceOf(ChunkIntegrityException.class)
                .hasMessage("Chunk length mismatch");
        assertThatThrownBy(() -> RecordCrypto.decryptChunk(KEY, FILE, SEQ,
                1, 3, TEXT.length - 1, STORED))
                .isInstanceOf(ChunkIntegrityException.class)
                .hasMessage("Chunk length mismatch");
        assertThatThrownBy(() -> RecordCrypto.decryptSmall(KEY, FILE, SEQ,
                1, 3, TEXT.length - 1, STORED))
                .isInstanceOf(ChunkIntegrityException.class)
                .hasMessage("Chunk length out of range");
    }

    /** An empty value reads back empty. */
    @Test
    void readsAnEmptyValue()
    {
        byte[] empty = RecordCrypto.encryptChunk(KEY, FILE, SEQ, 0, 1,
                new byte[0]);
        assertThat(RecordCrypto.decryptChunk(KEY, FILE, SEQ, 0, 1, 0, empty))
                .isEmpty();
        assertThat(RecordCrypto.decryptSmall(KEY, FILE, SEQ, 0, 1, 10,
                empty)).isEmpty();
    }
}
