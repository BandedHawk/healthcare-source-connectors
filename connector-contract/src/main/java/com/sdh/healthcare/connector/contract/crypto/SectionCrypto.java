/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.UUID;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KDF;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.HKDFParameterSpec;

/**
 * Section chunk encryption (design §7.2).
 * <ul>
 * <li>Section key = HKDF-SHA256(batch data key, info = "sdh-section" ‖
 * fileId ‖ seq), 32 bytes.</li>
 * <li>Each chunk (at most {@link #CHUNK_SIZE} plaintext bytes) is
 * zstd-compressed, then encrypted with AES-256-GCM, a random 96-bit nonce,
 * and AAD = fileId ‖ seq ‖ chunkIndex ‖ chunkCount.</li>
 * <li>Stored chunk = nonce ‖ ciphertext ‖ 128-bit tag.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SectionCrypto
{

    /** The most plaintext bytes in one chunk: 8 MiB. */
    public static final int CHUNK_SIZE = 8 * 1024 * 1024;

    /** The label that starts the HKDF info. */
    private static final byte[] INFO_LABEL =
            "sdh-section".getBytes(StandardCharsets.US_ASCII);

    /** The GCM nonce's length. */
    private static final int NONCE_BYTES = 12;

    /** The GCM tag's length. */
    private static final int TAG_BITS = 128;

    /** The zstd compression level. */
    private static final int ZSTD_LEVEL = 3;

    /** Makes the nonces. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Not instantiated: this class only has static methods. */
    private SectionCrypto()
    {
    }

    /**
     * How many chunks a section of a length is stored in.
     *
     * @param plaintextLength the section's length
     * @return the chunk count; at least 1, even for an empty section
     */
    public static int chunkCount(long plaintextLength)
    {
        return Math.toIntExact(Math.max(1,
                (plaintextLength + CHUNK_SIZE - 1) / CHUNK_SIZE));
    }

    /**
     * How many plaintext bytes a chunk of a section holds.
     *
     * @param plaintextLength the section's length
     * @param chunkIndex      the chunk, from 0
     * @return its length; 0 past the end
     */
    public static int expectedChunkLength(long plaintextLength,
            int chunkIndex)
    {
        long remaining = plaintextLength - (long) chunkIndex * CHUNK_SIZE;
        return (int) Math.min(CHUNK_SIZE, Math.max(0, remaining));
    }

    /**
     * Derives a section's key from its batch's data key.
     *
     * @param batchDataKey the batch's data key
     * @param fileId       the file's ID
     * @param seq          the section's number in the file
     * @return the AES-256 section key
     * @throws IllegalStateException if HKDF isn't available
     */
    public static SecretKey sectionKey(byte[] batchDataKey, UUID fileId,
            long seq)
    {
        byte[] info = ByteBuffer.allocate(INFO_LABEL.length + 16 + 8)
                .put(INFO_LABEL)
                .putLong(fileId.getMostSignificantBits())
                .putLong(fileId.getLeastSignificantBits())
                .putLong(seq)
                .array();
        try
        {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");
            return hkdf.deriveKey("AES", HKDFParameterSpec.ofExtract()
                    .addIKM(batchDataKey).thenExpand(info, 32));
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("HKDF failed", failure);
        }
    }

    /**
     * Compresses and encrypts one chunk.
     *
     * @param sectionKey the section's key
     * @param fileId     the file's ID
     * @param seq        the section's number in the file
     * @param chunkIndex the chunk, from 0
     * @param chunkCount the section's chunk count
     * @param plaintext  the chunk's bytes
     * @return nonce ‖ ciphertext ‖ tag
     * @throws IllegalStateException if encryption fails
     */
    public static byte[] encryptChunk(SecretKey sectionKey, UUID fileId,
            long seq, int chunkIndex, int chunkCount, byte[] plaintext)
    {
        byte[] compressed = Zstd.compress(plaintext, ZSTD_LEVEL);
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nonce);
        try
        {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, sectionKey,
                    new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(aad(fileId, seq, chunkIndex, chunkCount));
            byte[] out = new byte[NONCE_BYTES
                    + cipher.getOutputSize(compressed.length)];
            System.arraycopy(nonce, 0, out, 0, NONCE_BYTES);
            cipher.doFinal(compressed, 0, compressed.length, out,
                    NONCE_BYTES);
            return out;
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("Chunk encryption failed",
                    failure);
        }
    }

    /**
     * Decrypts and decompresses one chunk.
     *
     * @param sectionKey     the section's key
     * @param fileId         the file's ID
     * @param seq            the section's number in the file
     * @param chunkIndex     the chunk, from 0
     * @param chunkCount     the section's chunk count
     * @param expectedLength the chunk's plaintext length
     * @param stored         nonce ‖ ciphertext ‖ tag
     * @return the chunk's bytes
     * @throws ChunkIntegrityException if the chunk was altered, moved to
     *                                 another position or section, or
     *                                 decompresses to the wrong length
     * @throws IllegalStateException   if decryption fails otherwise
     */
    public static byte[] decryptChunk(SecretKey sectionKey, UUID fileId,
            long seq, int chunkIndex, int chunkCount, int expectedLength,
            byte[] stored)
    {
        byte[] compressed = decrypt(sectionKey, fileId, seq, chunkIndex,
                chunkCount, stored);
        long size = Zstd.getFrameContentSize(compressed);
        if (size != expectedLength)
        {
            throw new ChunkIntegrityException("Chunk length mismatch");
        }
        return Zstd.decompress(compressed, expectedLength);
    }

    /**
     * Like {@link #decryptChunk}, for small values whose length isn't known
     * up front (such as an encrypted file name): accepts any decompressed
     * length up to {@code maxLength}.
     *
     * @param key        the key
     * @param fileId     the file's ID
     * @param seq        the value's sequence number
     * @param chunkIndex the chunk, from 0
     * @param chunkCount the chunk count
     * @param maxLength  the longest value accepted
     * @param stored     nonce ‖ ciphertext ‖ tag
     * @return the value's bytes
     * @throws ChunkIntegrityException if the value was altered or moved,
     *                                 or is too long
     * @throws IllegalStateException   if decryption fails otherwise
     */
    public static byte[] decryptSmall(SecretKey key, UUID fileId, long seq,
            int chunkIndex, int chunkCount, int maxLength, byte[] stored)
    {
        byte[] compressed = decrypt(key, fileId, seq, chunkIndex, chunkCount,
                stored);
        long size = Zstd.getFrameContentSize(compressed);
        if (size < 0 || size > maxLength)
        {
            throw new ChunkIntegrityException("Chunk length out of range");
        }
        return Zstd.decompress(compressed, (int) size);
    }

    /**
     * Checks and decrypts a stored chunk, leaving it compressed.
     *
     * @param key        the key
     * @param fileId     the file's ID
     * @param seq        the section's number in the file
     * @param chunkIndex the chunk, from 0
     * @param chunkCount the section's chunk count
     * @param stored     nonce ‖ ciphertext ‖ tag
     * @return the compressed chunk
     * @throws ChunkIntegrityException if the chunk is too short, or was
     *                                 altered or moved
     * @throws IllegalStateException   if decryption fails otherwise
     */
    private static byte[] decrypt(SecretKey key, UUID fileId, long seq,
            int chunkIndex, int chunkCount, byte[] stored)
    {
        if (stored.length < NONCE_BYTES + TAG_BITS / 8)
        {
            throw new ChunkIntegrityException("Chunk too short");
        }
        try
        {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, stored, 0, NONCE_BYTES));
            cipher.updateAAD(aad(fileId, seq, chunkIndex, chunkCount));
            return cipher.doFinal(stored, NONCE_BYTES,
                    stored.length - NONCE_BYTES);
        }
        catch (AEADBadTagException tampered)
        {
            throw new ChunkIntegrityException("Chunk authentication failed");
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("Chunk decryption failed",
                    failure);
        }
    }

    /**
     * The AAD that binds a chunk to its place.
     *
     * @param fileId     the file's ID
     * @param seq        the section's number in the file
     * @param chunkIndex the chunk, from 0
     * @param chunkCount the section's chunk count
     * @return fileId ‖ seq ‖ chunkIndex ‖ chunkCount, big-endian
     */
    static byte[] aad(UUID fileId, long seq, int chunkIndex, int chunkCount)
    {
        return ByteBuffer.allocate(16 + 8 + 4 + 4)
                .putLong(fileId.getMostSignificantBits())
                .putLong(fileId.getLeastSignificantBits())
                .putLong(seq)
                .putInt(chunkIndex)
                .putInt(chunkCount)
                .array();
    }
}
