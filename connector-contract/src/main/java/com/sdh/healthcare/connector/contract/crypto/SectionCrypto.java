package com.sdh.healthcare.connector.contract.crypto;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.KDF;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.HKDFParameterSpec;

/**
 * Section chunk encryption (design §7.2).
 *
 * <ul>
 *   <li>Section key = HKDF-SHA256(batch data key, info = "sdh-section" ‖ fileId ‖ seq), 32 bytes.
 *   <li>Each chunk (at most {@link #CHUNK_SIZE} plaintext bytes) is zstd-compressed, then encrypted with
 *       AES-256-GCM, random 96-bit nonce, AAD = fileId ‖ seq ‖ chunkIndex ‖ chunkCount.
 *   <li>Stored chunk = nonce ‖ ciphertext ‖ 128-bit tag.
 * </ul>
 */
public final class SectionCrypto {
    public static final int CHUNK_SIZE = 8 * 1024 * 1024;
    private static final byte[] INFO_LABEL = "sdh-section".getBytes(StandardCharsets.US_ASCII);
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final int ZSTD_LEVEL = 3;
    private static final SecureRandom RANDOM = new SecureRandom();

    private SectionCrypto() {}

    public static int chunkCount(long plaintextLength) {
        return Math.toIntExact(Math.max(1, (plaintextLength + CHUNK_SIZE - 1) / CHUNK_SIZE));
    }

    public static int expectedChunkLength(long plaintextLength, int chunkIndex) {
        long remaining = plaintextLength - (long) chunkIndex * CHUNK_SIZE;
        return (int) Math.min(CHUNK_SIZE, Math.max(0, remaining));
    }

    public static SecretKey sectionKey(byte[] batchDataKey, UUID fileId, long seq) {
        byte[] info = ByteBuffer.allocate(INFO_LABEL.length + 16 + 8)
                .put(INFO_LABEL)
                .putLong(fileId.getMostSignificantBits())
                .putLong(fileId.getLeastSignificantBits())
                .putLong(seq)
                .array();
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");
            return hkdf.deriveKey(
                    "AES", HKDFParameterSpec.ofExtract().addIKM(batchDataKey).thenExpand(info, 32));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HKDF failed", e);
        }
    }

    public static byte[] encryptChunk(
            SecretKey sectionKey, UUID fileId, long seq, int chunkIndex, int chunkCount, byte[] plaintext) {
        byte[] compressed = Zstd.compress(plaintext, ZSTD_LEVEL);
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nonce);
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, sectionKey, new GCMParameterSpec(TAG_BITS, nonce));
            c.updateAAD(aad(fileId, seq, chunkIndex, chunkCount));
            byte[] out = new byte[NONCE_BYTES + c.getOutputSize(compressed.length)];
            System.arraycopy(nonce, 0, out, 0, NONCE_BYTES);
            c.doFinal(compressed, 0, compressed.length, out, NONCE_BYTES);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Chunk encryption failed", e);
        }
    }

    /**
     * Decrypts and decompresses one chunk. Throws {@link ChunkIntegrityException} if the chunk was
     * altered, moved to another position or section, or decompresses to the wrong length.
     */
    public static byte[] decryptChunk(
            SecretKey sectionKey, UUID fileId, long seq, int chunkIndex, int chunkCount, int expectedLength,
            byte[] stored) {
        if (stored.length < NONCE_BYTES + TAG_BITS / 8) {
            throw new ChunkIntegrityException("Chunk too short");
        }
        byte[] compressed;
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, sectionKey, new GCMParameterSpec(TAG_BITS, stored, 0, NONCE_BYTES));
            c.updateAAD(aad(fileId, seq, chunkIndex, chunkCount));
            compressed = c.doFinal(stored, NONCE_BYTES, stored.length - NONCE_BYTES);
        } catch (javax.crypto.AEADBadTagException e) {
            throw new ChunkIntegrityException("Chunk authentication failed");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Chunk decryption failed", e);
        }
        long size = Zstd.getFrameContentSize(compressed);
        if (size != expectedLength) {
            throw new ChunkIntegrityException("Chunk length mismatch");
        }
        return Zstd.decompress(compressed, expectedLength);
    }

    /**
     * Like {@link #decryptChunk} for small values whose length isn't known up front (such as an encrypted
     * filename): accepts any decompressed length up to {@code maxLength}.
     */
    public static byte[] decryptSmall(SecretKey key, UUID fileId, long seq, int chunkIndex, int chunkCount,
            int maxLength, byte[] stored) {
        if (stored.length < NONCE_BYTES + TAG_BITS / 8) {
            throw new ChunkIntegrityException("Chunk too short");
        }
        byte[] compressed;
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, stored, 0, NONCE_BYTES));
            c.updateAAD(aad(fileId, seq, chunkIndex, chunkCount));
            compressed = c.doFinal(stored, NONCE_BYTES, stored.length - NONCE_BYTES);
        } catch (javax.crypto.AEADBadTagException e) {
            throw new ChunkIntegrityException("Chunk authentication failed");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Chunk decryption failed", e);
        }
        long size = Zstd.getFrameContentSize(compressed);
        if (size < 0 || size > maxLength) {
            throw new ChunkIntegrityException("Chunk length out of range");
        }
        return Zstd.decompress(compressed, (int) size);
    }

    static byte[] aad(UUID fileId, long seq, int chunkIndex, int chunkCount) {
        return ByteBuffer.allocate(16 + 8 + 4 + 4)
                .putLong(fileId.getMostSignificantBits())
                .putLong(fileId.getLeastSignificantBits())
                .putLong(seq)
                .putInt(chunkIndex)
                .putInt(chunkCount)
                .array();
    }
}
