/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.crypto;

import com.github.luben.zstd.Zstd;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.ECPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.KDF;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.HKDFParameterSpec;

/**
 * The local {@code failed/<fileId>.enc} fallback (design §11): a whole file
 * encrypted to the recovery public key, for when storage or the KMS can't be
 * reached, or the file is over {@code max-file-bytes}. Only whoever holds the
 * recovery private key can read it, so the host needs no secret.
 * <p>
 * The format streams, and suits a file of any size:
 * <pre>
 * header = "SDHFB1" ‖ fileId (16) ‖ SHA-256(recovery public key) (32)
 *          ‖ u16 length ‖ ephemeral EC public key (X.509)
 * key    = HKDF-SHA256(ECDH(ephemeral, recovery),
 *                      salt = SHA-256(header), info = "sdh-fallback")
 * record = u32 length ‖ nonce (12) ‖ AES-256-GCM(zstd(plaintext)) ‖ tag
 * AAD    = SHA-256(header) ‖ u64 recordIndex ‖ u8 last
 * </pre>
 * Record 0 is a JSON object of metadata (the file's ID, original name and
 * size). The file's bytes follow in records of 8 MiB, the final one flagged
 * {@code last}, so a missing, reordered or spliced record is detected.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecoveryEnvelope
{

    /** The bytes every envelope starts with. */
    private static final byte[] MAGIC =
            "SDHFB1".getBytes(StandardCharsets.US_ASCII);

    /** How many bytes of the file each record holds: 8 MiB. */
    private static final int RECORD_BYTES = 8 << 20;

    /** How long each record's nonce is, in bytes. */
    private static final int NONCE_BYTES = 12;

    /** How long a GCM authentication tag is, in bits. */
    private static final int TAG_BITS = 128;

    /** How long a GCM authentication tag is, in bytes. */
    private static final int TAG_BYTES = TAG_BITS / Byte.SIZE;

    /**
     * The longest a record may be: a full record that compressed badly,
     * with room for zstd's framing. Anything longer is damage.
     */
    private static final int MAX_RECORD_LENGTH =
            RECORD_BYTES + (RECORD_BYTES >> 4) + 4096;

    /** How long a file ID is, in bytes. */
    private static final int FILE_ID_BYTES = 16;

    /** How long a SHA-256 hash is, in bytes. */
    private static final int HASH_BYTES = 32;

    /** How long the derived AES key is: 32 bytes, 256 bits. */
    private static final int KEY_BYTES = 32;

    /** The zstd level the records are compressed at. */
    private static final int ZSTD_LEVEL = 3;

    /** The cipher the records are encrypted with. */
    private static final String CIPHER = "AES/GCM/NoPadding";

    /** The HKDF info that ties the derived key to this use. */
    private static final byte[] KEY_INFO =
            "sdh-fallback".getBytes(StandardCharsets.US_ASCII);

    /** The metadata field holding the file's ID. */
    private static final String FILE_ID_FIELD = "fileId";

    /** The metadata field holding the file's original name, if known. */
    private static final String NAME_FIELD = "name";

    /** The metadata field holding the file's size in bytes. */
    private static final String SIZE_FIELD = "size";

    /** Makes the nonces. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * What an envelope says about the file inside it.
     *
     * @param fileId       the file's ID
     * @param originalName the file's name when it arrived, if it was kept
     * @param size         the file's size, in bytes
     */
    public record Metadata(UUID fileId, Optional<String> originalName,
            long size)
    {
    }

    /** Not used: there are only static methods. */
    private RecoveryEnvelope()
    {
    }

    /**
     * Encrypts a whole file to the recovery public key, writing the
     * envelope as it goes. Each record's plaintext is wiped from memory once
     * it is written.
     *
     * @param source       the file
     * @param fileId       the file's ID
     * @param originalName the file's name when it arrived, to keep with it;
     *                     empty to leave it out
     * @param recovery     the recovery public key
     * @param sink         where the envelope is written
     * @throws IOException           if the file can't be read or the
     *                               envelope written
     * @throws IllegalStateException if the Java runtime lacks the
     *                               algorithms
     */
    public static void encrypt(SeekableByteSource source, UUID fileId,
            Optional<String> originalName, ECPublicKey recovery,
            OutputStream sink) throws IOException
    {
        try
        {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(recovery.getParams());
            KeyPair ephemeral = generator.generateKeyPair();
            byte[] ephemeralPublic = ephemeral.getPublic().getEncoded();
            byte[] header = header(fileId, sha256(recovery.getEncoded()),
                    ephemeralPublic);
            SecretKey key = key(ephemeral.getPrivate(), recovery, header);
            byte[] headerHash = sha256(header);

            DataOutputStream output = new DataOutputStream(sink);
            output.write(header);
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put(FILE_ID_FIELD, fileId.toString());
            originalName.ifPresent(name -> metadata.put(NAME_FIELD, name));
            long size = source.size();
            metadata.put(SIZE_FIELD, size);
            writeRecord(output, key, headerHash, 0, false,
                    JSONObjectUtils.toJSONString(metadata)
                            .getBytes(StandardCharsets.UTF_8));
            long records = recordCount(size);
            for (long record = 0; record < records; record++)
            {
                long offset = record * RECORD_BYTES;
                int length = (int) Math.min(RECORD_BYTES, size - offset);
                byte[] plaintext = source.readFully(offset, length);
                writeRecord(output, key, headerHash, record + 1,
                        record == records - 1, plaintext);
                Arrays.fill(plaintext, (byte) 0);
            }
            output.flush();
        }
        catch (GeneralSecurityException unavailable)
        {
            throw new IllegalStateException("Fallback encryption failed",
                    unavailable);
        }
    }

    /**
     * Decrypts an envelope with the recovery private key.
     *
     * @param source          the envelope
     * @param recoveryPrivate the recovery private key
     * @param plaintext       where the file's bytes are written
     * @return what the envelope says about the file
     * @throws IOException if the key is wrong, or the envelope is damaged,
     *                     cut short or isn't one
     */
    public static Metadata decrypt(InputStream source,
            PrivateKey recoveryPrivate, OutputStream plaintext)
            throws IOException
    {
        DataInputStream input = new DataInputStream(source);
        byte[] magic = input.readNBytes(MAGIC.length);
        if (!Arrays.equals(magic, MAGIC))
        {
            throw new IOException("Not a recovery envelope");
        }
        byte[] fileIdBytes = input.readNBytes(FILE_ID_BYTES);
        byte[] recoveryKeyHash = input.readNBytes(HASH_BYTES);
        int ephemeralLength = input.readUnsignedShort();
        byte[] ephemeralPublic = input.readNBytes(ephemeralLength);
        if (fileIdBytes.length != FILE_ID_BYTES
                || recoveryKeyHash.length != HASH_BYTES
                || ephemeralPublic.length != ephemeralLength)
        {
            throw new EOFException("Truncated header");
        }
        ByteBuffer fileIdBuffer = ByteBuffer.wrap(fileIdBytes);
        UUID fileId =
                new UUID(fileIdBuffer.getLong(), fileIdBuffer.getLong());
        byte[] header = header(fileId, recoveryKeyHash, ephemeralPublic);
        byte[] headerHash = sha256(header);
        SecretKey key;
        try
        {
            ECPublicKey ephemeral = (ECPublicKey) KeyFactory.getInstance("EC")
                    .generatePublic(new X509EncodedKeySpec(ephemeralPublic));
            key = key(recoveryPrivate, ephemeral, header);
        }
        catch (GeneralSecurityException unusable)
        {
            throw new IOException("Unusable key or header");
        }
        Map<String, Object> metadata;
        try
        {
            metadata = JSONObjectUtils.parse(new String(
                    readRecord(input, key, headerHash, 0, false),
                    StandardCharsets.UTF_8));
        }
        catch (ParseException damaged)
        {
            throw new IOException("Damaged metadata");
        }
        long size = ((Number) metadata.get(SIZE_FIELD)).longValue();
        long records = recordCount(size);
        long written = 0;
        for (long record = 0; record < records; record++)
        {
            byte[] bytes = readRecord(input, key, headerHash, record + 1,
                    record == records - 1);
            plaintext.write(bytes);
            written += bytes.length;
        }
        if (written != size || input.read() != -1)
        {
            throw new IOException("Size mismatch or trailing data");
        }
        return new Metadata(fileId,
                Optional.ofNullable((String) metadata.get(NAME_FIELD)), size);
    }

    /**
     * How many data records a file takes: at least one, so an empty file
     * still has a record flagged {@code last}.
     *
     * @param size the file's size, in bytes
     * @return the number of data records, after the metadata record
     */
    private static long recordCount(long size)
    {
        return Math.max(1, (size + RECORD_BYTES - 1) / RECORD_BYTES);
    }

    /**
     * Builds an envelope's header.
     *
     * @param fileId          the file's ID
     * @param recoveryKeyHash the SHA-256 of the recovery public key
     * @param ephemeralPublic the ephemeral public key, X.509-encoded
     * @return the header's bytes
     */
    private static byte[] header(UUID fileId, byte[] recoveryKeyHash,
            byte[] ephemeralPublic)
    {
        return ByteBuffer.allocate(MAGIC.length + FILE_ID_BYTES + HASH_BYTES
                + Short.BYTES + ephemeralPublic.length)
                .put(MAGIC)
                .putLong(fileId.getMostSignificantBits())
                .putLong(fileId.getLeastSignificantBits())
                .put(recoveryKeyHash)
                .putShort((short) ephemeralPublic.length)
                .put(ephemeralPublic)
                .array();
    }

    /**
     * Derives the records' AES key: ECDH between the two keys, then HKDF
     * with the header's hash as the salt. The shared secret is wiped once
     * the key is derived.
     *
     * @param privateKey one side's private key
     * @param publicKey  the other side's public key
     * @param header     the envelope's header
     * @return the AES key
     * @throws GeneralSecurityException if the keys can't be used
     */
    private static SecretKey key(PrivateKey privateKey, PublicKey publicKey,
            byte[] header) throws GeneralSecurityException
    {
        KeyAgreement agreement = KeyAgreement.getInstance("ECDH");
        agreement.init(privateKey);
        agreement.doPhase(publicKey, true);
        byte[] secret = agreement.generateSecret();
        try
        {
            return KDF.getInstance("HKDF-SHA256").deriveKey("AES",
                    HKDFParameterSpec.ofExtract().addIKM(secret)
                            .addSalt(sha256(header))
                            .thenExpand(KEY_INFO, KEY_BYTES));
        }
        finally
        {
            Arrays.fill(secret, (byte) 0);
        }
    }

    /**
     * Builds a record's additional authenticated data, which ties the
     * record to its envelope and its place in it.
     *
     * @param headerHash the SHA-256 of the envelope's header
     * @param index      the record's index, from 0 for the metadata
     * @param last       whether it is the final record
     * @return the AAD's bytes
     */
    private static byte[] aad(byte[] headerHash, long index, boolean last)
    {
        return ByteBuffer.allocate(HASH_BYTES + Long.BYTES + 1)
                .put(headerHash)
                .putLong(index)
                .put((byte) (last ? 1 : 0))
                .array();
    }

    /**
     * Compresses, encrypts and writes one record.
     *
     * @param output     where the record is written
     * @param key        the AES key
     * @param headerHash the SHA-256 of the envelope's header
     * @param index      the record's index, from 0 for the metadata
     * @param last       whether it is the final record
     * @param plaintext  the record's bytes
     * @throws IOException              if the record can't be written
     * @throws GeneralSecurityException if it can't be encrypted
     */
    private static void writeRecord(DataOutputStream output, SecretKey key,
            byte[] headerHash, long index, boolean last, byte[] plaintext)
            throws IOException, GeneralSecurityException
    {
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nonce);
        Cipher cipher = Cipher.getInstance(CIPHER);
        cipher.init(Cipher.ENCRYPT_MODE, key,
                new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(aad(headerHash, index, last));
        byte[] ciphertext =
                cipher.doFinal(Zstd.compress(plaintext, ZSTD_LEVEL));
        output.writeInt(NONCE_BYTES + ciphertext.length);
        output.write(nonce);
        output.write(ciphertext);
    }

    /**
     * Reads, decrypts and decompresses one record.
     *
     * @param input      the envelope, at the record's start
     * @param key        the AES key
     * @param headerHash the SHA-256 of the envelope's header
     * @param index      the record's expected index, from 0 for the
     *                   metadata
     * @param last       whether it should be the final record
     * @return the record's bytes
     * @throws IOException if the record is missing, cut short, damaged, out
     *                     of place, or for another key
     */
    private static byte[] readRecord(DataInputStream input, SecretKey key,
            byte[] headerHash, long index, boolean last) throws IOException
    {
        int length;
        try
        {
            length = input.readInt();
        }
        catch (EOFException ended)
        {
            throw new EOFException("Truncated: record " + index + " missing");
        }
        if (length < NONCE_BYTES + TAG_BYTES || length > MAX_RECORD_LENGTH)
        {
            throw new IOException("Bad record length");
        }
        byte[] record = input.readNBytes(length);
        if (record.length != length)
        {
            throw new EOFException("Truncated record");
        }
        try
        {
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, record, 0, NONCE_BYTES));
            cipher.updateAAD(aad(headerHash, index, last));
            byte[] compressed = cipher.doFinal(record, NONCE_BYTES,
                    record.length - NONCE_BYTES);
            long size = Zstd.getFrameContentSize(compressed);
            if (size < 0 || size > RECORD_BYTES)
            {
                throw new IOException("Bad record content");
            }
            return Zstd.decompress(compressed, (int) size);
        }
        catch (GeneralSecurityException rejected)
        {
            throw new IOException("Wrong key, or record " + index
                    + " damaged or out of place");
        }
    }

    /**
     * Hashes bytes with SHA-256.
     *
     * @param bytes the bytes
     * @return their hash
     */
    private static byte[] sha256(byte[] bytes)
    {
        try
        {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        }
        catch (GeneralSecurityException unavailable)
        {
            throw new IllegalStateException(unavailable);
        }
    }
}
