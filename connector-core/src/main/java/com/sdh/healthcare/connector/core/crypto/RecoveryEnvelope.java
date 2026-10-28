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
 * The local {@code failed/<fileId>.enc} fallback (design §11): a whole file encrypted to the recovery public key,
 * for when storage or the KMS is unreachable, or the file is over {@code max-file-bytes}. Only the recovery private
 * key holder can read it, so no secret is needed on the host.
 *
 * <p>Format, streaming and any size:
 * <pre>
 * header  = "SDHFB1" ‖ fileId (16) ‖ SHA-256(recovery public key) (32) ‖ u16 len ‖ ephemeral EC public key (X.509)
 * key     = HKDF-SHA256(ECDH(ephemeral, recovery), salt = SHA-256(header), info = "sdh-fallback")
 * record  = u32 len ‖ nonce (12) ‖ AES-256-GCM(zstd(plaintext)) ‖ tag
 * AAD     = SHA-256(header) ‖ u64 recordIndex ‖ u8 last
 * </pre>
 * Record 0 is a JSON metadata object (original name, size); the data follows in 8 MiB records, the final one
 * flagged {@code last}, so truncation, reordering and splicing are all detected.
 */
public final class RecoveryEnvelope {
    private static final byte[] MAGIC = "SDHFB1".getBytes(StandardCharsets.US_ASCII);
    private static final int RECORD = 8 << 20;
    private static final int NONCE = 12;
    private static final SecureRandom RANDOM = new SecureRandom();

    public record Metadata(UUID fileId, Optional<String> originalName, long size) {}

    private RecoveryEnvelope() {}

    public static void encrypt(SeekableByteSource src, UUID fileId, Optional<String> originalName,
            ECPublicKey recovery, OutputStream sink) throws IOException {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
            g.initialize(recovery.getParams());
            KeyPair eph = g.generateKeyPair();
            byte[] ephPub = eph.getPublic().getEncoded();
            byte[] header = header(fileId, sha256(recovery.getEncoded()), ephPub);
            SecretKey key = key(eph.getPrivate(), recovery, header);
            byte[] headerHash = sha256(header);

            DataOutputStream out = new DataOutputStream(sink);
            out.write(header);
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("fileId", fileId.toString());
            originalName.ifPresent(n -> meta.put("name", n));
            long size = src.size();
            meta.put("size", size);
            writeRecord(out, key, headerHash, 0, false, JSONObjectUtils.toJSONString(meta).getBytes(StandardCharsets.UTF_8));
            long records = Math.max(1, (size + RECORD - 1) / RECORD);
            for (long i = 0; i < records; i++) {
                int len = (int) Math.min(RECORD, size - i * RECORD);
                byte[] plain = src.readFully(i * RECORD, len);
                writeRecord(out, key, headerHash, i + 1, i == records - 1, plain);
                Arrays.fill(plain, (byte) 0);
            }
            out.flush();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Fallback encryption failed", e);
        }
    }

    /** Decrypts to {@code plaintext}. Throws {@link IOException} if the key is wrong or the file is damaged. */
    public static Metadata decrypt(InputStream source, PrivateKey recoveryPrivate, OutputStream plaintext)
            throws IOException {
        DataInputStream in = new DataInputStream(source);
        byte[] magic = in.readNBytes(MAGIC.length);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IOException("Not a recovery envelope");
        }
        byte[] id = in.readNBytes(16);
        byte[] keyHash = in.readNBytes(32);
        int ephLen = in.readUnsignedShort();
        byte[] ephPub = in.readNBytes(ephLen);
        if (id.length != 16 || keyHash.length != 32 || ephPub.length != ephLen) {
            throw new EOFException("Truncated header");
        }
        ByteBuffer b = ByteBuffer.wrap(id);
        UUID fileId = new UUID(b.getLong(), b.getLong());
        byte[] header = header(fileId, keyHash, ephPub);
        byte[] headerHash = sha256(header);
        SecretKey key;
        try {
            ECPublicKey eph = (ECPublicKey) KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(ephPub));
            key = key(recoveryPrivate, eph, header);
        } catch (GeneralSecurityException e) {
            throw new IOException("Unusable key or header");
        }
        Map<String, Object> meta;
        try {
            meta = JSONObjectUtils.parse(new String(readRecord(in, key, headerHash, 0, false), StandardCharsets.UTF_8));
        } catch (ParseException e) {
            throw new IOException("Damaged metadata");
        }
        long size = ((Number) meta.get("size")).longValue();
        long records = Math.max(1, (size + RECORD - 1) / RECORD);
        long written = 0;
        for (long i = 0; i < records; i++) {
            byte[] p = readRecord(in, key, headerHash, i + 1, i == records - 1);
            plaintext.write(p);
            written += p.length;
        }
        if (written != size || in.read() != -1) {
            throw new IOException("Size mismatch or trailing data");
        }
        return new Metadata(fileId, Optional.ofNullable((String) meta.get("name")), size);
    }

    private static byte[] header(UUID fileId, byte[] keyHash, byte[] ephPub) {
        return ByteBuffer.allocate(MAGIC.length + 16 + 32 + 2 + ephPub.length)
                .put(MAGIC).putLong(fileId.getMostSignificantBits()).putLong(fileId.getLeastSignificantBits())
                .put(keyHash).putShort((short) ephPub.length).put(ephPub).array();
    }

    private static SecretKey key(PrivateKey priv, java.security.PublicKey pub, byte[] header)
            throws GeneralSecurityException {
        KeyAgreement ka = KeyAgreement.getInstance("ECDH");
        ka.init(priv);
        ka.doPhase(pub, true);
        byte[] secret = ka.generateSecret();
        try {
            return KDF.getInstance("HKDF-SHA256").deriveKey("AES", HKDFParameterSpec.ofExtract().addIKM(secret)
                    .addSalt(sha256(header)).thenExpand("sdh-fallback".getBytes(StandardCharsets.US_ASCII), 32));
        } finally {
            Arrays.fill(secret, (byte) 0);
        }
    }

    private static byte[] aad(byte[] headerHash, long index, boolean last) {
        return ByteBuffer.allocate(32 + 8 + 1).put(headerHash).putLong(index).put((byte) (last ? 1 : 0)).array();
    }

    private static void writeRecord(DataOutputStream out, SecretKey key, byte[] headerHash, long index, boolean last,
            byte[] plain) throws IOException, GeneralSecurityException {
        byte[] nonce = new byte[NONCE];
        RANDOM.nextBytes(nonce);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        c.updateAAD(aad(headerHash, index, last));
        byte[] ct = c.doFinal(Zstd.compress(plain, 3));
        out.writeInt(NONCE + ct.length);
        out.write(nonce);
        out.write(ct);
    }

    private static byte[] readRecord(DataInputStream in, SecretKey key, byte[] headerHash, long index, boolean last)
            throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException e) {
            throw new EOFException("Truncated: record " + index + " missing");
        }
        if (len < NONCE + 16 || len > RECORD + (RECORD >> 4) + 4096) {
            throw new IOException("Bad record length");
        }
        byte[] rec = in.readNBytes(len);
        if (rec.length != len) {
            throw new EOFException("Truncated record");
        }
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, rec, 0, NONCE));
            c.updateAAD(aad(headerHash, index, last));
            byte[] compressed = c.doFinal(rec, NONCE, rec.length - NONCE);
            long size = Zstd.getFrameContentSize(compressed);
            if (size < 0 || size > RECORD) {
                throw new IOException("Bad record content");
            }
            return Zstd.decompress(compressed, (int) size);
        } catch (GeneralSecurityException e) {
            throw new IOException("Wrong key, or record " + index + " damaged or out of place");
        }
    }

    private static byte[] sha256(byte[] b) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(b);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
