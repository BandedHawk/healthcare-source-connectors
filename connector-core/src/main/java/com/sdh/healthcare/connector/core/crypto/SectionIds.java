package com.sdh.healthcare.connector.core.crypto;

import java.security.GeneralSecurityException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** {@code sectionId = base32(HMAC-SHA-256(idKey, sectionPlaintext))} (design §7.3). */
public final class SectionIds {
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz234567".toCharArray();

    private final SecretKeySpec idKey;

    public SectionIds(byte[] idKey) {
        if (idKey.length != 32) {
            throw new IllegalArgumentException("ID key must be 256 bits");
        }
        this.idKey = new SecretKeySpec(idKey, "HmacSHA256");
    }

    public Mac newMac() {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(idKey);
            return mac;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public String of(byte[] plaintext) {
        return encode(newMac().doFinal(plaintext));
    }

    /** RFC 4648 base32, lower case, no padding. */
    public static String encode(byte[] bytes) {
        StringBuilder sb = new StringBuilder((bytes.length * 8 + 4) / 5);
        int buffer = 0;
        int bits = 0;
        for (byte b : bytes) {
            buffer = (buffer << 8) | (b & 0xff);
            bits += 8;
            while (bits >= 5) {
                sb.append(ALPHABET[(buffer >>> (bits - 5)) & 31]);
                bits -= 5;
            }
        }
        if (bits > 0) {
            sb.append(ALPHABET[(buffer << (5 - bits)) & 31]);
        }
        return sb.toString();
    }
}
