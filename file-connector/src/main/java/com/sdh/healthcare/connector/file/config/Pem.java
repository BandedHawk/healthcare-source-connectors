package com.sdh.healthcare.connector.file.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/** Minimal PEM reading with the JDK only: public keys and encrypted PKCS#8 private keys (PBES2, as OpenSSL writes). */
final class Pem {
    private static final Pattern BLOCK = Pattern.compile("-----BEGIN ([A-Z ]+)-----([^-]+)-----END \\1-----");

    private Pem() {}

    static byte[] block(Path file, String type) throws IOException {
        Matcher m = BLOCK.matcher(Files.readString(file, StandardCharsets.US_ASCII));
        while (m.find()) {
            if (m.group(1).equals(type)) {
                return Base64.getMimeDecoder().decode(m.group(2));
            }
        }
        throw new IOException("No " + type + " block in " + file.getFileName());
    }

    static PublicKey publicKey(Path file) throws IOException, GeneralSecurityException {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(block(file, "PUBLIC KEY"));
        try {
            return KeyFactory.getInstance("EC").generatePublic(spec);
        } catch (InvalidKeySpecException notEc) {
            return KeyFactory.getInstance("RSA").generatePublic(spec);
        }
    }

    static PrivateKey encryptedPrivateKey(Path file, char[] passphrase) throws IOException, GeneralSecurityException {
        String text = Files.readString(file, StandardCharsets.US_ASCII);
        if (!text.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----")) {
            throw new GeneralSecurityException("Private key " + file.getFileName()
                    + " must be an encrypted PKCS#8 PEM (BEGIN ENCRYPTED PRIVATE KEY)");
        }
        EncryptedPrivateKeyInfo epki = new EncryptedPrivateKeyInfo(block(file, "ENCRYPTED PRIVATE KEY"));
        AlgorithmParameters params = epki.getAlgParameters();
        // For PBES2 the parameters name the real scheme, e.g. PBEWithHmacSHA256AndAES_256.
        String alg = params != null ? params.toString() : epki.getAlgName();
        SecretKey pbe = SecretKeyFactory.getInstance(alg).generateSecret(new PBEKeySpec(passphrase));
        Cipher c = Cipher.getInstance(alg);
        c.init(Cipher.DECRYPT_MODE, pbe, params);
        PKCS8EncodedKeySpec spec;
        try {
            spec = epki.getKeySpec(c);
        } catch (InvalidKeySpecException e) {
            throw new GeneralSecurityException("Wrong passphrase for " + file.getFileName());
        }
        return KeyFactory.getInstance("EC").generatePrivate(spec);
    }
}
