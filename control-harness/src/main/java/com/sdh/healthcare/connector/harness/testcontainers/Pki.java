/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.testcontainers;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.PKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * The Testcontainers target's certificates and key files: one test CA,
 * its server certificates (Postgres, Kafka, the control APIs, the intake)
 * and the producer's client certificate; and the stages' encrypted
 * PKCS#8 keys. Every file is written owner-only. Test material only.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Pki
{

    /** How long the certificates are good for: the run. */
    static final Duration VALIDITY = Duration.ofDays(2);

    /**
     * A certificate and its private key.
     *
     * @param certificate the certificate
     * @param key         its private key
     */
    record Issued(X509Certificate certificate, PrivateKey key)
    {
    }

    /** The CA's name. */
    private final X500Name caName;

    /** The CA. */
    private final Issued ca;

    /** Where the files go. */
    private final Path directory;

    /**
     * Creates the CA.
     *
     * @param directory where its files go
     * @throws GeneralSecurityException if a key can't be made
     */
    Pki(Path directory) throws GeneralSecurityException
    {
        this.directory = directory;
        caName = name("CN=sdh-harness-ca,O=SDH Test");
        KeyPair keys = ec();
        try
        {
            JcaX509v3CertificateBuilder builder = builder(caName, keys);
            builder.addExtension(Extension.basicConstraints, true,
                    new BasicConstraints(true));
            builder.addExtension(Extension.keyUsage, true,
                    new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
            ca = new Issued(new JcaX509CertificateConverter().getCertificate(
                    builder.build(new JcaContentSignerBuilder(
                            "SHA256withECDSA").build(keys.getPrivate()))),
                    keys.getPrivate());
        }
        catch (java.io.IOException | OperatorCreationException failure)
        {
            throw new GeneralSecurityException("No test CA", failure);
        }
    }

    /**
     * The CA's certificate, as PEM.
     *
     * @return the PEM
     * @throws GeneralSecurityException if it can't be encoded
     */
    String caPem() throws GeneralSecurityException
    {
        return pem("CERTIFICATE", ca.certificate().getEncoded());
    }

    /**
     * Writes the CA's certificate.
     *
     * @return its file
     * @throws Exception if it can't be written
     */
    Path caFile() throws Exception
    {
        return write("ca.crt", caPem());
    }

    /**
     * Issues a server certificate.
     *
     * @param names its host names and addresses, the first its CN
     * @return the certificate and key
     * @throws GeneralSecurityException if it can't be issued
     */
    Issued server(String... names) throws GeneralSecurityException
    {
        List<GeneralName> alternatives = new ArrayList<>();
        for (String each : names)
        {
            alternatives.add(new GeneralName(address(each)
                    ? GeneralName.iPAddress : GeneralName.dNSName, each));
        }
        return issue("CN=" + names[0] + ",O=SDH Test", alternatives,
                KeyPurposeId.id_kp_serverAuth);
    }

    /**
     * Issues a client certificate.
     *
     * @param subject its subject, such as {@code CN=producer,O=Example}
     * @return the certificate and key
     * @throws GeneralSecurityException if it can't be issued
     */
    Issued client(String subject) throws GeneralSecurityException
    {
        return issue(subject, List.of(), KeyPurposeId.id_kp_clientAuth);
    }

    /**
     * Issues a certificate.
     *
     * @param subject      its subject
     * @param alternatives its subject alternative names
     * @param purpose      what it is for
     * @return the certificate and key
     * @throws GeneralSecurityException if it can't be issued
     */
    private Issued issue(String subject, List<GeneralName> alternatives,
            KeyPurposeId purpose) throws GeneralSecurityException
    {
        KeyPair keys = ec();
        try
        {
            JcaX509v3CertificateBuilder builder = builder(name(subject),
                    keys);
            if (!alternatives.isEmpty())
            {
                builder.addExtension(Extension.subjectAlternativeName, false,
                        new GeneralNames(alternatives.toArray(
                                GeneralName[]::new)));
            }
            builder.addExtension(Extension.extendedKeyUsage, false,
                    new ExtendedKeyUsage(purpose));
            return new Issued(new JcaX509CertificateConverter()
                    .getCertificate(builder.build(new JcaContentSignerBuilder(
                            "SHA256withECDSA").build(ca.key()))),
                    keys.getPrivate());
        }
        catch (java.io.IOException | OperatorCreationException failure)
        {
            throw new GeneralSecurityException("Couldn't issue " + subject,
                    failure);
        }
    }

    /**
     * A certificate and the CA's, as PEM.
     *
     * @param issued the certificate
     * @return the chain
     * @throws GeneralSecurityException if it can't be encoded
     */
    String chainPem(Issued issued) throws GeneralSecurityException
    {
        return pem("CERTIFICATE", issued.certificate().getEncoded())
                + caPem();
    }

    /**
     * A private key as unencrypted PKCS#8 PEM.
     *
     * @param issued the certificate whose key it is
     * @return the PEM
     */
    static String keyPem(Issued issued)
    {
        return pem("PRIVATE KEY", issued.key().getEncoded());
    }

    /**
     * A certificate and its key in a PKCS#12 store.
     *
     * @param issued   the certificate
     * @param password the store's password
     * @return the store
     * @throws Exception if it can't be made
     */
    byte[] keyStore(Issued issued, char[] password) throws Exception
    {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("key", issued.key(), password, new Certificate[] {
            issued.certificate(), ca.certificate()});
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.store(out, password);
        return out.toByteArray();
    }

    /**
     * A TLS context trusting the CA, with a client certificate if given.
     *
     * @param client the client's certificate; null for none
     * @return the context
     * @throws Exception if it can't be made
     */
    SSLContext context(Issued client) throws Exception
    {
        KeyStore trusted = KeyStore.getInstance("PKCS12");
        trusted.load(null, null);
        trusted.setCertificateEntry("ca", ca.certificate());
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trusted);
        KeyManagerFactory keys = null;
        if (client != null)
        {
            char[] password = "client".toCharArray();
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setKeyEntry("client", client.key(), password,
                    new Certificate[] {client.certificate(),
                        ca.certificate()});
            keys = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, password);
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys == null ? null : keys.getKeyManagers(),
                trust.getTrustManagers(), new SecureRandom());
        return context;
    }

    /**
     * Writes a stage's key pair: the private half encrypted (PBES2,
     * AES-256-CBC, HMAC-SHA256), the public half as PEM.
     *
     * @param name       the files' name, without extension
     * @param keys       the key pair
     * @param passphrase the private half's passphrase
     * @return the private half's file; the public half's is
     *         {@code <name>-public.pem} beside it
     * @throws Exception if they can't be written
     */
    Path encryptedKey(String name, KeyPair keys, String passphrase)
            throws Exception
    {
        StringWriter text = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(text))
        {
            writer.writeObject(new JcaPKCS8Generator(keys.getPrivate(),
                    new JceOpenSSLPKCS8EncryptorBuilder(
                            PKCS8Generator.AES_256_CBC)
                            .setPRF(PKCS8Generator.PRF_HMACSHA256)
                            .setIterationCount(100_000)
                            .setPassword(passphrase.toCharArray())
                            .build()));
        }
        publicKey(name + "-public", keys);
        return write(name + ".pem", text.toString());
    }

    /**
     * Writes a public key as PEM.
     *
     * @param name the file's name, without extension
     * @param keys the key pair
     * @return the file
     * @throws Exception if it can't be written
     */
    Path publicKey(String name, KeyPair keys) throws Exception
    {
        return write(name + ".pem", pem("PUBLIC KEY",
                keys.getPublic().getEncoded()));
    }

    /**
     * Writes a file, owner-only.
     *
     * @param name    its name
     * @param content its content
     * @return the file
     * @throws Exception if it can't be written
     */
    Path write(String name, String content) throws Exception
    {
        Path file = directory.resolve(name);
        Files.deleteIfExists(file);
        Files.createFile(file, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------")));
        return Files.writeString(file, content);
    }

    /**
     * A new EC P-256 key pair.
     *
     * @return the pair
     * @throws GeneralSecurityException if EC isn't available
     */
    static KeyPair ec() throws GeneralSecurityException
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Whether a name is an IP address.
     *
     * @param name the name
     * @return true if it is
     */
    private static boolean address(String name)
    {
        if (!name.matches("[0-9.]+|[0-9a-fA-F:]+:[0-9a-fA-F:]*"))
        {
            return false;
        }
        try
        {
            InetAddress.getByName(name);
            return true;
        }
        catch (UnknownHostException notOne)
        {
            return false;
        }
    }

    /**
     * An X.500 name.
     *
     * @param name the name, as RFC 2253 text
     * @return it
     */
    private static X500Name name(String name)
    {
        return X500Name.getInstance(new X500Principal(name).getEncoded());
    }

    /**
     * A certificate builder, issued by the CA.
     *
     * @param subject the subject
     * @param keys    the subject's keys
     * @return the builder
     */
    private JcaX509v3CertificateBuilder builder(X500Name subject,
            KeyPair keys)
    {
        Instant now = Instant.now();
        return new JcaX509v3CertificateBuilder(caName,
                new BigInteger(64, new SecureRandom()),
                Date.from(now.minusSeconds(60)), Date.from(now.plus(VALIDITY)),
                subject, keys.getPublic());
    }

    /**
     * DER as PEM.
     *
     * @param type the PEM type
     * @param der  the DER
     * @return the PEM
     */
    static String pem(String type, byte[] der)
    {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(US_ASCII))
                        .encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
