/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.Base64;
import java.util.Date;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * The demo's certificates for the HTTP push intake: a CA, the landing
 * node's server certificate for 127.0.0.1, and client certificates the
 * producers present (mTLS). Made fresh for each run, valid for a day.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class DemoPki
{

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

    /** The server's certificate chain, as PEM. */
    final Path serverCertificate;

    /** The server's private key, as PEM. */
    final Path serverKey;

    /** The CA's certificate, as PEM: what the node trusts clients from. */
    final Path caCertificate;

    /**
     * Makes the CA and the server's certificate, and writes their files.
     *
     * @param directory where to write them
     * @throws Exception if they can't be made or written
     */
    DemoPki(Path directory) throws Exception
    {
        caName = name("CN=sdh-demo-ca,O=SDH Demo");
        KeyPair caKeys = ec();
        JcaX509v3CertificateBuilder caBuilder = builder(caName, caKeys);
        caBuilder.addExtension(Extension.basicConstraints, true,
                new BasicConstraints(true));
        caBuilder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        ca = new Issued(new JcaX509CertificateConverter().getCertificate(
                caBuilder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(caKeys.getPrivate()))), caKeys.getPrivate());
        Issued server = issue("CN=127.0.0.1,O=SDH Demo", true);
        Files.createDirectories(directory);
        serverCertificate = Files.writeString(directory.resolve("server.crt"),
                pem("CERTIFICATE", server.certificate().getEncoded())
                        + pem("CERTIFICATE", ca.certificate().getEncoded()));
        serverKey = Files.writeString(directory.resolve("server.key"),
                pem("PRIVATE KEY", server.key().getEncoded()));
        caCertificate = Files.writeString(directory.resolve("ca.crt"),
                pem("CERTIFICATE", ca.certificate().getEncoded()));
    }

    /**
     * Issues a certificate from the CA.
     *
     * @param subject its subject, such as {@code CN=producer,O=Example}
     * @param server  whether it is for the server, with 127.0.0.1 and
     *                localhost as its names
     * @return the certificate and key
     * @throws Exception if it can't be made
     */
    Issued issue(String subject, boolean server) throws Exception
    {
        KeyPair keys = ec();
        JcaX509v3CertificateBuilder builder = builder(name(subject), keys);
        if (server)
        {
            builder.addExtension(Extension.subjectAlternativeName, false,
                    new GeneralNames(new GeneralName[] {
                        new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
                        new GeneralName(GeneralName.dNSName, "localhost")}));
        }
        return new Issued(new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(ca.key()))), keys.getPrivate());
    }

    /**
     * A client's TLS context: it trusts the CA, and presents a
     * certificate.
     *
     * @param client the certificate to present
     * @return the context
     * @throws Exception if it can't be made
     */
    SSLContext clientContext(Issued client) throws Exception
    {
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", ca.certificate());
        TrustManagerFactory trusted = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trusted.init(trust);
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("client", client.key(), new char[0],
                new Certificate[] {client.certificate()});
        KeyManagerFactory keys = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, new char[0]);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), trusted.getTrustManagers(),
                new SecureRandom());
        return context;
    }

    /**
     * A distinguished name, as Java writes it.
     *
     * @param name the name
     * @return the name
     */
    static X500Name name(String name)
    {
        return X500Name.getInstance(new X500Principal(name).getEncoded());
    }

    /**
     * A P-256 key pair.
     *
     * @return the pair
     * @throws Exception if P-256 is missing
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * A certificate builder, issued by the CA, valid from a minute ago for
     * a day.
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
                Date.from(now.minusSeconds(60)),
                Date.from(now.plus(Duration.ofDays(1))), subject,
                keys.getPublic());
    }

    /**
     * Encodes as PEM.
     *
     * @param type the PEM type, such as {@code CERTIFICATE}
     * @param der  the encoded bytes
     * @return the PEM text
     */
    static String pem(String type, byte[] der)
    {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64,
                        "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
