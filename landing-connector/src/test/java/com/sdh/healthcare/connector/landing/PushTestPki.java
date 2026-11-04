/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import java.math.BigInteger;
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
 * Certificates for the HTTP push tests: a CA, the server's certificate for
 * {@code 127.0.0.1}, and client certificates, written as PEM files where
 * the node needs them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class PushTestPki
{

    /** A certificate and its key. */
    record Issued(X509Certificate certificate, PrivateKey key)
    {
    }

    /** The CA's name. */
    private final X500Name caName;

    /** The CA. */
    final Issued ca;

    /** The server's certificate, for 127.0.0.1 and localhost. */
    final Issued server;

    /** The server's certificate chain, a PEM file. */
    final Path serverCertificate;

    /** The server's key, a PEM file. */
    final Path serverKey;

    /** The CA's certificate, a PEM file. */
    final Path caCertificate;

    /**
     * Makes a CA and the server's certificate, and writes their files.
     *
     * @param directory where the files go
     * @param name      the CA's common name
     * @throws Exception if they can't be made or written
     */
    PushTestPki(Path directory, String name) throws Exception
    {
        caName = name("CN=" + name + ",O=SDH Test");
        KeyPair caKeys = ec();
        ca = new Issued(sign(caName, caKeys, caName, caKeys.getPrivate(),
                true), caKeys.getPrivate());
        server = issue("CN=127.0.0.1,O=SDH Test", true);
        Files.createDirectories(directory);
        serverCertificate = Files.writeString(
                directory.resolve(name + "-server.crt"),
                pem("CERTIFICATE", server.certificate().getEncoded())
                        + pem("CERTIFICATE", ca.certificate().getEncoded()));
        serverKey = Files.writeString(directory.resolve(name + "-server.key"),
                pem("PRIVATE KEY", server.key().getEncoded()));
        caCertificate = Files.writeString(directory.resolve(name + "-ca.crt"),
                pem("CERTIFICATE", ca.certificate().getEncoded()));
    }

    /**
     * Issues a certificate from this CA.
     *
     * @param subject its subject
     * @param server  whether it is a server's, for 127.0.0.1 and localhost
     * @return the certificate and its key
     * @throws Exception if it can't be made
     */
    Issued issue(String subject, boolean server) throws Exception
    {
        KeyPair keys = ec();
        JcaX509v3CertificateBuilder builder = builder(name(subject), keys,
                caName);
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
     * A client's TLS context that trusts this CA's servers and, if given,
     * presents a client certificate.
     *
     * @param client the client certificate; null for none
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
        KeyManagerFactory keys = null;
        if (client != null)
        {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setKeyEntry("client", client.key(), new char[0],
                    new Certificate[] {client.certificate()});
            keys = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, new char[0]);
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys == null ? null : keys.getKeyManagers(),
                trusted.getTrustManagers(), new SecureRandom());
        return context;
    }

    /**
     * A name as RFC 2253 reads it, as the JDK and the node do. Bouncy
     * Castle's own parser takes the parts in the opposite order.
     *
     * @param name the name, such as {@code CN=producer,O=Example}
     * @return the name
     */
    static X500Name name(String name)
    {
        return X500Name.getInstance(new X500Principal(name).getEncoded());
    }

    /**
     * A new P-256 key pair.
     *
     * @return the keys
     * @throws Exception if they can't be made
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Signs a certificate.
     *
     * @param subject   its subject
     * @param keys      its keys
     * @param issuer    the issuer's name
     * @param issuerKey the issuer's key
     * @param ca        whether it is a CA
     * @return the certificate
     * @throws Exception if it can't be made
     */
    private static X509Certificate sign(X500Name subject, KeyPair keys,
            X500Name issuer, PrivateKey issuerKey, boolean ca)
            throws Exception
    {
        JcaX509v3CertificateBuilder builder = builder(subject, keys, issuer);
        if (ca)
        {
            builder.addExtension(Extension.basicConstraints, true,
                    new BasicConstraints(true));
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(
                    KeyUsage.keyCertSign | KeyUsage.cRLSign));
        }
        return new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(issuerKey)));
    }

    /**
     * A certificate builder, valid from a minute ago for a day.
     *
     * @param subject its subject
     * @param keys    its keys
     * @param issuer  the issuer's name
     * @return the builder
     */
    private static JcaX509v3CertificateBuilder builder(X500Name subject,
            KeyPair keys, X500Name issuer)
    {
        Instant now = Instant.now();
        return new JcaX509v3CertificateBuilder(issuer,
                new BigInteger(64, new SecureRandom()),
                Date.from(now.minusSeconds(60)),
                Date.from(now.plus(Duration.ofDays(1))), subject,
                keys.getPublic());
    }

    /**
     * PEM text.
     *
     * @param type the PEM type
     * @param der  the encoded object
     * @return the text
     */
    static String pem(String type, byte[] der)
    {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes())
                        .encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
