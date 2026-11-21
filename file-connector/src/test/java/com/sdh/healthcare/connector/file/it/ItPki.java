/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
 * A test CA, and the server certificates it issues, for services run over
 * TLS in the integration tests. Made fresh for each run, valid for a day.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ItPki
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

    /**
     * Makes a CA.
     *
     * @param name the CA's common name
     * @throws Exception if it can't be made
     */
    ItPki(String name) throws Exception
    {
        caName = name("CN=" + name + ",O=SDH Test");
        KeyPair keys = ec();
        JcaX509v3CertificateBuilder builder = builder(caName, keys);
        builder.addExtension(Extension.basicConstraints, true,
                new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        ca = new Issued(new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(keys.getPrivate()))), keys.getPrivate());
    }

    /**
     * The CA's certificate, as PEM: what a client trusts.
     *
     * @return the PEM text
     * @throws Exception if it can't be encoded
     */
    String caPem() throws Exception
    {
        return pem("CERTIFICATE", ca.certificate().getEncoded());
    }

    /**
     * Issues a server certificate for the names given: each an IP address
     * or a DNS name.
     *
     * @param names the server's names; the first is its common name
     * @return the certificate and key
     * @throws Exception if it can't be made
     */
    Issued server(String... names) throws Exception
    {
        KeyPair keys = ec();
        JcaX509v3CertificateBuilder builder = builder(
                name("CN=" + names[0] + ",O=SDH Test"), keys);
        List<GeneralName> alternatives = new ArrayList<>();
        for (String each : names)
        {
            alternatives.add(new GeneralName(isAddress(each)
                    ? GeneralName.iPAddress : GeneralName.dNSName, each));
        }
        builder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(alternatives.toArray(GeneralName[]::new)));
        return new Issued(new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA")
                        .build(ca.key()))), keys.getPrivate());
    }

    /**
     * A server's certificate chain, as PEM: its own, then the CA's.
     *
     * @param server the server's certificate and key
     * @return the PEM text
     * @throws Exception if it can't be encoded
     */
    String chainPem(Issued server) throws Exception
    {
        return pem("CERTIFICATE", server.certificate().getEncoded()) + caPem();
    }

    /**
     * A server's private key, as unencrypted PKCS #8 PEM.
     *
     * @param server the server's certificate and key
     * @return the PEM text
     */
    static String keyPem(Issued server)
    {
        return pem("PRIVATE KEY", server.key().getEncoded());
    }

    /**
     * A PKCS #12 key store holding a server's key and its chain.
     *
     * @param server   the server's certificate and key
     * @param password the store's password
     * @return the store's bytes
     * @throws Exception if it can't be written
     */
    byte[] keyStore(Issued server, char[] password) throws Exception
    {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("server", server.key(), password,
                new Certificate[] {server.certificate(), ca.certificate()});
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.store(out, password);
        return out.toByteArray();
    }

    /**
     * Whether a name is an IP address, written as one.
     *
     * @param name the name
     * @return whether it is
     */
    private static boolean isAddress(String name)
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
     * A distinguished name, as Java writes it.
     *
     * @param name the name
     * @return the name
     */
    private static X500Name name(String name)
    {
        return X500Name.getInstance(new X500Principal(name).getEncoded());
    }

    /**
     * A P-256 key pair.
     *
     * @return the pair
     * @throws Exception if P-256 is missing
     */
    private static KeyPair ec() throws Exception
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
