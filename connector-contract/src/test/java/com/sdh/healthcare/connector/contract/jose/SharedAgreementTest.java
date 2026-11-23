/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.util.Base64URL;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for one key agreement shared by many messages
 * (docs/PERFORMANCE.md §13): {@link SharedAgreementEncrypter},
 * {@link SharedAgreementDecrypter}, and the providers {@link JoseCrypto}
 * chooses.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SharedAgreementTest
{

    /** The recipient's key pair. */
    KeyPair recipient;

    /** Creates the tests; JUnit runs each test on a new instance. */
    SharedAgreementTest()
    {
    }

    /**
     * Makes the recipient's key, and the default settings.
     *
     * @throws Exception if the key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        recipient = p256();
        JoseCrypto.configure(JoseCrypto.Settings.DEFAULT);
    }

    /** Puts the process's settings back as they start. */
    @AfterEach
    void tearDown()
    {
        JoseCrypto.configure(new JoseCrypto.Settings(JoseCrypto.JDK, 1,
                Duration.ofSeconds(1)));
    }

    /**
     * A new P-256 key pair.
     *
     * @return it
     * @throws Exception if it can't be made
     */
    static KeyPair p256() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Encrypts a text to the recipient.
     *
     * @param encrypter the encrypter
     * @param text      the text
     * @return the compact serialisation
     * @throws JOSEException if encryption fails
     */
    static String encrypt(com.nimbusds.jose.JWEEncrypter encrypter,
            String text) throws JOSEException
    {
        JWEObject jwe = new JWEObject(
                new JWEHeader.Builder(JWEAlgorithm.ECDH_ES_A256KW,
                        EncryptionMethod.A256GCM).keyID("rcp").build(),
                new Payload(text));
        jwe.encrypt(encrypter);
        return jwe.serialize();
    }

    /**
     * Decrypts a message.
     *
     * @param decrypter the decrypter
     * @param compact   the compact serialisation
     * @return its text
     * @throws Exception if it can't be parsed or decrypted
     */
    static String decrypt(com.nimbusds.jose.JWEDecrypter decrypter,
            String compact) throws Exception
    {
        JWEObject jwe = JWEObject.parse(compact);
        jwe.decrypt(decrypter);
        return jwe.getPayload().toString();
    }

    /**
     * The ephemeral key a message names.
     *
     * @param compact the compact serialisation
     * @return the key
     * @throws ParseException if it can't be parsed
     */
    static ECKey epk(String compact) throws ParseException
    {
        return (ECKey) JWEObject.parse(compact).getHeader()
                .getEphemeralPublicKey();
    }

    /**
     * Messages under one agreement open with the library's own decrypter,
     * one by one, and each has its own content key and IV.
     *
     * @throws Exception if a message can't be made or opened
     */
    @Test
    void messagesAreStandardJwes() throws Exception
    {
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        ECDHDecrypter standard =
                new ECDHDecrypter((ECPrivateKey) recipient.getPrivate());
        Set<String> keys = new HashSet<>();
        Set<String> ivs = new HashSet<>();
        Set<ECKey> epks = new HashSet<>();
        for (int i = 0; i < 20; i++)
        {
            String compact = encrypt(shared, "record " + i);
            assertThat(decrypt(standard, compact)).isEqualTo("record " + i);
            String[] parts = compact.split("\\.");
            keys.add(parts[1]);
            ivs.add(parts[2]);
            epks.add(epk(compact));
        }
        assertThat(keys).hasSize(20);
        assertThat(ivs).hasSize(20);
        assertThat(epks).hasSize(1);
    }

    /**
     * The caching decrypter opens the library's own messages, each with
     * its own ephemeral key, and its own shared ones.
     *
     * @throws Exception if a message can't be made or opened
     */
    @Test
    void decrypterOpensEitherKind() throws Exception
    {
        SharedAgreementDecrypter cached = new SharedAgreementDecrypter(
                (ECPrivateKey) recipient.getPrivate());
        ECDHEncrypter standard =
                new ECDHEncrypter((ECPublicKey) recipient.getPublic());
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        for (int i = 0; i < 5; i++)
        {
            assertThat(decrypt(cached, encrypt(standard, "a" + i)))
                    .isEqualTo("a" + i);
            assertThat(decrypt(cached, encrypt(shared, "b" + i)))
                    .isEqualTo("b" + i);
        }
    }

    /**
     * An agreement is replaced after its most messages.
     *
     * @throws Exception if a message can't be made
     */
    @Test
    void agreementEndsAfterItsMessages() throws Exception
    {
        JoseCrypto.configure(new JoseCrypto.Settings(JoseCrypto.JDK, 3,
                Duration.ofHours(1)));
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        ECKey[] seen = new ECKey[7];
        for (int i = 0; i < seen.length; i++)
        {
            seen[i] = epk(encrypt(shared, "x"));
        }
        assertThat(seen[0]).isEqualTo(seen[2]).isNotEqualTo(seen[3]);
        assertThat(seen[3]).isEqualTo(seen[5]).isNotEqualTo(seen[6]);
    }

    /**
     * An agreement is replaced once its lifetime is up.
     *
     * @throws Exception if a message can't be made
     */
    @Test
    void agreementEndsAfterItsLifetime() throws Exception
    {
        JoseCrypto.configure(new JoseCrypto.Settings(JoseCrypto.JDK, 1000,
                Duration.ofMillis(50)));
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        ECKey first = epk(encrypt(shared, "x"));
        assertThat(epk(encrypt(shared, "x"))).isEqualTo(first);
        Thread.sleep(80);
        assertThat(epk(encrypt(shared, "x"))).isNotEqualTo(first);
    }

    /**
     * One agreement a message, as the library's encrypter does.
     *
     * @throws Exception if a message can't be made
     */
    @Test
    void oneMessageAnAgreement() throws Exception
    {
        JoseCrypto.configure(new JoseCrypto.Settings(JoseCrypto.JDK, 1,
                Duration.ofHours(1)));
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        assertThat(epk(encrypt(shared, "x")))
                .isNotEqualTo(epk(encrypt(shared, "x")));
    }

    /**
     * A message whose ephemeral key is off the curve is refused, even
     * with a valid key's agreement kept.
     *
     * @throws Exception if a message can't be made
     */
    @Test
    void offCurveEphemeralKeyIsRefused() throws Exception
    {
        SharedAgreementDecrypter cached = new SharedAgreementDecrypter(
                (ECPrivateKey) recipient.getPrivate());
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        String good = encrypt(shared, "x");
        assertThat(decrypt(cached, good)).isEqualTo("x");
        ECKey epk = epk(good);
        byte[] y = epk.getY().decode();
        y[y.length - 1] ^= 1;
        String[] parts = good.split("\\.");
        String json = new Base64URL(parts[0]).decodeToString().replace(
                epk.getY().toString(), Base64URL.encode(y).toString());
        String forged = Base64URL.encode(json) + "." + parts[1] + "."
                + parts[2] + "." + parts[3] + "." + parts[4];
        assertThatThrownBy(() -> decrypt(cached, forged))
                .isInstanceOfAny(JOSEException.class, ParseException.class);
    }

    /**
     * A message changed under the same agreement fails its own checks.
     *
     * @throws Exception if a message can't be made
     */
    @Test
    void tamperedMessageFails() throws Exception
    {
        SharedAgreementDecrypter cached = new SharedAgreementDecrypter(
                (ECPrivateKey) recipient.getPrivate());
        SharedAgreementEncrypter shared = new SharedAgreementEncrypter(
                (ECPublicKey) recipient.getPublic());
        String one = encrypt(shared, "one");
        String two = encrypt(shared, "two");
        String[] a = one.split("\\.");
        String[] b = two.split("\\.");
        // The second message's content key with the first's content.
        String mixed = a[0] + "." + b[1] + "." + a[2] + "." + a[3] + "."
                + a[4];
        assertThatThrownBy(() -> decrypt(cached, mixed))
                .isInstanceOf(JOSEException.class);
    }

    /**
     * Each provider seals messages that the JDK opens, and opens the
     * JDK's, through the sealer and opener the pipeline uses.
     *
     * @param provider the provider
     * @throws Exception if a message can't be sealed or opened
     */
    @ParameterizedTest
    @ValueSource(strings = {JoseCrypto.JDK, JoseCrypto.ACCP,
            JoseCrypto.CONSCRYPT})
    void providersInteroperate(String provider) throws Exception
    {
        KeyPair signing = p256();
        RecordStored message = new RecordStored("ABC", UUID.randomUUID(), 7,
                100, 50, "ccda", "application/json", "ccda/1", 50, 1,
                new StorageRef("cat", "sdh_ingest", "records",
                        LocalDate.of(2026, 11, 13), 42L, "kek-1"));
        Map<String, java.security.PrivateKey> keys =
                Map.of("rcp", recipient.getPrivate());
        Map<String, ECPublicKey> signers =
                Map.of("sig", (ECPublicKey) signing.getPublic());

        JoseCrypto.configure(JoseCrypto.Settings.DEFAULT);
        byte[] byJdk = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signing.getPrivate()), "sig",
                recipient.getPublic(), "rcp").seal(message);

        JoseCrypto.configure(new JoseCrypto.Settings(provider, 100,
                Duration.ofMinutes(1)));
        assertThat(JoseCrypto.provider() == null)
                .isEqualTo(JoseCrypto.JDK.equals(provider));
        MessageSealer sealer = new MessageSealer(JoseCrypto.apply(
                new ECDSASigner((ECPrivateKey) signing.getPrivate())), "sig",
                recipient.getPublic(), "rcp");
        byte[] byProvider = sealer.seal(message);
        MessageOpener opener = new MessageOpener(keys, signers);
        assertThat(opener.open(byJdk)).isEqualTo(message);
        assertThat(opener.open(byProvider)).isEqualTo(message);
        assertThat(opener.open(sealer.seal(message))).isEqualTo(message);

        JoseCrypto.configure(JoseCrypto.Settings.DEFAULT);
        assertThat(new MessageOpener(keys, signers).open(byProvider))
                .isEqualTo(message);
        assertThat(new String(byProvider, StandardCharsets.US_ASCII))
                .startsWith("ey");
    }

    /** Unknown providers and empty agreements are refused. */
    @Test
    void settingsAreChecked()
    {
        assertThatThrownBy(() -> new JoseCrypto.Settings("bouncy", 1,
                Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JoseCrypto.Settings(JoseCrypto.JDK, 0,
                Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JoseCrypto.Settings(JoseCrypto.JDK, 1,
                Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new JoseCrypto.Settings(" ACCP ", 1,
                Duration.ofSeconds(1)).provider()).isEqualTo("accp");
    }
}
