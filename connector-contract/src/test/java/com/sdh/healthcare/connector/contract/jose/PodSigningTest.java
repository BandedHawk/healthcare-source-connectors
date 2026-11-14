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
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jca.JCAContext;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException.Reason;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for pod signing keys (docs/PERFORMANCE.md §3.5): {@link PodSigner},
 * {@link PodStatement}, and how {@link MessageSealer} and
 * {@link MessageOpener} use them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PodSigningTest
{

    /** The stage key's ID. */
    static final String STAGE_KID = "stage-1";

    /** The recipient key's ID. */
    static final String RECIPIENT_KID = "rcp-1";

    /** The stage's key pair, as its KMS would hold it. */
    KeyPair stageKeys;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** Signs for the stage, counting calls and failing when told. */
    CountingSigner stage;

    /** The time the pod signer sees. */
    MovableClock clock;

    /** The statements the pod signer has made. */
    List<PodStatement> made;

    /** The message the tests seal. */
    RecordStored message;

    /** Creates the tests; JUnit runs each test on a new instance. */
    PodSigningTest()
    {
    }

    /** A clock a test moves on. */
    static final class MovableClock extends Clock
    {

        /** The time now. */
        Instant now = Instant.parse("2026-11-13T12:00:00.250Z");

        /** Creates the clock at a fixed start. */
        MovableClock()
        {
        }

        /**
         * Moves the clock on.
         *
         * @param by how far
         */
        void advance(Duration by)
        {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone()
        {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return this;
        }

        @Override
        public Instant instant()
        {
            return now;
        }
    }

    /** Signs with a key as a KMS would, counting calls; may be made to fail. */
    static final class CountingSigner implements JWSSigner
    {

        /** Signs with the key. */
        final ECDSASigner signer;

        /** How many times it has signed. */
        int calls;

        /** Whether it fails, as a KMS outage would. */
        boolean down;

        /**
         * Creates a signer.
         *
         * @param key the private key
         * @throws JOSEException if the key is unusable
         */
        CountingSigner(ECPrivateKey key) throws JOSEException
        {
            signer = new ECDSASigner(key);
        }

        @Override
        public Base64URL sign(JWSHeader header, byte[] signingInput)
                throws JOSEException
        {
            if (down)
            {
                throw new JOSEException("KMS unavailable");
            }
            calls++;
            return signer.sign(header, signingInput);
        }

        @Override
        public Set<JWSAlgorithm> supportedJWSAlgorithms()
        {
            return signer.supportedJWSAlgorithms();
        }

        @Override
        public JCAContext getJCAContext()
        {
            return signer.getJCAContext();
        }
    }

    /**
     * Makes an EC P-256 key pair.
     *
     * @return the key pair
     * @throws Exception if the JDK has no such generator
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Makes the keys, the clock and the pod signer's parts.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        stageKeys = ec();
        recipient = ec();
        stage = new CountingSigner((ECPrivateKey) stageKeys.getPrivate());
        clock = new MovableClock();
        made = new ArrayList<>();
        message = new RecordStored("ABC", UUID.randomUUID(), 7, 100, 50,
                "ccda", "application/json", "ccda/1", 50, 1,
                new StorageRef("cat", "sdh_ingest", "records",
                        LocalDate.of(2026, 11, 13), 42L, "kek-1"));
    }

    /**
     * A pod signer with a one-hour lifetime.
     *
     * @return the signer
     */
    PodSigner pod()
    {
        return new PodSigner(stage, STAGE_KID, STAGE_KID, "split-0",
                Duration.ofHours(1), clock, made::add);
    }

    /**
     * A sealer that signs with a signer.
     *
     * @param signer the signer
     * @return the sealer
     */
    MessageSealer sealer(JWSSigner signer)
    {
        return new MessageSealer(signer, STAGE_KID, recipient.getPublic(),
                RECIPIENT_KID);
    }

    /**
     * An opener trusting the stage's key.
     *
     * @param revoked the statement IDs it refuses
     * @return the opener
     */
    MessageOpener opener(Set<String> revoked)
    {
        return new MessageOpener(Map.of(RECIPIENT_KID, recipient.getPrivate()),
                Map.of(STAGE_KID, (ECPublicKey) stageKeys.getPublic()),
                revoked);
    }

    /**
     * Why an opener refuses a message.
     *
     * @param opener the opener
     * @param body   the message
     * @return the reason
     */
    static Reason refusal(MessageOpener opener, byte[] body)
    {
        try
        {
            opener.open(body);
        }
        catch (MessageRejectedException rejected)
        {
            return rejected.reason();
        }
        throw new AssertionError("The message was opened");
    }

    /**
     * Seals a message by hand, signed with a pod key under a statement,
     * claiming a signing time the pod signer wouldn't give.
     *
     * @param signing  what the pod signs with
     * @param signedAt the claimed signing time
     * @return the message
     * @throws Exception if it can't be sealed
     */
    byte[] sealAt(PodSigner.Signing signing, Instant signedAt)
            throws Exception
    {
        JWSObject jws = new JWSObject(
                new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(STAGE_KID)
                        .type(MessageSealer.JWS_TYPE)
                        .customParam(MessageSealer.STATEMENT_PARAM,
                                signing.statement())
                        .customParam(MessageSealer.SIGNED_AT_PARAM,
                                signedAt.getEpochSecond())
                        .build(),
                new Payload(JSONObjectUtils.toJSONString(
                        PayloadCodec.toJson(message))));
        jws.sign(signing.signer());
        JWEObject jwe = new JWEObject(
                new JWEHeader.Builder(JWEAlgorithm.ECDH_ES_A256KW,
                        EncryptionMethod.A256GCM).keyID(RECIPIENT_KID)
                        .contentType(MessageSealer.JWE_CONTENT_TYPE).build(),
                new Payload(jws));
        jwe.encrypt(new ECDHEncrypter((ECPublicKey) recipient.getPublic()));
        return jwe.serialize().getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Messages a pod signs open with only the stage's public key, and the
     * stage's key signs once for all of them, not once per message.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void podSignedMessagesOpenAndTheStageSignsOnce() throws Exception
    {
        MessageSealer sealer = sealer(pod());
        MessageOpener opener = opener(Set.of());
        for (int index = 0; index < 20; index++)
        {
            ConnectorMessage opened = opener.open(sealer.seal(message));
            assertThat(opened).isEqualTo(message);
        }
        assertThat(stage.calls).isEqualTo(1);
        assertThat(made).hasSize(1);
        assertThat(made.getFirst().subject()).isEqualTo(STAGE_KID);
        assertThat(made.getFirst().pod()).isEqualTo("split-0");
    }

    /**
     * A new key is made at half the lifetime, and messages signed under
     * the old one still open later, as when they wait in a queue.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void renewsAtHalfLifeAndOldMessagesStillOpen() throws Exception
    {
        PodSigner pod = pod();
        MessageSealer sealer = sealer(pod);
        byte[] early = sealer.seal(message);
        clock.advance(Duration.ofMinutes(29));
        sealer.seal(message);
        assertThat(made).hasSize(1);
        clock.advance(Duration.ofMinutes(2));
        sealer.seal(message);
        assertThat(made).hasSize(2);
        assertThat(made.get(1).id()).isNotEqualTo(made.get(0).id());

        clock.advance(Duration.ofDays(3));
        assertThat(opener(Set.of()).open(early)).isEqualTo(message);
    }

    /**
     * If the stage can't sign a new statement, as in a KMS outage, the
     * current key is used until it expires, and then signing fails.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keepsItsKeyThroughAnOutageUntilItExpires() throws Exception
    {
        PodSigner pod = pod();
        MessageSealer sealer = sealer(pod);
        sealer.seal(message);
        stage.down = true;
        clock.advance(Duration.ofMinutes(59));
        byte[] late = sealer.seal(message);
        assertThat(opener(Set.of()).open(late)).isEqualTo(message);
        assertThat(made).hasSize(1);

        clock.advance(Duration.ofMinutes(2));
        assertThatThrownBy(() -> sealer.seal(message))
                .isInstanceOf(IllegalStateException.class);

        stage.down = false;
        assertThat(opener(Set.of()).open(sealer.seal(message)))
                .isEqualTo(message);
        assertThat(made).hasSize(2);
    }

    /**
     * Messages under a revoked statement are refused; those under the
     * pod's next statement are not.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesARevokedStatement() throws Exception
    {
        PodSigner pod = pod();
        MessageSealer sealer = sealer(pod);
        byte[] before = sealer.seal(message);
        clock.advance(Duration.ofMinutes(31));
        byte[] after = sealer.seal(message);
        MessageOpener opener = opener(Set.of(made.getFirst().id()));
        assertThat(refusal(opener, before)).isEqualTo(Reason.REVOKED_SIGNER);
        assertThat(opener.open(after)).isEqualTo(message);
    }

    /**
     * A message claiming to be signed outside its statement's validity is
     * refused, before or after it, and so is one with no signing time.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesASigningTimeOutsideTheStatement() throws Exception
    {
        PodSigner pod = pod();
        PodSigner.Signing signing = pod.current();
        PodStatement statement = made.getFirst();
        MessageOpener opener = opener(Set.of());

        assertThat(opener.open(sealAt(signing, statement.notAfter())))
                .isEqualTo(message);
        assertThat(refusal(opener, sealAt(signing,
                statement.notAfter().plusSeconds(1))))
                .isEqualTo(Reason.STATEMENT_INVALID);
        assertThat(refusal(opener, sealAt(signing,
                statement.notBefore().minusSeconds(1))))
                .isEqualTo(Reason.STATEMENT_INVALID);
    }

    /**
     * A statement the stage didn't sign is refused, even with a trusted
     * key ID: a pod key can't vouch for itself.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesAStatementTheStageDidNotSign() throws Exception
    {
        KeyPair stranger = ec();
        PodSigner forged = new PodSigner(
                new ECDSASigner((ECPrivateKey) stranger.getPrivate()),
                STAGE_KID, STAGE_KID, "intruder", Duration.ofHours(1),
                clock, made::add);
        assertThat(refusal(opener(Set.of()), sealer(forged).seal(message)))
                .isEqualTo(Reason.BAD_SIGNATURE);
    }

    /**
     * A message signed with someone else's key under a real statement is
     * refused: the statement binds the one pod key.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesAnotherKeyUnderARealStatement() throws Exception
    {
        PodSigner.Signing real = pod().current();
        KeyPair stranger = ec();
        PodSigner.Signing swapped = new PodSigner.Signing(
                new ECDSASigner((ECPrivateKey) stranger.getPrivate()),
                real.statement(), real.id(), real.notAfter());
        assertThat(refusal(opener(Set.of()),
                sealAt(swapped, clock.instant())))
                .isEqualTo(Reason.BAD_SIGNATURE);
    }

    /**
     * A message signed with the stage's key itself, as before pod keys,
     * still opens.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void stageSignedMessagesStillOpen() throws Exception
    {
        assertThat(opener(Set.of()).open(sealer(stage).seal(message)))
                .isEqualTo(message);
    }

    /**
     * A statement survives being signed and verified, and names the key
     * it was made for.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aStatementRoundTrips() throws Exception
    {
        PodStatement statement = new PodStatement("id-1", STAGE_KID, "p-1",
                (ECPublicKey) ec().getPublic(),
                Instant.parse("2026-11-13T12:00:00Z"),
                Instant.parse("2026-11-13T13:00:00Z"));
        String compact = statement.sign(stage, STAGE_KID);
        assertThat(PodStatement.verify(compact, STAGE_KID,
                (ECPublicKey) stageKeys.getPublic())).isEqualTo(statement);
        assertThatThrownBy(() -> PodStatement.verify(compact, "other",
                (ECPublicKey) stageKeys.getPublic()))
                .isInstanceOf(MessageRejectedException.class);
        assertThat(statement.covers(statement.notBefore())).isTrue();
        assertThat(statement.covers(statement.notAfter().plusSeconds(1)))
                .isFalse();
    }

    /** A lifetime under a minute is refused. */
    @Test
    void refusesAShortLifetime()
    {
        assertThatThrownBy(() -> new PodSigner(stage, STAGE_KID, STAGE_KID,
                "p", Duration.ofSeconds(59), clock, made::add))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
