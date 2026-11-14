/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jca.JCAContext;
import com.nimbusds.jose.util.Base64URL;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Signs a stage's messages with a key of this pod's own (docs/PERFORMANCE.md
 * §3.5), so the stage's KMS key signs one statement per pod per half
 * lifetime instead of every message. The key pair is made in memory and
 * never leaves it; the statement ({@link PodStatement}), signed by the
 * stage's key, vouches for it for {@code lifetime} (1 hour by default).
 * A new key and statement are made at half the lifetime; if that fails, as
 * when the KMS is down, the current ones are used until they expire.
 * <p>
 * {@link MessageSealer} puts the statement and the signing time in each
 * message's protected header; {@link MessageOpener} checks both.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PodSigner implements JWSSigner
{

    /** The usual lifetime of a pod's key. */
    public static final Duration DEFAULT_LIFETIME = Duration.ofHours(1);

    /**
     * What a message is signed with now.
     *
     * @param signer    signs with the pod's key
     * @param statement the statement vouching for the key, as a compact JWS
     * @param id        the statement's ID
     * @param notAfter  when the statement stops vouching for the key
     */
    public record Signing(ECDSASigner signer, String statement, String id,
            Instant notAfter)
    {
    }

    /** Signs the statements, for the stage. */
    private final JWSSigner stage;

    /** The stage key's ID. */
    private final String stageKid;

    /** The stage the pod signs for. */
    private final String subject;

    /** The pod's name, for audit. */
    private final String pod;

    /** How long a key is vouched for. */
    private final Duration lifetime;

    /** Gives the times statements and messages are stamped with. */
    private final Clock clock;

    /** Told of each new statement, to log its ID for revocation. */
    private final Consumer<PodStatement> onStatement;

    /** The JCA context, for the signer interface. */
    private final JCAContext jca = new JCAContext();

    /** What messages are signed with now; null until the first. */
    private Signing current;

    /** When to make a new key: half the lifetime. */
    private Instant renewAt;

    /**
     * Creates a signer; its first key is made on first use.
     *
     * @param stage       signs the statements for the stage, such as
     *                    through its KMS
     * @param stageKid    the stage key's ID, which readers trust
     * @param subject     the stage the pod signs for
     * @param pod         the pod's name, for audit
     * @param lifetime    how long each key is vouched for; at least a
     *                    minute
     * @param clock       gives the times
     * @param onStatement told of each new statement, such as to log its ID
     *                    for revocation
     * @throws IllegalArgumentException if the lifetime is under a minute
     */
    public PodSigner(JWSSigner stage, String stageKid, String subject,
            String pod, Duration lifetime, Clock clock,
            Consumer<PodStatement> onStatement)
    {
        if (lifetime.compareTo(Duration.ofMinutes(1)) < 0)
        {
            throw new IllegalArgumentException(
                    "A pod key's lifetime is at least a minute");
        }
        this.stage = stage;
        this.stageKid = stageKid;
        this.subject = subject;
        this.pod = pod;
        this.lifetime = lifetime;
        this.clock = clock;
        this.onStatement = onStatement;
    }

    /**
     * What to sign a message with now, making a new key first if the
     * current one is past half its lifetime.
     *
     * @return the signer and its statement
     * @throws IllegalStateException if there is no key that is still
     *                               vouched for, and a new one couldn't be
     *                               vouched for
     */
    public synchronized Signing current()
    {
        Instant now = clock.instant();
        if (current == null || !now.isBefore(renewAt))
        {
            try
            {
                renew(now);
            }
            catch (RuntimeException failure)
            {
                // Kept until it expires: a KMS outage of less than half the
                // lifetime goes unnoticed.
                if (current == null || now.isAfter(current.notAfter()))
                {
                    throw failure;
                }
            }
        }
        return current;
    }

    /**
     * The time to stamp a message with as its signing time.
     *
     * @return now, to the second
     */
    public Instant now()
    {
        return clock.instant().truncatedTo(ChronoUnit.SECONDS);
    }

    /**
     * Makes a new key pair and has the stage vouch for it.
     *
     * @param now the time now
     */
    private void renew(Instant now)
    {
        KeyPair pair;
        try
        {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            pair = generator.generateKeyPair();
        }
        catch (GeneralSecurityException unavailable)
        {
            throw new IllegalStateException("No EC P-256 key generator",
                    unavailable);
        }
        Instant notBefore = now.truncatedTo(ChronoUnit.SECONDS);
        Instant notAfter = notBefore.plus(lifetime);
        PodStatement statement = new PodStatement(
                UUID.randomUUID().toString(), subject, pod,
                (ECPublicKey) pair.getPublic(), notBefore, notAfter);
        String compact = statement.sign(stage, stageKid);
        try
        {
            current = new Signing(
                    new ECDSASigner((ECPrivateKey) pair.getPrivate()),
                    compact, statement.id(), notAfter);
        }
        catch (JOSEException unusable)
        {
            throw new IllegalStateException("Unusable pod key", unusable);
        }
        renewAt = notBefore.plus(lifetime.dividedBy(2));
        onStatement.accept(statement);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Signs with the pod's current key. A message signed this way verifies
     * only with its statement, which {@link MessageSealer} adds.
     */
    @Override
    public Base64URL sign(JWSHeader header, byte[] signingInput)
            throws JOSEException
    {
        return current().signer().sign(header, signingInput);
    }

    @Override
    public Set<JWSAlgorithm> supportedJWSAlgorithms()
    {
        return Set.of(JWSAlgorithm.ES256);
    }

    @Override
    public JCAContext getJCAContext()
    {
        return jca;
    }
}
