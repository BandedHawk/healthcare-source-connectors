/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;
import com.nimbusds.jose.jca.JCAAware;
import com.nimbusds.jose.jca.JCAContext;
import java.security.Provider;
import java.time.Duration;
import java.util.Locale;
import org.conscrypt.Conscrypt;

/**
 * How messages are sealed and opened, for the process (DESIGN.md §7.4,
 * docs/PERFORMANCE.md §13): which JCA provider does their elliptic-curve
 * work, and how many messages share one key agreement. Set once at
 * startup, from {@code sdh.crypto.jose}, before any message is sealed;
 * until then the JDK's providers and one agreement for each message.
 * <p>
 * The provider does the P-256 work only (key generation, ECDH and ES256),
 * which is nearly all a message's cost; AES key wrap and AES-GCM stay with
 * the JDK, which every provider can stand beside. Choices:
 * <ul>
 * <li>{@code jdk}: the JDK's own, in Java; the default, and the one for
 * any platform without a native provider;</li>
 * <li>{@code accp}: Amazon Corretto Crypto Provider (AWS-LC, native), for
 * AWS, and for any Linux x86_64 or arm64 host;</li>
 * <li>{@code conscrypt}: Google's Conscrypt (BoringSSL, native), for
 * Google Cloud, and for Azure, which has no Java provider of its
 * own.</li>
 * </ul>
 * A native provider that can't load stops the process at startup rather
 * than fall back without saying so.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class JoseCrypto
{

    /** The JDK's providers. */
    public static final String JDK = "jdk";

    /** Amazon Corretto Crypto Provider. */
    public static final String ACCP = "accp";

    /** Conscrypt. */
    public static final String CONSCRYPT = "conscrypt";

    /**
     * How a process seals and opens messages.
     *
     * @param provider            {@link #JDK}, {@link #ACCP} or
     *                            {@link #CONSCRYPT}
     * @param agreementMessages   the most messages one key agreement (one
     *                            ephemeral key) encrypts; 1 for an
     *                            agreement a message
     * @param agreementLifetime   the longest one key agreement is used for
     */
    public record Settings(String provider, int agreementMessages,
            Duration agreementLifetime)
    {

        /**
         * The defaults: the JDK's providers, an agreement for up to 10,000
         * messages or 60 seconds.
         */
        public static final Settings DEFAULT =
                new Settings(JDK, 10_000, Duration.ofSeconds(60));

        /**
         * Checks the settings.
         *
         * @throws IllegalArgumentException if a limit isn't positive or
         *                                  the provider isn't known
         */
        public Settings
        {
            provider = provider == null ? JDK
                    : provider.trim().toLowerCase(Locale.ROOT);
            if (!provider.equals(JDK) && !provider.equals(ACCP)
                    && !provider.equals(CONSCRYPT))
            {
                throw new IllegalArgumentException("sdh.crypto.jose.provider"
                        + " must be jdk, accp or conscrypt");
            }
            if (agreementMessages < 1 || agreementLifetime == null
                    || agreementLifetime.isNegative()
                    || agreementLifetime.isZero())
            {
                throw new IllegalArgumentException("A key agreement must"
                        + " cover at least one message, for some time");
            }
        }
    }

    /** The settings in force. */
    private static volatile Settings settings =
            new Settings(JDK, 1, Duration.ofSeconds(1));

    /** The provider for the elliptic-curve work; null for the JDK's. */
    private static volatile Provider provider;

    /** Not instantiated: this class only has static methods. */
    private JoseCrypto()
    {
    }

    /**
     * Sets how messages are sealed and opened, loading the provider.
     *
     * @param chosen the settings
     * @throws IllegalStateException if the provider can't be loaded
     */
    public static synchronized void configure(Settings chosen)
    {
        provider = load(chosen.provider());
        settings = chosen;
    }

    /**
     * The settings in force.
     *
     * @return them
     */
    public static Settings settings()
    {
        return settings;
    }

    /**
     * The provider for the elliptic-curve work.
     *
     * @return it; null for the JDK's
     */
    public static Provider provider()
    {
        return provider;
    }

    /**
     * Has a signer or verifier use the chosen provider.
     *
     * @param <T>  its type
     * @param jose the signer or verifier
     * @return it
     */
    public static <T extends JCAAware<JCAContext>> T apply(T jose)
    {
        Provider chosen = provider;
        if (chosen != null)
        {
            jose.getJCAContext().setProvider(chosen);
        }
        return jose;
    }

    /**
     * Loads a provider, and checks that its native library works.
     *
     * @param name its name
     * @return it; null for the JDK's
     * @throws IllegalStateException if it can't be loaded
     */
    private static Provider load(String name)
    {
        try
        {
            return switch (name)
            {
                case ACCP ->
                {
                    AmazonCorrettoCryptoProvider accp =
                            AmazonCorrettoCryptoProvider.INSTANCE;
                    accp.assertHealthy();
                    yield accp;
                }
                case CONSCRYPT ->
                {
                    Conscrypt.checkAvailability();
                    yield Conscrypt.newProvider();
                }
                default -> null;
            };
        }
        catch (RuntimeException | LinkageError unavailable)
        {
            throw new IllegalStateException("The " + name + " crypto"
                    + " provider can't be loaded on this platform; set"
                    + " sdh.crypto.jose.provider to jdk", unavailable);
        }
    }
}
