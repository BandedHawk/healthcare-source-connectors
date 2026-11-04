/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static com.sdh.healthcare.connector.landing.config.SmbSettings.checkPort;
import static com.sdh.healthcare.connector.landing.config.SmbSettings.checkPositive;
import static com.sdh.healthcare.connector.landing.config.SmbSettings.required;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.x500.X500Principal;

/**
 * The HTTP push endpoint's settings, checked (design §5.3.15). The endpoint
 * is always served over TLS, and every push is authenticated: there is no
 * setting to turn either off.
 *
 * @param port         the HTTPS port; 0 picks a free one, for tests
 * @param auth         how senders are authenticated
 * @param certificate  the server's certificate chain, a PEM file
 * @param privateKey   its private key, a PEM file
 * @param privateKeyPassword the key's password; null if it isn't
 *                     encrypted
 * @param issuer       the token issuer, for {@link Auth#OAUTH2}
 * @param jwkSetUri    the issuer's key set, if it has no discovery
 *                     document; null otherwise
 * @param audience     the audience tokens must name, for
 *                     {@link Auth#OAUTH2}
 * @param scope        the scope tokens must carry, for
 *                     {@link Auth#OAUTH2}
 * @param tokenSubject the one {@code sub} tokens must carry, for
 *                     {@link Auth#OAUTH2}; null for any
 * @param trustedCas   the client CAs, a PEM file, for {@link Auth#MTLS}
 * @param subjects     the accepted client subjects, in canonical form, for
 *                     {@link Auth#MTLS}
 * @param maxItemBytes the largest body accepted; 0 for no limit
 * @param maxWait      how long a push waits for another to finish
 * @param retryAfter   the {@code Retry-After} sent with a 503
 * @param readTimeout  how long the body may stall
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record HttpSettings(int port, Auth auth, Path certificate,
        Path privateKey, String privateKeyPassword, String issuer,
        String jwkSetUri, String audience, String scope,
        String tokenSubject, Path trustedCas,
        Set<String> subjects, long maxItemBytes, Duration maxWait,
        Duration retryAfter, Duration readTimeout)
{

    /** The settings' prefix, for messages. */
    private static final String PREFIX = "sdh.landing.http.";

    /** How senders are authenticated. */
    public enum Auth
    {

        /** A bearer JWT from the configured issuer. */
        OAUTH2,

        /** A client certificate from a trusted CA, with a listed subject. */
        MTLS
    }

    /**
     * Checks the endpoint's settings.
     *
     * @param http the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if one is missing, unreadable or
     *                                  out of range
     */
    public static HttpSettings of(LandingProperties.Http http)
    {
        if (http.port() != 0)
        {
            checkPort(http.port(), PREFIX + "port");
        }
        Auth auth = switch (http.auth() == null ? ""
                : http.auth().trim().toLowerCase(Locale.ROOT))
        {
            case "oauth2" -> Auth.OAUTH2;
            case "mtls" -> Auth.MTLS;
            default -> throw new IllegalArgumentException(
                    PREFIX + "auth must be oauth2 or mtls");
        };
        LandingProperties.Tls tls = http.tls();
        Path certificate = readable(tls.certificate(),
                PREFIX + "tls.certificate");
        Path privateKey = readable(tls.privateKey(),
                PREFIX + "tls.private-key");
        String issuer = null;
        String jwkSetUri = null;
        String audience = null;
        String scope = null;
        String tokenSubject = null;
        Path trustedCas = null;
        Set<String> subjects = Set.of();
        if (auth == Auth.OAUTH2)
        {
            LandingProperties.OAuth2 oauth2 = http.oauth2();
            issuer = https(required(oauth2.issuer(),
                    PREFIX + "oauth2.issuer"), PREFIX + "oauth2.issuer");
            if (oauth2.jwkSetUri() != null && !oauth2.jwkSetUri().isBlank())
            {
                jwkSetUri = https(oauth2.jwkSetUri(),
                        PREFIX + "oauth2.jwk-set-uri");
            }
            audience = required(oauth2.audience(),
                    PREFIX + "oauth2.audience");
            scope = required(oauth2.scope(), PREFIX + "oauth2.scope");
            tokenSubject = blankToNull(oauth2.subject());
        }
        else
        {
            trustedCas = readable(http.mtls().trustedCas(),
                    PREFIX + "mtls.trusted-cas");
            subjects = subjects(http.mtls().subjects());
        }
        if (http.maxItemBytes() == null
                || http.maxItemBytes().toBytes() < 0)
        {
            throw new IllegalArgumentException(
                    PREFIX + "max-item-bytes must not be negative");
        }
        checkPositive(http.maxWait(), PREFIX + "max-wait");
        checkPositive(http.retryAfter(), PREFIX + "retry-after");
        checkPositive(http.readTimeout(), PREFIX + "read-timeout");
        return new HttpSettings(http.port(), auth, certificate, privateKey,
                blankToNull(tls.privateKeyPassword()), issuer, jwkSetUri,
                audience, scope, tokenSubject, trustedCas, subjects,
                http.maxItemBytes().toBytes(), http.maxWait(),
                http.retryAfter(), http.readTimeout());
    }

    /**
     * Tells whether only one sender is accepted: one token subject, or one
     * certificate subject. An intake's results go to one producer, so
     * results need it (design §5.3.8).
     *
     * @return true if there is one sender
     */
    public boolean oneSender()
    {
        return auth == Auth.OAUTH2 ? tokenSubject != null
                : subjects.size() == 1;
    }

    /**
     * A subject in the form the allow-list compares: canonical, so case
     * and spacing don't matter.
     *
     * @param subject a distinguished name, such as
     *                {@code CN=claims-producer,O=Example}
     * @return its canonical form
     * @throws IllegalArgumentException if it isn't a distinguished name
     */
    public static String canonicalSubject(String subject)
    {
        return new X500Principal(subject).getName(X500Principal.CANONICAL);
    }

    /**
     * Leaves out the key's password, so the settings can't leak it into a
     * log.
     *
     * @return the settings, without the password
     */
    @Override
    public String toString()
    {
        return "HttpSettings[port=" + port + ", auth=" + auth
                + ", issuer=" + issuer + ", audience=" + audience
                + ", scope=" + scope + ", subjects=" + subjects.size()
                + ", maxItemBytes=" + maxItemBytes + "]";
    }

    /**
     * Checks the allow-list of client subjects.
     *
     * @param listed the subjects as configured
     * @return their canonical forms
     * @throws IllegalArgumentException if the list is empty or a subject
     *                                  isn't a distinguished name
     */
    private static Set<String> subjects(List<String> listed)
    {
        if (listed == null || listed.isEmpty())
        {
            throw new IllegalArgumentException(PREFIX + "mtls.subjects must"
                    + " list the client certificates' subjects");
        }
        try
        {
            return listed.stream().map(HttpSettings::canonicalSubject)
                    .collect(Collectors.toUnmodifiableSet());
        }
        catch (IllegalArgumentException malformed)
        {
            throw new IllegalArgumentException(PREFIX + "mtls.subjects"
                    + " must be distinguished names, such as"
                    + " CN=producer,O=Example");
        }
    }

    /**
     * Checks that a file setting is there and readable.
     *
     * @param value   the setting
     * @param setting its name, for the message
     * @return the file
     * @throws IllegalArgumentException if it is missing or unreadable
     */
    private static Path readable(Path value, String setting)
    {
        if (value == null)
        {
            throw new IllegalArgumentException(setting + " is required");
        }
        if (!Files.isReadable(value))
        {
            throw new IllegalArgumentException(setting
                    + " can't be read");
        }
        return value;
    }

    /**
     * Checks that a URL uses HTTPS, so the issuer's keys can't be swapped
     * on the way.
     *
     * @param value   the URL
     * @param setting its name, for the message
     * @return the URL
     * @throws IllegalArgumentException if it isn't an HTTPS URL
     */
    private static String https(String value, String setting)
    {
        URI uri;
        try
        {
            uri = URI.create(value.trim());
        }
        catch (IllegalArgumentException malformed)
        {
            throw new IllegalArgumentException(setting + " must be a URL");
        }
        if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null)
        {
            throw new IllegalArgumentException(
                    setting + " must be an https URL");
        }
        return value.trim();
    }

    /**
     * Turns a blank setting into null.
     *
     * @param value the setting
     * @return the setting, or null if it is blank
     */
    private static String blankToNull(String value)
    {
        return value == null || value.isBlank() ? null : value;
    }
}
