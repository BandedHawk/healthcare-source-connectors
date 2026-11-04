/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.landing.config.HttpSettings;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Tests for the token checks of {@link PushWebConfiguration}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PushWebConfigurationTest
{

    /** The issuer. */
    static final String ISSUER = "https://idp.example.org";

    /**
     * OAuth2 settings.
     *
     * @param subject the one subject accepted; null for any
     * @return the settings
     */
    static HttpSettings oauth2(String subject)
    {
        return new HttpSettings(0, HttpSettings.Auth.OAUTH2, null, null, null,
                ISSUER, null, "sdh-landing", "sdh.push", subject, null,
                Set.of(), 0, Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(1));
    }

    /**
     * A token.
     *
     * @param subject its subject
     * @return the token
     */
    static Jwt token(String subject)
    {
        Instant now = Instant.now();
        return Jwt.withTokenValue("t").header("alg", "ES256").issuer(ISSUER)
                .subject(subject).audience(List.of("sdh-landing"))
                .issuedAt(now).expiresAt(now.plusSeconds(60)).build();
    }

    /**
     * With a subject set, only tokens for it are accepted, so the intake
     * has one sender, as results need; without, any subject is.
     */
    @Test
    void acceptsOnlyTheOneSubjectWhenSet()
    {
        HttpSettings one = oauth2("producer-a");
        assertThat(one.oneSender()).isTrue();
        assertThat(PushWebConfiguration.validator(one)
                .validate(token("producer-a")).hasErrors()).isFalse();
        assertThat(PushWebConfiguration.validator(one)
                .validate(token("producer-b")).hasErrors()).isTrue();

        HttpSettings any = oauth2(null);
        assertThat(any.oneSender()).isFalse();
        assertThat(PushWebConfiguration.validator(any)
                .validate(token("producer-b")).hasErrors()).isFalse();

        HttpSettings mtls = new HttpSettings(0, HttpSettings.Auth.MTLS, null,
                null, null, null, null, null, null, null, null,
                Set.of("cn=a", "cn=b"), 0, Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofSeconds(1));
        assertThat(mtls.oneSender()).isFalse();
    }
}
