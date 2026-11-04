/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.web;

import com.sdh.healthcare.connector.landing.config.HttpSettings;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import jakarta.servlet.DispatcherType;
import java.util.List;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.pem.PemSslStoreBundle;
import org.springframework.boot.ssl.pem.PemSslStoreDetails;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.SupplierJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * The web side of an HTTP push intake (design §5.3.15), present only when
 * the node runs a web server (see {@link WebServerSwitch}):
 * <ul>
 *   <li>Tomcat serves only HTTPS, with the configured certificate, and for
 *       mTLS requires a client certificate from a trusted CA;</li>
 *   <li>Spring Security lets through only an authenticated {@code POST} to
 *       the intake's endpoint: a JWT from the issuer, for the audience,
 *       with the scope; or a client certificate whose subject is listed;
 *       </li>
 *   <li>a push is cut off only by a stall longer than the read timeout,
 *       never by its total time.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class PushWebConfiguration
{

    /** The SSL bundle the server uses. */
    static final String BUNDLE = "sdh-push";

    /**
     * The signatures a token may have: the RSA and elliptic-curve ones an
     * issuer's public keys can check. Shared-secret and unsigned tokens are
     * never accepted.
     */
    static final Set<SignatureAlgorithm> SIGNATURES = Set.of(
            SignatureAlgorithm.RS256, SignatureAlgorithm.RS384,
            SignatureAlgorithm.RS512, SignatureAlgorithm.PS256,
            SignatureAlgorithm.PS384, SignatureAlgorithm.PS512,
            SignatureAlgorithm.ES256, SignatureAlgorithm.ES384,
            SignatureAlgorithm.ES512);

    /** The authority every accepted sender has. */
    static final String SENDER = "SDH_PUSH_SENDER";

    /**
     * The endpoint's checked settings.
     *
     * @param properties the node's settings
     * @return the checked settings
     */
    @Bean
    HttpSettings httpSettings(LandingProperties properties)
    {
        return HttpSettings.of(properties.landing().http());
    }

    /**
     * Sets up Tomcat: the port, TLS with the configured certificate, a
     * client certificate for mTLS, the read timeout, and no form parsing,
     * so no request body is read before the route sees it.
     *
     * @param settings the endpoint's settings
     * @return the customizer
     */
    @Bean
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> pushServer(
            HttpSettings settings)
    {
        return new PushServerCustomizer(settings);
    }

    /**
     * Checks bearer tokens: signed by the issuer's keys, from the issuer,
     * for the audience, in date, with a subject. The issuer's keys are
     * fetched on the first push, so the node starts while the issuer is
     * down.
     *
     * @param settings the endpoint's settings
     * @return the decoder
     */
    static JwtDecoder jwtDecoder(HttpSettings settings)
    {
        return new SupplierJwtDecoder(() ->
        {
            NimbusJwtDecoder decoder = (settings.jwkSetUri() != null
                    ? NimbusJwtDecoder.withJwkSetUri(settings.jwkSetUri())
                    : NimbusJwtDecoder.withIssuerLocation(settings.issuer()))
                    .jwsAlgorithms(algorithms ->
                    {
                        algorithms.clear();
                        algorithms.addAll(SIGNATURES);
                    })
                    .build();
            decoder.setJwtValidator(validator(settings));
            return decoder;
        });
    }

    /**
     * The checks a token must pass. With a subject set, a token for any
     * other subject is refused, so the intake has one sender.
     *
     * @param settings the endpoint's settings
     * @return the validator
     */
    static OAuth2TokenValidator<Jwt> validator(HttpSettings settings)
    {
        String audience = settings.audience();
        String only = settings.tokenSubject();
        return new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(settings.issuer()),
                new JwtClaimValidator<List<String>>(JwtClaimNames.AUD,
                        named -> named != null && named.contains(audience)),
                new JwtClaimValidator<String>(JwtClaimNames.SUB,
                        subject -> subject != null && !subject.isBlank()
                                && (only == null || only.equals(subject))));
    }

    /**
     * The accepted client certificates' subjects, for mTLS. Any other
     * subject is refused, even from a trusted CA.
     *
     * @param settings the endpoint's settings
     * @return the lookup
     */
    @Bean
    UserDetailsService listedSubjects(HttpSettings settings)
    {
        return subject ->
        {
            if (!settings.subjects().contains(subject))
            {
                throw new UsernameNotFoundException("Subject not listed");
            }
            return new User(subject, "", AuthorityUtils.createAuthorityList(
                    SENDER));
        };
    }

    /**
     * Lets through only an authenticated push to the intake's endpoint.
     * Requests are stateless: no session, no cookie, no saved request.
     *
     * @param http       the security builder
     * @param settings   the endpoint's settings
     * @param properties the node's settings
     * @param subjects   the listed subjects, for mTLS
     * @return the filter chain
     * @throws Exception if it can't be built
     */
    @Bean
    SecurityFilterChain pushSecurity(HttpSecurity http, HttpSettings settings,
            LandingProperties properties, UserDetailsService subjects)
            throws Exception
    {
        String endpoint = PushController.path(properties.landing().intake());
        String authority = settings.auth() == HttpSettings.Auth.OAUTH2
                ? "SCOPE_" + settings.scope() : SENDER;
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(
                        SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .authorizeHttpRequests(requests -> requests
                        // An error dispatch only follows a request that was
                        // already let through or refused.
                        .dispatcherTypeMatchers(DispatcherType.ERROR)
                        .permitAll()
                        .requestMatchers(HttpMethod.POST, endpoint)
                        .hasAuthority(authority)
                        .anyRequest().denyAll());
        if (settings.auth() == HttpSettings.Auth.OAUTH2)
        {
            http.oauth2ResourceServer(server -> server.jwt(jwt -> jwt
                    .decoder(jwtDecoder(settings))));
        }
        else
        {
            http.x509(x509 -> x509
                    .x509PrincipalExtractor(certificate ->
                            HttpSettings.canonicalSubject(certificate
                                    .getSubjectX500Principal().getName()))
                    .userDetailsService(subjects));
        }
        return http.build();
    }

    /** Sets up Tomcat for the push endpoint. */
    private static final class PushServerCustomizer implements
            WebServerFactoryCustomizer<TomcatServletWebServerFactory>, Ordered
    {

        /** The endpoint's settings. */
        private final HttpSettings settings;

        /**
         * Creates the customizer.
         *
         * @param settings the endpoint's settings
         */
        PushServerCustomizer(HttpSettings settings)
        {
            this.settings = settings;
        }

        /**
         * Runs after Spring Boot's own customizers, so these settings win.
         *
         * @return the lowest precedence
         */
        @Override
        public int getOrder()
        {
            return Ordered.LOWEST_PRECEDENCE;
        }

        /**
         * Applies the port, TLS and connector settings.
         *
         * @param factory the Tomcat factory
         */
        @Override
        public void customize(TomcatServletWebServerFactory factory)
        {
            factory.setPort(settings.port());
            PemSslStoreDetails key = PemSslStoreDetails
                    .forCertificates(location(settings.certificate()
                            .toString()))
                    .withPrivateKey(location(settings.privateKey()
                            .toString()))
                    .withPrivateKeyPassword(settings.privateKeyPassword());
            PemSslStoreDetails trust = settings.trustedCas() == null ? null
                    : PemSslStoreDetails.forCertificates(
                            location(settings.trustedCas().toString()));
            factory.setSslBundles(new DefaultSslBundleRegistry(BUNDLE,
                    SslBundle.of(new PemSslStoreBundle(key, trust))));
            Ssl ssl = new Ssl();
            ssl.setEnabled(true);
            ssl.setBundle(BUNDLE);
            ssl.setClientAuth(settings.auth() == HttpSettings.Auth.MTLS
                    ? Ssl.ClientAuth.NEED : Ssl.ClientAuth.NONE);
            factory.setSsl(ssl);
            int readTimeout = Math.toIntExact(
                    settings.readTimeout().toMillis());
            factory.addConnectorCustomizers(connector ->
            {
                connector.setProperty("connectionTimeout",
                        Integer.toString(readTimeout));
                // No form parsing, so no body is read before the route.
                connector.setMaxPostSize(0);
                connector.setMaxSavePostSize(0);
            });
        }

        /**
         * A file path as a resource location.
         *
         * @param path the path
         * @return the location
         */
        private static String location(String path)
        {
            return "file:" + path;
        }
    }
}
