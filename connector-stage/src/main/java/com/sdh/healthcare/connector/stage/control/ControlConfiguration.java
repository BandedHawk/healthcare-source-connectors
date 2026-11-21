/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.compliance.ComplianceConfiguration;
import com.sdh.healthcare.connector.stage.admin.AuditTarget;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.servlet.DispatcherType;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.catalina.connector.Connector;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration;
import org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterAutoConfiguration;
import org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration;
import org.springframework.boot.servlet.autoconfigure.HttpEncodingAutoConfiguration;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.pem.PemSslStoreBundle;
import org.springframework.boot.ssl.pem.PemSslStoreDetails;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.SupplierJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.servlet.function.RequestPredicates;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

/**
 * A stage's control API (CONTROL-API-DESIGN.md §1, §2, §4): Tomcat on the
 * API's port with TLS, and on a plain probe port that serves only liveness
 * and readiness; Spring Security, which lets through only an authenticated
 * {@code GET}, {@code POST} or {@code DELETE} of the API's paths, by
 * OIDC bearer token or by client
 * certificate; the routes; and the API's audit journal. Every request is
 * stateless: no session, no cookie, no saved request.
 *
 * <p>Only the auto-configurations the API needs are imported, so nothing
 * else of Spring Boot's (a generated login, an error page that shows a
 * failure's message) is ever served.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@EnableWebSecurity
@EnableConfigurationProperties(ControlSettings.class)
@ImportAutoConfiguration({TomcatServletWebServerAutoConfiguration.class,
        DispatcherServletAutoConfiguration.class,
        WebMvcAutoConfiguration.class,
        HttpMessageConvertersAutoConfiguration.class,
        JacksonAutoConfiguration.class, HttpEncodingAutoConfiguration.class,
        SecurityAutoConfiguration.class,
        ServletWebSecurityAutoConfiguration.class,
        SecurityFilterAutoConfiguration.class})
@Import(ComplianceConfiguration.class)
public class ControlConfiguration
{

    /** The SSL bundle the API's server uses. */
    static final String BUNDLE = "sdh-control";

    /** The liveness probe's path. */
    static final String LIVE = "/livez";

    /** The readiness probe's path. */
    static final String READY = "/readyz";

    /** The OpenAPI description's path. */
    static final String OPENAPI = ControlRoute.PREFIX + "/openapi.yaml";

    /** Where the OpenAPI description is, on the class path. */
    static final String OPENAPI_RESOURCE = "control/openapi-v1.yaml";

    /** The methods the API serves. */
    static final Set<String> API_METHODS = Set.of("GET", "POST", "DELETE");

    /** The control API's connections to the database. */
    static final int POOL = 4;

    /** The longest a principal's name may be. */
    static final int PRINCIPAL_LIMIT = 256;

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

    /** Created by Spring. */
    public ControlConfiguration()
    {
    }

    /**
     * Sets up Tomcat: the API's port, with TLS and, for mTLS, a client
     * certificate from a trusted CA; and the probe port, plain. The
     * settings are checked first, so a control API that couldn't
     * authenticate its callers never listens.
     *
     * @param settings the control API's settings
     * @return the customizer
     */
    @Bean
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> controlServer(
            ControlSettings settings)
    {
        settings.check();
        return new ServerCustomizer(settings);
    }

    /**
     * The API's audit journal and audit export, open for the server's
     * life.
     *
     * @param commands the stage's operator commands
     * @param context  the context, for the stage's settings
     * @return the journal
     * @throws Exception if it can't be opened, when the API doesn't start
     */
    @Bean(destroyMethod = "close")
    ControlJournal controlJournal(ControlServer.Commands commands,
            ApplicationContext context) throws Exception
    {
        return ControlJournal.open(commands.cli(),
                stageSettings(commands, context));
    }

    /**
     * Serves the operations.
     *
     * @param commands the stage's operator commands
     * @param context  the context, for the stage's settings
     * @param control  the control API's settings
     * @param journal  the API's journal, open before any request
     * @param store    the idempotency keys and operation records
     * @param keys     the principals' sealing keys
     * @param results  the break-glass results
     * @return the service, running approved operations until it closes
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    ControlService controlService(ControlServer.Commands commands,
            ApplicationContext context, ControlSettings control,
            ControlJournal journal, ControlStore store, OperatorKeys keys,
            ControlResults results)
    {
        return new ControlService(commands.cli(),
                stageSettings(commands, context), control, store, keys,
                results);
    }

    /**
     * The break-glass results, held encrypted under the stage's records
     * KEK beside the stage's audit journal.
     *
     * @param controlDatabase the pool
     * @param commands        the stage's operator commands
     * @param context         the context, for the stage's settings
     * @param journal         the API's journal, which holds the stage's
     *                        keys
     * @return the results
     */
    @Bean
    ControlResults controlResults(HikariDataSource controlDatabase,
            ControlServer.Commands commands, ApplicationContext context,
            ControlJournal journal)
    {
        try (AuditTarget target = commands.cli().stage().audit()
                .apply(stageSettings(commands, context)))
        {
            return new ControlResults(controlDatabase,
                    target.tables().schema(), journal.wrap(),
                    journal.resultKek());
        }
    }

    /**
     * The principals' sealing keys, in the pipeline's database beside the
     * stage's audit journal, which every stage of the pipeline shares.
     *
     * @param controlDatabase the pool
     * @param commands        the stage's operator commands
     * @param context         the context, for the stage's settings
     * @return the keys
     */
    @Bean
    OperatorKeys operatorKeys(HikariDataSource controlDatabase,
            ControlServer.Commands commands, ApplicationContext context)
    {
        try (AuditTarget target = commands.cli().stage().audit()
                .apply(stageSettings(commands, context)))
        {
            return new OperatorKeys(controlDatabase,
                    target.tables().schema());
        }
    }

    /**
     * The control API's connections to the pipeline's database, for its
     * idempotency keys and operation records.
     *
     * @param commands the stage's operator commands
     * @param context  the context, for the stage's settings
     * @return the pool
     */
    @Bean(destroyMethod = "close")
    HikariDataSource controlDatabase(ControlServer.Commands commands,
            ApplicationContext context)
    {
        try (AuditTarget target = commands.cli().stage().audit()
                .apply(stageSettings(commands, context)))
        {
            return LeaseDatabase.dataSource(target.lease().jdbc(),
                    "sdh-control", POOL);
        }
    }

    /**
     * The idempotency keys and operation records, beside the stage's audit
     * journal.
     *
     * @param controlDatabase the pool
     * @param commands        the stage's operator commands
     * @param context         the context, for the stage's settings
     * @return the store
     */
    @Bean
    ControlStore controlStore(HikariDataSource controlDatabase,
            ControlServer.Commands commands, ApplicationContext context)
    {
        try (AuditTarget target = commands.cli().stage().audit()
                .apply(stageSettings(commands, context)))
        {
            return new ControlStore(controlDatabase,
                    target.tables().schema());
        }
    }

    /**
     * The stage's settings, bound as the stage binds them.
     *
     * @param commands the stage's operator commands
     * @param context  the context
     * @return the settings
     */
    private static Object stageSettings(ControlServer.Commands commands,
            ApplicationContext context)
    {
        return context.getBean(commands.cli().stage().settings());
    }

    /**
     * The routes: the probes, the OpenAPI description, each query and
     * action, and the operation records.
     *
     * @param service the service
     * @return the routes
     */
    @Bean
    RouterFunction<ServerResponse> controlRoutes(ControlService service)
    {
        RouterFunctions.Builder routes = RouterFunctions.route()
                .GET(LIVE, request -> ServerResponse.ok()
                        .contentType(org.springframework.http.MediaType
                                .TEXT_PLAIN).body("ok"))
                .GET(READY, request -> ServerResponse.ok()
                        .contentType(org.springframework.http.MediaType
                                .TEXT_PLAIN).body("ready"))
                .GET(OPENAPI, request -> ServerResponse.ok()
                        .contentType(org.springframework.http.MediaType
                                .parseMediaType("application/yaml"))
                        .body(new ClassPathResource(OPENAPI_RESOURCE)));
        for (ControlRoute route : ControlRoute.all())
        {
            routes.route(RequestPredicates.method(HttpMethod.valueOf(
                    route.method())).and(RequestPredicates.path(
                            route.path())),
                    request -> serve(service, route, request));
        }
        routes.GET(ControlService.OPERATIONS, request -> operations(service,
                null, request));
        routes.GET(ControlService.OPERATIONS + "/{id}", request ->
                operations(service, request.pathVariable("id"), request));
        for (ControlService.Decision decision
                : ControlService.Decision.values())
        {
            routes.POST(ControlService.OPERATIONS + "/{id}/"
                    + decision.wire(), request -> decide(service,
                            request.pathVariable("id"), decision, request));
        }
        routes.GET(ControlService.OPERATIONS + "/{id}/result", request ->
        {
            Authentication caller = caller(request);
            String id = request.pathVariable("id");
            return ServerResponse.ok().build((in, out) ->
            {
                service.result(caller, id, out);
                return null;
            });
        });
        return routes.build();
    }

    /**
     * Serves a request for the operation records.
     *
     * @param service the service
     * @param id      the operation's ID, as given; null to list
     * @param request the request
     * @return the response, written by the service
     */
    private static ServerResponse operations(ControlService service,
            String id, ServerRequest request)
    {
        Authentication caller = caller(request);
        return ServerResponse.ok().build((in, out) ->
        {
            service.operations(caller, id, in.getParameterMap(), out);
            return null;
        });
    }

    /**
     * Serves a decision on an operation that needs a second person.
     *
     * @param service  the service
     * @param id       the operation's ID, as given
     * @param decision what is asked
     * @param request  the request
     * @return the response, written by the service
     */
    private static ServerResponse decide(ControlService service, String id,
            ControlService.Decision decision, ServerRequest request)
    {
        Authentication caller = caller(request);
        return ServerResponse.ok().build((in, out) ->
        {
            byte[] body = in.getInputStream().readNBytes(
                    ControlService.MAX_DECISION + 1);
            if (body.length > ControlService.MAX_DECISION)
            {
                ControlService.send(out, ControlService.problem(413,
                        "too-large", "The body is over "
                                + ControlService.MAX_DECISION + " bytes",
                        null), null);
                return null;
            }
            service.decide(caller, id, decision, body, out);
            return null;
        });
    }

    /**
     * The authenticated caller of a request.
     *
     * @param request the request
     * @return the caller
     */
    private static Authentication caller(ServerRequest request)
    {
        return (Authentication) request.servletRequest().getUserPrincipal();
    }

    /**
     * Serves one request of a route.
     *
     * @param service the service
     * @param route   the route
     * @param request the request
     * @return the response, written by the service
     */
    private static ServerResponse serve(ControlService service,
            ControlRoute route, ServerRequest request)
    {
        Authentication caller = caller(request);
        return ServerResponse.ok().build((in, out) ->
        {
            byte[] body = in.getInputStream().readNBytes(
                    ControlService.MAX_BODY + 1);
            if (body.length > ControlService.MAX_BODY)
            {
                ControlService.send(out, ControlService.problem(413,
                        "too-large", "The body is over "
                                + ControlService.MAX_BODY + " bytes",
                        null), null);
                return null;
            }
            service.serve(new ControlService.Call(route, caller,
                    request.pathVariables(), in.getParameterMap(),
                    in.getHeader("Idempotency-Key"), in.getRequestURI(),
                    body), out);
            return null;
        });
    }

    /**
     * Checks bearer tokens: signed by the issuer's keys, from the issuer,
     * for the audience, in date, issued for no longer than allowed, and
     * naming the caller. The issuer's keys are fetched on the first
     * request, so the API starts while the issuer is down.
     *
     * @param settings the control API's settings
     * @return the decoder
     */
    static JwtDecoder jwtDecoder(ControlSettings settings)
    {
        ControlSettings.Jwt jwt = settings.auth().jwt();
        return new SupplierJwtDecoder(() ->
        {
            NimbusJwtDecoder decoder = (jwt.jwkSetUri() != null
                    && !jwt.jwkSetUri().isBlank()
                    ? NimbusJwtDecoder.withJwkSetUri(jwt.jwkSetUri())
                    : NimbusJwtDecoder.withIssuerLocation(jwt.issuerUri()))
                    .jwsAlgorithms(algorithms ->
                    {
                        algorithms.clear();
                        algorithms.addAll(SIGNATURES);
                    })
                    .build();
            decoder.setJwtValidator(validator(jwt));
            return decoder;
        });
    }

    /**
     * The checks a token must pass.
     *
     * @param jwt the token settings
     * @return the validator
     */
    static OAuth2TokenValidator<Jwt> validator(ControlSettings.Jwt jwt)
    {
        String audience = jwt.audience();
        Duration longest = jwt.maxLifetime();
        return new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefaultWithIssuer(jwt.issuerUri()),
                new JwtClaimValidator<List<String>>(JwtClaimNames.AUD,
                        named -> named != null && named.contains(audience)),
                token -> lifetime(token, longest),
                token -> principal(token.getClaimAsString(
                        jwt.principalClaim())) != null
                        ? OAuth2TokenValidatorResult.success()
                        : failure("The token names no caller"));
    }

    /**
     * Checks that a token was issued for no longer than allowed: one with
     * no issue or expiry time is refused.
     *
     * @param token   the token
     * @param longest the longest allowed
     * @return the result
     */
    static OAuth2TokenValidatorResult lifetime(Jwt token, Duration longest)
    {
        Instant issued = token.getIssuedAt();
        Instant expires = token.getExpiresAt();
        if (issued == null || expires == null)
        {
            return failure("The token has no issue or expiry time");
        }
        return Duration.between(issued, expires).compareTo(longest) > 0
                ? failure("The token was issued for too long")
                : OAuth2TokenValidatorResult.success();
    }

    /**
     * A caller's name, if it is one the audit can record: present, not
     * too long, and with no control characters.
     *
     * @param name the name
     * @return it; null if it isn't usable
     */
    static String principal(String name)
    {
        if (name == null || name.isBlank() || name.length() > PRINCIPAL_LIMIT
                || name.chars().anyMatch(Character::isISOControl))
        {
            return null;
        }
        return name;
    }

    /**
     * A token check's failure.
     *
     * @param why the reason, which names no claim's value
     * @return the result
     */
    private static OAuth2TokenValidatorResult failure(String why)
    {
        return OAuth2TokenValidatorResult.failure(
                new OAuth2Error("invalid_token", why, null));
    }

    /**
     * A token as its caller: named by the principal claim, with an
     * authority for each role in the roles claim, a list or names
     * separated by spaces.
     *
     * @param jwt the token settings
     * @return the converter
     */
    static Converter<Jwt, AbstractAuthenticationToken> tokenCaller(
            ControlSettings.Jwt jwt)
    {
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setPrincipalClaimName(jwt.principalClaim());
        converter.setJwtGrantedAuthoritiesConverter(token ->
                roles(token.getClaim(jwt.rolesClaim())));
        return converter;
    }

    /**
     * A roles claim's roles, as authorities.
     *
     * @param claim the claim: a list, or names separated by spaces
     * @return the authorities
     */
    static Collection<GrantedAuthority> roles(Object claim)
    {
        List<String> names;
        if (claim instanceof Collection<?> listed)
        {
            names = listed.stream().map(String::valueOf).toList();
        }
        else if (claim instanceof String spaced)
        {
            names = List.of(spaced.trim().split("\\s+"));
        }
        else
        {
            names = List.of();
        }
        return names.stream().filter(name -> !name.isBlank())
                .map(name -> (GrantedAuthority) new SimpleGrantedAuthority(
                        ControlService.ROLE + name))
                .toList();
    }

    /**
     * Client certificates' callers, for mTLS: each listed subject with its
     * roles. Any other subject is refused, even from a trusted CA.
     *
     * @param settings the control API's settings
     * @return the lookup
     */
    static UserDetailsService certificateCallers(ControlSettings settings)
    {
        return subject ->
        {
            List<String> roles = settings.certificateRoles(subject);
            if (roles == null || principal(subject) == null)
            {
                throw new UsernameNotFoundException("Subject not listed");
            }
            return new User(subject, "", roles.stream()
                    .map(role -> new SimpleGrantedAuthority(
                            ControlService.ROLE + role))
                    .toList());
        };
    }

    /**
     * Lets through only an authenticated request of the API's paths on
     * its port, and only the probes on the probe port. A request refused
     * for want of a credential is audited.
     *
     * @param http     the security builder
     * @param settings the control API's settings
     * @return the filter chain
     * @throws Exception if it can't be built
     */
    @Bean
    SecurityFilterChain controlSecurity(HttpSecurity http,
            ControlSettings settings) throws Exception
    {
        int probePort = settings.probePort();
        RequestMatcher onProbePort =
                request -> request.getLocalPort() == probePort;
        RequestMatcher probe = request -> onProbePort.matches(request)
                && HttpMethod.GET.matches(request.getMethod())
                && (LIVE.equals(request.getRequestURI())
                        || READY.equals(request.getRequestURI()));
        RequestMatcher api = request -> !onProbePort.matches(request)
                && API_METHODS.contains(request.getMethod())
                && request.getRequestURI().startsWith(
                        ControlRoute.PREFIX + "/");
        AuthenticationEntryPoint unauthenticated = (request, response,
                failure) -> unauthenticated(settings, failure, response);
        AccessDeniedHandler denied = (request, response, failure) ->
                ControlService.send(response, ControlService.problem(403,
                        "forbidden", null, null), null);
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(
                        SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .exceptionHandling(handling -> handling
                        .authenticationEntryPoint(unauthenticated)
                        .accessDeniedHandler(denied))
                .authorizeHttpRequests(requests -> requests
                        // An error dispatch only follows a request that was
                        // already let through or refused.
                        .dispatcherTypeMatchers(DispatcherType.ERROR)
                        .permitAll()
                        .requestMatchers(probe).permitAll()
                        .requestMatchers(api).authenticated()
                        .anyRequest().denyAll());
        if (settings.auth().mode() == ControlSettings.Mode.JWT)
        {
            ControlSettings.Jwt jwt = settings.auth().jwt();
            http.oauth2ResourceServer(server -> server
                    .authenticationEntryPoint(unauthenticated)
                    .accessDeniedHandler(denied)
                    .jwt(token -> token.decoder(jwtDecoder(settings))
                            .jwtAuthenticationConverter(tokenCaller(jwt))));
        }
        else
        {
            http.x509(x509 -> x509
                    .x509PrincipalExtractor(certificate ->
                            ControlSettings.canonical(certificate
                                    .getSubjectX500Principal().getName()))
                    .userDetailsService(certificateCallers(settings)));
        }
        return http.build();
    }

    /**
     * Refuses, and audits, a request without a valid credential. The
     * event names why by the failure's class alone.
     *
     * @param settings the control API's settings
     * @param failure  why it was refused
     * @param response where the refusal goes
     * @throws java.io.IOException if it can't be written
     */
    private static void unauthenticated(ControlSettings settings,
            AuthenticationException failure,
            jakarta.servlet.http.HttpServletResponse response)
            throws java.io.IOException
    {
        Telemetry.get().auditWarn("control.unauthenticated", Fields
                .of(Attr.AUTH, settings.auth().mode().name()
                        .toLowerCase(Locale.ROOT))
                .with(Attr.DECISION, "unauthenticated")
                .with(Attr.REASON, failure.getClass().getSimpleName())
                .with(Attr.RESULT, "refused"));
        if (settings.auth().mode() == ControlSettings.Mode.JWT)
        {
            response.setHeader("WWW-Authenticate", "Bearer");
        }
        ControlService.send(response, ControlService.problem(401,
                "unauthenticated", null, null), null);
    }

    /** Sets up Tomcat for the control API. */
    private static final class ServerCustomizer implements
            WebServerFactoryCustomizer<TomcatServletWebServerFactory>, Ordered
    {

        /** The control API's settings. */
        private final ControlSettings settings;

        /**
         * Creates the customizer.
         *
         * @param settings the control API's settings
         */
        ServerCustomizer(ControlSettings settings)
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
         * Applies the ports and TLS.
         *
         * @param factory the Tomcat factory
         */
        @Override
        public void customize(TomcatServletWebServerFactory factory)
        {
            factory.setPort(settings.port());
            ControlSettings.Tls tls = settings.tls();
            if (tls.enabled())
            {
                boolean mtls = settings.auth().mode()
                        == ControlSettings.Mode.MTLS;
                PemSslStoreDetails key = PemSslStoreDetails
                        .forCertificates(location(tls.certificate()
                                .toString()))
                        .withPrivateKey(location(tls.key().toString()))
                        .withPrivateKeyPassword(tls.keyPassword());
                PemSslStoreDetails trust = mtls
                        ? PemSslStoreDetails.forCertificates(location(
                                settings.auth().mtls().trustedCa()
                                        .toString()))
                        : null;
                factory.setSslBundles(new DefaultSslBundleRegistry(BUNDLE,
                        SslBundle.of(new PemSslStoreBundle(key, trust))));
                Ssl ssl = new Ssl();
                ssl.setEnabled(true);
                ssl.setBundle(BUNDLE);
                ssl.setClientAuth(mtls ? Ssl.ClientAuth.NEED
                        : Ssl.ClientAuth.NONE);
                factory.setSsl(ssl);
            }
            Connector probes = new Connector(
                    TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
            probes.setPort(settings.probePort());
            factory.addAdditionalConnectors(probes);
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
