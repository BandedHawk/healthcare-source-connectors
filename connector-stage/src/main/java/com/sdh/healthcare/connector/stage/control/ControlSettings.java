/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.stage.admin.Permission;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.x500.X500Principal;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The control API's settings, {@code sdh.control.*}
 * (CONTROL-API-DESIGN.md §2). Nothing is granted unless
 * {@code permissions} grants it, and the API never starts without a way to
 * authenticate its callers.
 *
 * @param port        the API's port, over TLS
 * @param probePort   the plain port that serves only liveness and
 *                    readiness, for Kubernetes
 * @param tls         the API's TLS
 * @param auth        how callers are authenticated
 * @param permissions each permission's roles, by the permission's name
 *                    ({@code pipeline.view}); in YAML, a name in brackets
 *                    ({@code "[pipeline.view]"}) keeps its dot
 * @param approval    how long an operation needing four eyes may wait
 *                    for its approval
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh.control")
public record ControlSettings(@DefaultValue("8443") int port,
        @DefaultValue("8081") int probePort, @DefaultValue Tls tls,
        @DefaultValue Auth auth, Map<String, List<String>> permissions,
        @DefaultValue Approval approval)
{

    /** The setting that turns a stage's process into its control mode. */
    public static final String MODE = "sdh.control.mode";

    /**
     * The API's TLS.
     *
     * @param enabled     whether the API serves TLS; the {@code hipaa}
     *                    profile refuses it off
     * @param certificate the server's certificate chain, PEM
     * @param key         its private key, PEM
     * @param keyPassword the key's password, if it has one
     */
    public record Tls(@DefaultValue("true") boolean enabled, Path certificate,
            Path key, String keyPassword)
    {

        /**
         * Leaves out the key's password, so the settings can't leak it
         * into a log.
         *
         * @return the settings, without the password
         */
        @Override
        public String toString()
        {
            return "Tls[enabled=" + enabled + ", certificate=" + certificate
                    + ", key=" + key + "]";
        }
    }

    /**
     * How callers are authenticated.
     *
     * @param mode how: by token or by client certificate
     * @param jwt  the token's checks
     * @param mtls the certificates' checks
     */
    public record Auth(@DefaultValue("jwt") Mode mode, @DefaultValue Jwt jwt,
            @DefaultValue Mtls mtls)
    {
    }

    /** How callers are authenticated. */
    public enum Mode
    {

        /** An OIDC bearer token. */
        JWT,

        /** A client certificate. */
        MTLS
    }

    /**
     * An OIDC bearer token's checks.
     *
     * @param issuerUri      the issuer the token must name, and where its
     *                       keys are found unless {@code jwkSetUri} says
     * @param jwkSetUri      where the issuer's keys are, if not found
     *                       from the issuer
     * @param audience       the audience the token must name
     * @param principalClaim the claim that names the caller
     * @param rolesClaim     the claim that holds the caller's roles: a
     *                       list, or names separated by spaces
     * @param maxLifetime    the longest a token may be issued for
     */
    public record Jwt(String issuerUri, String jwkSetUri, String audience,
            @DefaultValue("sub") String principalClaim,
            @DefaultValue("groups") String rolesClaim,
            @DefaultValue("1h") Duration maxLifetime)
    {
    }

    /**
     * Client certificates' checks.
     *
     * @param trustedCa the CA certificates a client's must be from, PEM
     * @param roles     each certificate subject's roles, by its subject's
     *                  name; in YAML, a name in brackets
     */
    public record Mtls(Path trustedCa, Map<String, List<String>> roles)
    {
    }

    /**
     * How long an operation that needs a second person may wait
     * (CONTROL-API-DESIGN.md §6).
     *
     * @param ttl           how long a request may wait for its approval
     *                      before it expires
     * @param breakGlassTtl how long a break-glass request may wait, and
     *                      how long its result is held for its requester
     *                      once it has run
     */
    public record Approval(@DefaultValue("24h") Duration ttl,
            @DefaultValue("1h") Duration breakGlassTtl)
    {
    }

    /**
     * Checks the settings, as the API starts.
     *
     * @throws IllegalStateException if the API couldn't authenticate its
     *                               callers, or serve TLS as asked, or a
     *                               permission is unknown
     */
    public void check()
    {
        List<String> wrong = new ArrayList<>();
        if (tls.enabled() && (tls.certificate() == null
                || tls.key() == null))
        {
            wrong.add("sdh.control.tls.certificate and .key are required");
        }
        if (auth.mode() == Mode.JWT)
        {
            if (blank(auth.jwt().issuerUri()))
            {
                wrong.add("sdh.control.auth.jwt.issuer-uri is required");
            }
            if (blank(auth.jwt().audience()))
            {
                wrong.add("sdh.control.auth.jwt.audience is required");
            }
            if (!auth.jwt().maxLifetime().isPositive())
            {
                wrong.add("sdh.control.auth.jwt.max-lifetime must be"
                        + " positive");
            }
        }
        else
        {
            if (!tls.enabled())
            {
                wrong.add("client certificates need"
                        + " sdh.control.tls.enabled");
            }
            if (auth.mtls().trustedCa() == null)
            {
                wrong.add("sdh.control.auth.mtls.trusted-ca is required");
            }
        }
        if (permissions != null)
        {
            for (String name : permissions.keySet())
            {
                if (permission(name) == null)
                {
                    wrong.add("sdh.control.permissions names no permission "
                            + name + "; the permissions are " + Arrays
                                    .stream(Permission.values())
                                    .map(Permission::key).toList());
                }
            }
        }
        if (!approval.ttl().isPositive()
                || !approval.breakGlassTtl().isPositive()
                || approval.breakGlassTtl().compareTo(approval.ttl()) > 0)
        {
            wrong.add("sdh.control.approval.ttl and .break-glass-ttl must"
                    + " be positive, the break-glass TTL no longer");
        }
        if (port == probePort)
        {
            wrong.add("sdh.control.probe-port must differ from"
                    + " sdh.control.port");
        }
        if (!wrong.isEmpty())
        {
            throw new IllegalStateException("The control API can't start: "
                    + String.join("; ", wrong));
        }
    }

    /**
     * The roles each permission is granted to.
     *
     * @return the roles, by permission; a permission no role is granted
     *         is absent
     */
    public Map<Permission, Set<String>> grants()
    {
        Map<Permission, Set<String>> grants = new EnumMap<>(Permission.class);
        if (permissions != null)
        {
            permissions.forEach((name, roles) ->
            {
                Permission permission = permission(name);
                if (permission != null && roles != null)
                {
                    grants.computeIfAbsent(permission,
                            any -> new LinkedHashSet<>()).addAll(roles);
                }
            });
        }
        return grants;
    }

    /**
     * A certificate subject's roles.
     *
     * @param subject the subject's name, canonical
     * @return its roles; null if it isn't listed
     */
    public List<String> certificateRoles(String subject)
    {
        Map<String, List<String>> listed = auth.mtls().roles();
        if (listed == null)
        {
            return null;
        }
        for (Map.Entry<String, List<String>> entry : listed.entrySet())
        {
            if (canonical(entry.getKey()).equals(subject))
            {
                return entry.getValue() == null ? List.of()
                        : entry.getValue();
            }
        }
        return null;
    }

    /**
     * A certificate subject's name, canonical, so the names settings give
     * and certificates carry compare equal.
     *
     * @param subject the name
     * @return it, canonical
     */
    public static String canonical(String subject)
    {
        return new X500Principal(subject).getName(X500Principal.CANONICAL);
    }

    /**
     * The permission a name in settings names.
     *
     * @param name the name, such as {@code pipeline.view}
     * @return the permission; null if it names none
     */
    private static Permission permission(String name)
    {
        for (Permission permission : Permission.values())
        {
            if (permission.key().equals(name))
            {
                return permission;
            }
        }
        return null;
    }

    /**
     * Whether text is missing.
     *
     * @param text the text
     * @return true if it is null or blank
     */
    private static boolean blank(String text)
    {
        return text == null || text.isBlank();
    }
}
