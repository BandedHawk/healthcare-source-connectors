/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The rules of the compliance profiles (design §7.6), applied to a node's
 * settings as names and values, with nothing read from the network. Only
 * setting names and check IDs ever leave this class: a value can hold a
 * password or a token, so none is put in a finding or a message.
 *
 * <p>The settings are Spring's canonical names ({@code sdh.lease.jdbc.url},
 * however they were set) of everything under {@code sdh.},
 * {@code camel.component.} and {@code spring.rabbitmq.}. Endpoints are
 * recognised by their values, so a setting added later is checked too.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ComplianceRules
{

    /** The profile setting. */
    public static final String PROFILE = "sdh.compliance.profile";

    /** Where waivers are set, each {@code <prefix><check id>}. */
    public static final String WAIVE = "sdh.compliance.waive.";

    /** The longest waiver reason accepted. */
    public static final int MAX_REASON = 200;

    /** The audit export's endpoint, which may be plain HTTP. */
    static final String OTLP_ENDPOINT = "sdh.telemetry.otlp.endpoint";

    /** Whether the durable audit journal is on; {@code true} if unset. */
    static final String JOURNAL_ENABLED = "sdh.telemetry.journal.enabled";

    /** The settings naming who holds the keys; {@code local} if unset. */
    private static final List<String> KEY_PROVIDERS = List.of(
            "sdh.crypto.wrap-provider", "sdh.crypto.sign-provider");

    /**
     * The settings the rules read by name, which a node must look up by
     * these canonical names: set as environment variables, their names
     * can't be told apart from names with more dots.
     *
     * @return the names, waivers included
     */
    public static List<String> namedSettings()
    {
        List<String> names = new ArrayList<>(List.of(PROFILE, OTLP_ENDPOINT,
                JOURNAL_ENABLED, KAFKA_COMPONENT_PROTOCOL, RABBIT_SSL));
        names.addAll(KEY_PROVIDERS);
        for (ComplianceCheck check : ComplianceCheck.values())
        {
            names.add(WAIVE + check.id());
        }
        return names;
    }

    /** Kafka's own TLS setting, for every endpoint of the component. */
    private static final String KAFKA_COMPONENT_PROTOCOL =
            "camel.component.kafka.security-protocol";

    /**
     * RabbitMQ's TLS setting, for Spring's connection factory, which
     * sending uses; receiving sets {@code rabbitmq.ssl} beside its URI.
     */
    private static final String RABBIT_SSL = "spring.rabbitmq.ssl.enabled";

    /** Kafka security protocols that encrypt. */
    private static final Set<String> KAFKA_TLS = Set.of("ssl", "sasl_ssl");

    /** Postgres SSL modes that verify the server. */
    private static final Set<String> VERIFIED_SSL_MODES =
            Set.of("verify-ca", "verify-full");

    /** Camel components that stay in the process: no transport. */
    private static final Set<String> IN_PROCESS =
            Set.of("direct", "seda", "vm", "stub", "mock", "disruptor");

    /** Camel components whose transport is set elsewhere, and checked. */
    private static final Set<String> CHECKED_ELSEWHERE =
            Set.of("jms", "activemq", "amqp", "azure-servicebus",
                    "azure-eventhubs");

    /** A Camel endpoint URI's component. */
    private static final Pattern COMPONENT =
            Pattern.compile("^([a-z0-9][a-z0-9+.-]*):(?!//)");

    /** The JVM options that must be off, and those that must be on. */
    private static final Map<String, String> JVM_OPTIONS = jvmOptions();

    /** Not instantiated: the rules are static. */
    private ComplianceRules()
    {
    }

    /**
     * What a check found: one setting, or JVM option, it refuses.
     *
     * @param check   the check
     * @param setting the setting's name, never its value
     */
    public record Finding(ComplianceCheck check, String setting)
    {

        /**
         * The finding as a line of a message.
         *
         * @return the check's ID, the setting and what it means
         */
        public String describe()
        {
            return check.id() + ": " + setting + " (" + check.meaning() + ")";
        }
    }

    /**
     * What the profile made of the settings.
     *
     * @param profile the profile in force
     * @param refused what it refuses: if any, the node must not start
     * @param waived  what it found but an operator waived, with the reason
     */
    public record Outcome(ComplianceProfile profile, List<Finding> refused,
            Map<Finding, String> waived)
    {

        /**
         * Copies the lists, so the outcome can't change.
         */
        public Outcome
        {
            refused = List.copyOf(refused);
            waived = Collections.unmodifiableMap(
                    new LinkedHashMap<>(waived));
        }

        /**
         * Whether the node may start.
         *
         * @return true if nothing is refused
         */
        public boolean passed()
        {
            return refused.isEmpty();
        }
    }

    /**
     * Applies the profile the settings choose.
     *
     * @param settings  the settings, by canonical name
     * @param jvmOption a JVM option's value by name (as
     *                  {@code HotSpotDiagnosticMXBean} gives it); empty if
     *                  the JVM doesn't say
     * @return what the profile found
     * @throws IllegalArgumentException if the profile is unknown, or a
     *                                  waiver names no check or gives no
     *                                  reason
     */
    public static Outcome evaluate(Map<String, String> settings,
            Function<String, Optional<String>> jvmOption)
    {
        ComplianceProfile profile = ComplianceProfile.parse(
                settings.getOrDefault(PROFILE, ""));
        Map<ComplianceCheck, String> waivers = waivers(settings);
        if (profile == ComplianceProfile.NONE)
        {
            return new Outcome(profile, List.of(), Map.of());
        }
        List<Finding> refused = new ArrayList<>();
        Map<Finding, String> waived = new LinkedHashMap<>();
        for (Finding finding : findings(settings, jvmOption))
        {
            String reason = waivers.get(finding.check());
            if (reason == null)
            {
                refused.add(finding);
            }
            else
            {
                waived.put(finding, reason);
            }
        }
        return new Outcome(profile, refused, waived);
    }

    /**
     * The waivers set, each checked.
     *
     * @param settings the settings
     * @return each waived check's reason
     * @throws IllegalArgumentException if a waiver names no check, or its
     *                                  reason is blank, too long or holds
     *                                  control characters
     */
    static Map<ComplianceCheck, String> waivers(Map<String, String> settings)
    {
        Map<ComplianceCheck, String> waivers = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : settings.entrySet())
        {
            if (!entry.getKey().startsWith(WAIVE))
            {
                continue;
            }
            String id = entry.getKey().substring(WAIVE.length());
            ComplianceCheck check = ComplianceCheck.byId(id)
                    .orElseThrow(() -> new IllegalArgumentException(
                            entry.getKey() + " names no check; the checks"
                            + " are " + checkIds()));
            String reason = entry.getValue() == null ? ""
                    : entry.getValue().strip();
            if (reason.isEmpty() || reason.length() > MAX_REASON
                    || reason.chars().anyMatch(Character::isISOControl))
            {
                throw new IllegalArgumentException(entry.getKey()
                        + " must give the reason the check is waived: one"
                        + " line of at most " + MAX_REASON + " characters");
            }
            waivers.put(check, reason);
        }
        return waivers;
    }

    /**
     * Everything the {@code hipaa} profile's checks find, waived or not.
     *
     * @param settings  the settings
     * @param jvmOption a JVM option's value by name
     * @return the findings, in a stable order
     */
    static List<Finding> findings(Map<String, String> settings,
            Function<String, Optional<String>> jvmOption)
    {
        List<Finding> findings = new ArrayList<>();
        for (Map.Entry<String, String> entry : settings.entrySet())
        {
            String name = entry.getKey();
            if (name.startsWith("sdh.compliance.") || entry.getValue() == null)
            {
                continue;
            }
            endpoint(name, entry.getValue().strip(), settings)
                    .ifPresent(check -> findings.add(new Finding(check, name)));
            if (name.endsWith(".no-auth")
                    && "true".equalsIgnoreCase(entry.getValue().strip()))
            {
                findings.add(new Finding(ComplianceCheck.STORAGE_NO_AUTH,
                        name));
            }
        }
        for (String provider : KEY_PROVIDERS)
        {
            if ("local".equalsIgnoreCase(
                    settings.getOrDefault(provider, "local").strip()))
            {
                findings.add(new Finding(ComplianceCheck.KEYS_LOCAL,
                        provider));
            }
        }
        if (settings.getOrDefault(OTLP_ENDPOINT, "").isBlank())
        {
            findings.add(new Finding(ComplianceCheck.AUDIT_EXPORT,
                    OTLP_ENDPOINT));
        }
        if (!Boolean.parseBoolean(
                settings.getOrDefault(JOURNAL_ENABLED, "true").trim()))
        {
            findings.add(new Finding(ComplianceCheck.AUDIT_JOURNAL,
                    JOURNAL_ENABLED));
        }
        JVM_OPTIONS.forEach((option, wanted) ->
        {
            Optional<String> value = jvmOption.apply(option);
            if (value.isEmpty() || !value.get().equalsIgnoreCase(wanted))
            {
                findings.add(new Finding(ComplianceCheck.JVM_DUMPS,
                        "-XX:" + option));
            }
        });
        return findings;
    }

    /**
     * What is wrong with a setting, if it is an endpoint.
     *
     * @param name     the setting's name
     * @param value    its value
     * @param settings all the settings, for options set elsewhere
     * @return the check it fails; empty if it isn't an endpoint, or is one
     *         that passes
     */
    static Optional<ComplianceCheck> endpoint(String name, String value,
            Map<String, String> settings)
    {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.contains("defaultendpointsprotocol=http;")
                || lower.contains("usedevelopmentemulator=true"))
        {
            return Optional.of(ComplianceCheck.TRANSPORT_PLAINTEXT);
        }
        if (lower.startsWith("http://") || lower.startsWith("ws://"))
        {
            // The audit export holds no PHI by construction (design §12),
            // so a collector beside the node may be plain HTTP.
            return name.equals(OTLP_ENDPOINT) ? Optional.empty()
                    : Optional.of(ComplianceCheck.TRANSPORT_PLAINTEXT);
        }
        if (lower.startsWith("amqp://"))
        {
            return Optional.of(ComplianceCheck.TRANSPORT_PLAINTEXT);
        }
        if (lower.startsWith("tcp://"))
        {
            // An Artemis core URL: TLS is an option of the URL.
            return "true".equals(options(lower).get("sslenabled"))
                    ? Optional.empty()
                    : Optional.of(ComplianceCheck.TRANSPORT_PLAINTEXT);
        }
        if (lower.startsWith("jdbc:"))
        {
            return database(name, lower, settings);
        }
        if (!name.endsWith(".uri"))
        {
            return Optional.empty();
        }
        Matcher component = COMPONENT.matcher(lower);
        return component.find()
                ? broker(name, component.group(1), lower, settings)
                : Optional.empty();
    }

    /**
     * What is wrong with a JDBC URL.
     *
     * @param name     the setting's name
     * @param url      the URL, in lower case
     * @param settings all the settings, for a catalog's {@code jdbc.sslmode}
     * @return the check it fails; empty if it passes
     */
    private static Optional<ComplianceCheck> database(String name, String url,
            Map<String, String> settings)
    {
        if (!url.startsWith("jdbc:postgresql:"))
        {
            return Optional.of(ComplianceCheck.TRANSPORT_UNCHECKED);
        }
        String mode = options(url).get("sslmode");
        if (mode == null)
        {
            // An Iceberg JDBC catalog passes its jdbc.* properties to the
            // driver, beside the URI.
            String parent = name.substring(0, name.lastIndexOf('.') + 1);
            String sibling = settings.get(parent + "jdbc.sslmode");
            mode = sibling == null ? null
                    : sibling.strip().toLowerCase(Locale.ROOT);
        }
        return mode != null && VERIFIED_SSL_MODES.contains(mode)
                ? Optional.empty()
                : Optional.of(ComplianceCheck.TRANSPORT_DATABASE);
    }

    /**
     * What is wrong with a Camel endpoint URI.
     *
     * @param name      the setting's name, for options set beside it, such
     *                  as a receiving RabbitMQ connection's {@code ssl}
     * @param component the URI's component, such as {@code kafka}
     * @param uri       the URI, in lower case
     * @param settings  all the settings, for options set on the component
     * @return the check it fails; empty if it passes
     */
    private static Optional<ComplianceCheck> broker(String name,
            String component, String uri, Map<String, String> settings)
    {
        String parent = name.substring(0, name.lastIndexOf('.') + 1);
        if (IN_PROCESS.contains(component)
                || CHECKED_ELSEWHERE.contains(component))
        {
            return Optional.empty();
        }
        Map<String, String> options = options(uri);
        Boolean passes = switch (component)
        {
            case "kafka" -> KAFKA_TLS.contains(options.getOrDefault(
                    "securityprotocol",
                    settings.getOrDefault(KAFKA_COMPONENT_PROTOCOL, "")
                            .strip().toLowerCase(Locale.ROOT)));
            case "aws2-sqs" -> !options.containsKey("uriendpointoverride")
                    || options.get("uriendpointoverride")
                            .startsWith("https://");
            case "google-pubsub" ->
                    !"false".equals(options.get("authenticate"));
            case "spring-rabbitmq" -> "true".equalsIgnoreCase(
                    settings.getOrDefault(RABBIT_SSL, "").strip())
                    || "true".equalsIgnoreCase(settings.getOrDefault(
                            parent + "rabbitmq.ssl", "").strip());
            default -> null;
        };
        if (passes == null)
        {
            return Optional.of(ComplianceCheck.TRANSPORT_UNCHECKED);
        }
        return passes ? Optional.empty()
                : Optional.of(ComplianceCheck.TRANSPORT_BROKER);
    }

    /**
     * A URI's query options, by lower-case name; values not decoded.
     *
     * @param uri the URI, in lower case
     * @return the options; empty if it has no query
     */
    static Map<String, String> options(String uri)
    {
        Map<String, String> options = new LinkedHashMap<>();
        int query = uri.indexOf('?');
        if (query < 0)
        {
            return options;
        }
        for (String pair : uri.substring(query + 1).split("[&;]"))
        {
            int equals = pair.indexOf('=');
            if (equals > 0)
            {
                options.put(pair.substring(0, equals),
                        pair.substring(equals + 1));
            }
        }
        return options;
    }

    /**
     * The JVM options the profile requires (design §14.1), with the value
     * each must have.
     *
     * @return the options
     */
    private static Map<String, String> jvmOptions()
    {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("HeapDumpOnOutOfMemoryError", "false");
        options.put("CreateCoredumpOnCrash", "false");
        options.put("ErrorFile", "/dev/null");
        options.put("DisableAttachMechanism", "true");
        return Collections.unmodifiableMap(options);
    }

    /**
     * Every check's ID, for messages.
     *
     * @return the IDs, comma-separated
     */
    static String checkIds()
    {
        List<String> ids = new ArrayList<>();
        for (ComplianceCheck check : ComplianceCheck.values())
        {
            ids.add(check.id());
        }
        return String.join(", ", ids);
    }
}
