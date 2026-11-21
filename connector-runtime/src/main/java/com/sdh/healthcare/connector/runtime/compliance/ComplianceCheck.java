/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import java.util.Locale;
import java.util.Optional;

/**
 * The checks of the {@code hipaa} compliance profile (docs/HIPAA.md §8.1,
 * design §7.6). Each can be waived on its own, with a reason, as
 * {@code sdh.compliance.waive.<id>}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum ComplianceCheck
{

    /**
     * A plaintext endpoint: {@code http://}, {@code ws://},
     * {@code amqp://}, Artemis without TLS, an Azure connection string over
     * HTTP or to a development emulator (45 CFR 164.312(e)).
     */
    TRANSPORT_PLAINTEXT("transport-plaintext",
            "a plaintext endpoint"),

    /**
     * A Postgres connection that doesn't verify the server's certificate:
     * {@code sslmode} must be {@code verify-ca} or {@code verify-full}
     * (45 CFR 164.312(d), (e)).
     */
    TRANSPORT_DATABASE("transport-database",
            "a database connection without verified TLS"),

    /**
     * A broker reached without TLS: Kafka without {@code SSL} or
     * {@code SASL_SSL}, RabbitMQ without {@code spring.rabbitmq.ssl}, SQS
     * at a plaintext endpoint, Pub/Sub without authentication
     * (45 CFR 164.312(d), (e)).
     */
    TRANSPORT_BROKER("transport-broker",
            "a broker connection without TLS or authentication"),

    /**
     * A connection the profile can't judge: a Camel component or a JDBC
     * driver it doesn't know.
     */
    TRANSPORT_UNCHECKED("transport-unchecked",
            "a connection the profile can't check"),

    /**
     * Keys held in a local keystore or PEM file rather than a KMS
     * (45 CFR 164.312(a)(2)(iv); 74 FR 42742, keys kept apart from the
     * data).
     */
    KEYS_LOCAL("keys-local", "keys held locally, not in a KMS"),

    /**
     * No audit export: audit events would reach only the console log
     * (45 CFR 164.312(b)).
     */
    AUDIT_EXPORT("audit-export", "no audit export"),

    /**
     * No durable audit journal: audit events would be kept only as long
     * as the export and console log keep them (45 CFR 164.312(b),
     * 164.414(b)).
     */
    AUDIT_JOURNAL("audit-journal", "no durable audit journal"),

    /**
     * Object storage reached without authentication.
     */
    STORAGE_NO_AUTH("storage-no-auth",
            "object storage without authentication"),

    /**
     * A JVM that may write memory, and so PHI, to disk or let a tool read
     * it: heap dumps, core dumps, an {@code hs_err} file, or the attach
     * mechanism (design §7.5, §14.1).
     */
    JVM_DUMPS("jvm-dumps", "a JVM that may dump memory");

    /** The check's ID, as waivers name it. */
    private final String id;

    /** What a finding of it means, for messages. */
    private final String meaning;

    /**
     * Creates a check.
     *
     * @param id      the check's ID
     * @param meaning what a finding of it means
     */
    ComplianceCheck(String id, String meaning)
    {
        this.id = id;
        this.meaning = meaning;
    }

    /**
     * The check's ID, as {@code sdh.compliance.waive.<id>} names it.
     *
     * @return the ID
     */
    public String id()
    {
        return id;
    }

    /**
     * What a finding of this check means, for messages.
     *
     * @return the meaning
     */
    public String meaning()
    {
        return meaning;
    }

    /**
     * The check with an ID.
     *
     * @param id the ID, in any case, with dashes, dots or underscores
     *           between its words (an environment variable's name gives
     *           dots)
     * @return the check; empty if no check has that ID
     */
    public static Optional<ComplianceCheck> byId(String id)
    {
        String wanted = id.toLowerCase(Locale.ROOT).replaceAll("[._]", "-");
        for (ComplianceCheck check : values())
        {
            if (check.id.equals(wanted))
            {
                return Optional.of(check);
            }
        }
        return Optional.empty();
    }
}
