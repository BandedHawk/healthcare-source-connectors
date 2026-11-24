/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.testcontainers;

import java.util.Locale;

/**
 * How the Testcontainers target's stages log in to Kafka (HIPAA.md §11.4,
 * L1). Either way the connection is TLS, the broker's certificate is
 * verified, and each stage is its own Kafka user.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum KafkaLogin
{
    /** {@code SASL_SSL} with a SCRAM-SHA-512 password per stage. */
    SCRAM,
    /**
     * {@code SSL} with a client certificate per stage (mutual TLS), the
     * broker taking the certificate's CN as the user.
     */
    CERTIFICATE;

    /**
     * The login a setting names.
     *
     * @param name {@code scram} or {@code certificate}; blank or null for
     *             {@code scram}
     * @return the login
     * @throws IllegalArgumentException if it names neither
     */
    public static KafkaLogin parse(String name)
    {
        if (name == null || name.isBlank())
        {
            return SCRAM;
        }
        try
        {
            return valueOf(name.strip().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknown)
        {
            throw new IllegalArgumentException("No Kafka login " + name
                    + "; they are scram and certificate", unknown);
        }
    }
}
