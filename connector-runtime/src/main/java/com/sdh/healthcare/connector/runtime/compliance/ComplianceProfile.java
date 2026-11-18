/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import java.util.Locale;

/**
 * The compliance profile a node starts under, {@code sdh.compliance.profile}
 * (design §7.6).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum ComplianceProfile
{

    /**
     * The default: a node starts only if every check of
     * {@link ComplianceCheck} passes or is waived with a reason.
     */
    HIPAA,

    /**
     * No checks, for development and test deployments that hold no PHI.
     * Audited as {@code compliance.off} at every start.
     */
    NONE;

    /**
     * The profile a setting names.
     *
     * @param value the setting, in any case; blank for the default
     * @return the profile
     * @throws IllegalArgumentException if it names no profile
     */
    public static ComplianceProfile parse(String value)
    {
        String name = value == null ? "" : value.strip();
        if (name.isEmpty())
        {
            return HIPAA;
        }
        try
        {
            return valueOf(name.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException(ComplianceRules.PROFILE
                    + " must be hipaa or none", e);
        }
    }

    /**
     * The profile's name, as it is set.
     *
     * @return the name, in lower case
     */
    public String id()
    {
        return name().toLowerCase(Locale.ROOT);
    }
}
