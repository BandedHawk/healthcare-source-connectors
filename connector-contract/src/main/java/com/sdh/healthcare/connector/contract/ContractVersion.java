/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract;

/**
 * The version of the wire contract: the message payloads, the encryption of
 * chunks and the layout of the tables.
 * <p>
 * A reader accepts any message with the same major version. A minor version
 * only adds optional fields, which older readers ignore. A major change needs
 * the readers upgraded first.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ContractVersion
{

    /**
     * The major version. 2: sections carry their content type and mapping,
     * there is a {@code section.failed} message, and {@code file.completed}
     * counts the failed sections.
     */
    public static final int MAJOR = 2;

    /**
     * The minor version. 1: sections carry their origin (where their text
     * starts, as a line and column in the section and in the file), and
     * failed sections where in the file they failed.
     */
    public static final int MINOR = 1;

    /** The version this build writes, as "major.minor", such as "2.1". */
    public static final String CURRENT = MAJOR + "." + MINOR;

    /** Not instantiated: this class only holds constants. */
    private ContractVersion()
    {
    }

    /**
     * Tells whether this build can read a payload of a version: whether its
     * major version is this build's.
     *
     * @param version the payload's version, as "major.minor" (or a major
     *                version alone)
     * @return true if it can be read; false for another major version, or
     *         for {@code null} or text that isn't a version
     */
    public static boolean isReadable(String version)
    {
        if (version == null)
        {
            return false;
        }
        int dot = version.indexOf('.');
        try
        {
            int major = Integer.parseInt(
                    dot < 0 ? version : version.substring(0, dot));
            return major == MAJOR;
        }
        catch (NumberFormatException notANumber)
        {
            return false;
        }
    }
}
