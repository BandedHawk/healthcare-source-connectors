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
     * The major version. 3: what was a section is a record
     * ({@code record.stored}, {@code record.failed}, the {@code records}
     * table), and what was a file's ID is an item's ({@code itemId}).
     * Nothing was released under 2, so 3 doesn't read 2.x messages.
     * <p>
     * Under 2 (2.0 to 2.4), the contract gained: each record's content type
     * and mapping; the {@code record.failed} message and the failed-record
     * count in {@code file.completed}; each record's origin (where its text
     * starts, as a line and column in the record and in the item), and
     * where in the item a failed record failed; the {@code item.landed}
     * message, which a landing node sends to its splitter, with the item it
     * replaces; and the {@code item.received} and {@code item.result}
     * messages, which only an intake's producer receives, with the earlier
     * message each message replaces. All of it is part of 3.0.
     */
    public static final int MAJOR = 3;

    /** The minor version, from 0 for each major version. */
    public static final int MINOR = 0;

    /** The version this build writes, as "major.minor", such as "3.0". */
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
