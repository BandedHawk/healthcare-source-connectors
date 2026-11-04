/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * RFC 9562 version 7 UUIDs: 48-bit Unix milliseconds, then random bits.
 * Sort roughly by creation time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class UuidV7
{

    /** The source of the random bits. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Not instantiable. */
    private UuidV7()
    {
    }

    /**
     * Makes a UUID.
     *
     * @param epochMillis the time, in Unix milliseconds
     * @return the UUID
     */
    public static UUID next(long epochMillis)
    {
        long mostSignificant = (epochMillis & 0xFFFF_FFFF_FFFFL) << 16
                | 0x7000L | (RANDOM.nextInt() & 0x0FFFL);
        long leastSignificant = (RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL)
                | 0x8000_0000_0000_0000L;
        return new UUID(mostSignificant, leastSignificant);
    }
}
