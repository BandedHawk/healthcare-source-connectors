package com.sdh.healthcare.connector.core.pipeline;

import java.security.SecureRandom;
import java.util.UUID;

/** RFC 9562 version 7 UUIDs: 48-bit Unix milliseconds, then random bits. Sort roughly by creation time. */
public final class UuidV7 {
    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {}

    public static UUID next(long epochMillis) {
        long msb = (epochMillis & 0xFFFF_FFFF_FFFFL) << 16 | 0x7000L | (RANDOM.nextInt() & 0x0FFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(msb, lsb);
    }
}
