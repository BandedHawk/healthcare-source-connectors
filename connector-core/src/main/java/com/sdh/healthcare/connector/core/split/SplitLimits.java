package com.sdh.healthcare.connector.core.split;

/** Per-format limits (design §6.3). A violation fails the file with a {@code LIMIT_*} error code. */
public record SplitLimits(long maxFileBytes, long maxSectionBytes, long maxSectionsPerFile) {
    public static final long GiB = 1L << 30;
    public static final long TiB = 1L << 40;

    public SplitLimits {
        if (maxFileBytes <= 0 || maxSectionBytes <= 0 || maxSectionsPerFile <= 0) {
            throw new IllegalArgumentException("Limits must be positive");
        }
    }

    public static SplitLimits defaultsFor(String format) {
        long maxFile = "csv".equals(format) ? 10 * GiB : TiB;
        return new SplitLimits(maxFile, 512L << 20, Long.MAX_VALUE);
    }
}
