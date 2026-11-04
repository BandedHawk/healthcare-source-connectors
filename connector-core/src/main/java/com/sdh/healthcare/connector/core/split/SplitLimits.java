/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

/**
 * Per-format limits (design §6.3). A violation fails the file with a
 * {@code LIMIT_*} error code.
 *
 * @param maxFileBytes       the largest file
 * @param maxSectionBytes    the largest section
 * @param maxSectionsPerFile the most sections in one file
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SplitLimits(long maxFileBytes, long maxSectionBytes,
        long maxSectionsPerFile)
{

    /** A gibibyte. */
    public static final long GiB = 1L << 30;

    /** A tebibyte. */
    public static final long TiB = 1L << 40;

    /**
     * Checks the limits.
     *
     * @param maxFileBytes       the largest file
     * @param maxSectionBytes    the largest section
     * @param maxSectionsPerFile the most sections in one file
     * @throws IllegalArgumentException if a limit isn't positive
     */
    public SplitLimits
    {
        if (maxFileBytes <= 0 || maxSectionBytes <= 0
                || maxSectionsPerFile <= 0)
        {
            throw new IllegalArgumentException("Limits must be positive");
        }
    }

    /**
     * The default limits for a format: files up to 10 GiB for CSV and
     * 1 TiB otherwise, sections up to 512 MiB, any number of sections.
     *
     * @param format the format's name
     * @return the limits
     */
    public static SplitLimits defaultsFor(String format)
    {
        long maxFile = "csv".equals(format) ? 10 * GiB : TiB;
        return new SplitLimits(maxFile, 512L << 20, Long.MAX_VALUE);
    }
}
