/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.util.HashMap;
import java.util.Map;

/**
 * Run-time split settings (design §6.2, §14).
 *
 * @param boundary        the format's section boundary; null means the
 *                        splitter's default
 * @param options         format-specific options, keyed as in
 *                        configuration (e.g. {@code csv.header})
 * @param limits          size and count limits
 * @param readBufferBytes read buffer size; 4 MiB by default (design §13)
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SplitConfig(String boundary, Map<String, String> options,
        SplitLimits limits, int readBufferBytes)
{

    /** The default read buffer size: 4 MiB. */
    public static final int DEFAULT_READ_BUFFER = 4 << 20;

    /**
     * Copies the options.
     *
     * @param boundary        the section boundary; null for the default
     * @param options         format-specific options
     * @param limits          size and count limits
     * @param readBufferBytes read buffer size
     */
    public SplitConfig
    {
        options = Map.copyOf(options);
    }

    /**
     * The default settings for a format.
     *
     * @param format the format's name
     * @return the settings
     */
    public static SplitConfig defaults(String format)
    {
        return new SplitConfig(null, Map.of(),
                SplitLimits.defaultsFor(format), DEFAULT_READ_BUFFER);
    }

    /**
     * A copy with another boundary.
     *
     * @param sectionBoundary the boundary; null for the default
     * @return the copy
     */
    public SplitConfig withBoundary(String sectionBoundary)
    {
        return new SplitConfig(sectionBoundary, options, limits,
                readBufferBytes);
    }

    /**
     * A copy with one option set.
     *
     * @param key   the option's key
     * @param value its value
     * @return the copy
     */
    public SplitConfig withOption(String key, String value)
    {
        Map<String, String> changed = new HashMap<>(options);
        changed.put(key, value);
        return new SplitConfig(boundary, changed, limits, readBufferBytes);
    }

    /**
     * A copy with other limits.
     *
     * @param splitLimits the limits
     * @return the copy
     */
    public SplitConfig withLimits(SplitLimits splitLimits)
    {
        return new SplitConfig(boundary, options, splitLimits,
                readBufferBytes);
    }

    /**
     * A copy with another read buffer size.
     *
     * @param bytes the size
     * @return the copy
     */
    public SplitConfig withReadBuffer(int bytes)
    {
        return new SplitConfig(boundary, options, limits, bytes);
    }

    /**
     * The boundary, or a fallback when none is set.
     *
     * @param fallback the splitter's default
     * @return the boundary
     */
    public String boundaryOr(String fallback)
    {
        return boundary == null || boundary.isBlank() ? fallback : boundary;
    }

    /**
     * A text option.
     *
     * @param key      the option's key
     * @param fallback used when it is missing or blank
     * @return its value
     */
    public String option(String key, String fallback)
    {
        String value = options.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * A boolean option.
     *
     * @param key      the option's key
     * @param fallback used when it is missing or blank
     * @return its value
     */
    public boolean booleanOption(String key, boolean fallback)
    {
        String value = options.get(key);
        return value == null || value.isBlank()
                ? fallback : Boolean.parseBoolean(value.trim());
    }

    /**
     * A whole-number option.
     *
     * @param key      the option's key
     * @param fallback used when it is missing or blank
     * @return its value
     * @throws NumberFormatException if it isn't a number
     */
    public int intOption(String key, int fallback)
    {
        String value = options.get(key);
        return value == null || value.isBlank()
                ? fallback : Integer.parseInt(value.trim());
    }
}
