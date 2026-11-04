/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.util.ServiceLoader;

/**
 * Splits one source format into self-contained sections (design §6.1).
 * Discovered with {@link ServiceLoader}; proprietary formats can be added
 * as a jar.
 * <p>
 * Contract:
 * <ul>
 * <li>Sections come out in source order and never overlap.</li>
 * <li>{@link #open} must accept {@link ResumePoint#START} and the
 * {@link Section#resume()} of any section it produced, and then produce
 * exactly the sections that followed.</li>
 * <li>A splitter holds no section bytes in memory beyond small context
 * (headers, envelopes, start tags).</li>
 * <li>Errors are reported as {@link SplitException} with a code and offset
 * only.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface SectionSplitter
{

    /**
     * Format name used in configuration, e.g. {@code "ccda"}.
     *
     * @return the name
     */
    String format();

    /**
     * Checks the configuration at startup, so a bad setting fails the
     * service rather than a file.
     *
     * @param config the configuration
     * @throws IllegalArgumentException if a setting is invalid
     */
    default void validate(SplitConfig config)
    {
    }

    /**
     * Starts splitting a file.
     *
     * @param source the file
     * @param from   where to start: {@link ResumePoint#START}, or a
     *               section's resume point
     * @param config the configuration
     * @return the session that produces the sections
     * @throws IOException    if the file can't be read
     * @throws SplitException if the file can't be split from there
     */
    SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config) throws IOException, SplitException;
}
