package com.sdh.healthcare.connector.core.split;

import java.io.IOException;

/**
 * Splits one source format into self-contained sections (design §6.1). Discovered with {@link java.util.ServiceLoader};
 * proprietary formats can be added as a jar.
 *
 * <p>Contract:
 * <ul>
 *   <li>Sections come out in source order and never overlap.</li>
 *   <li>{@link #open} must accept {@link ResumePoint#START} and the {@link Section#resume()} of any section it
 *       produced, and then produce exactly the sections that followed.</li>
 *   <li>A splitter holds no section bytes in memory beyond small context (headers, envelopes, start tags).</li>
 *   <li>Errors are reported as {@link SplitException} with a code and offset only.</li>
 * </ul>
 */
public interface SectionSplitter {

    /** Format name used in configuration, e.g. {@code "ccda"}. */
    String format();

    /** Checks the configuration at startup, so a bad setting fails the service rather than a file. */
    default void validate(SplitConfig cfg) {}

    SplitSession open(SeekableByteSource src, ResumePoint from, SplitConfig cfg) throws IOException, SplitException;
}
