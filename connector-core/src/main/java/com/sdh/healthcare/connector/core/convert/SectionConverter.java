package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;

/**
 * Converts one split section to JSON before it is stored (design §6.4). Found by format name in
 * {@link ConverterRegistry}; formats without a converter (FHIR, which is already JSON; generic XML; delimited) are
 * stored as split.
 *
 * <p>Contract:
 * <ul>
 *   <li>Deterministic: the same section bytes always give the same JSON bytes, because the section ID is a keyed
 *       hash of what is stored (design §7.3).</li>
 *   <li>Thread-safe: the pipeline converts on its worker pool.</li>
 *   <li>Problems are reported as {@link ConversionException} with a code and a position only, never content. The
 *       section is then stored unconverted and announced as failed; the file carries on.</li>
 * </ul>
 */
public interface SectionConverter {

    /** The splitter format this converter applies to, e.g. {@code "csv"}. */
    String format();

    /** Name and version of the JSON mapping, carried in messages so readers know the shape, e.g. {@code "csv-row/1"}. */
    String mapping();

    /** Checks the configuration at startup. */
    default void validate(SplitConfig cfg) {}

    /** Returns the section as UTF-8 JSON. */
    byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException;
}
