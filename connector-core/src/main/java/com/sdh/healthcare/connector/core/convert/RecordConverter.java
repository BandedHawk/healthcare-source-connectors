/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;

/**
 * Converts one split record to JSON before it is stored (design §6.4). Found
 * by format name in {@link ConverterRegistry}; formats without a converter
 * (FHIR, which is already JSON; generic XML; delimited) are stored as split.
 * <p>
 * Contract:
 * <ul>
 * <li>Deterministic: the same record bytes always give the same JSON bytes,
 * because the record ID is a keyed hash of what is stored (design §7.3).</li>
 * <li>Thread-safe: the pipeline converts on its worker pool.</li>
 * <li>Problems are reported as {@link ConversionException} with a code and a
 * position only, never content. The record is then stored unconverted and
 * announced as failed; the file carries on.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface RecordConverter
{

    /**
     * Which kind of file this converter handles, as the name of the splitter
     * format it pairs with: the records that splitter produces are the ones
     * this converter is given. The connector's configured format selects both,
     * so the value must match the splitter's
     * {@link com.sdh.healthcare.connector.core.split.RecordSplitter#format()}
     * exactly, for example {@code "csv"} or {@code "ccda"}.
     * <p>
     * {@link ConverterRegistry} indexes converters by this name. When it finds
     * them on the class path ({@link ConverterRegistry#load()}), two converters
     * claiming the same name stop the service from starting.
     *
     * @return the splitter format name; never {@code null}, and the same on
     *         every call
     */
    String format();

    /**
     * The name and version of the JSON this converter produces, such as
     * {@code "csv-row/1"} or {@code "ccda/2"}. Every converted record is
     * stored and announced with it, so whatever reads the JSON downstream
     * knows its shape and can tell when it has changed. A record that could
     * not be converted is stored as split and carries no mapping.
     * <p>
     * The part after the {@code /} is a version: change it whenever the JSON
     * changes in a way that existing readers would get wrong, and only then.
     *
     * @return the mapping name and version; never {@code null}, and the same on
     *         every call
     */
    String mapping();

    /**
     * Checks, when the connector starts, that the split settings are ones this
     * converter can work with. The pipeline calls it once, after the
     * splitter's own check, so a bad setting stops the service at startup
     * rather than making every file fail later. The default accepts any
     * settings; a converter that depends on a setting overrides it.
     *
     * @param config the connector's split settings, the same ones every
     *               record will later be converted with
     * @throws IllegalArgumentException if a setting can't be used with this
     *                                  converter, with a message naming the
     *                                  setting (never file content)
     */
    default void validate(SplitConfig config)
    {
    }

    /**
     * The most heap {@link #convert} needs at once, as a multiple of the
     * record's size, the record itself included. The pipeline converts
     * several records at a time, and uses this to keep them within the heap
     * it sets aside for conversion: a large record waits until there is room
     * for it. Overestimating only makes large records wait sooner;
     * underestimating risks running out of memory. The default, 4, is the most
     * any built-in converter needs.
     *
     * @return the peak heap as a multiple of the record's size; at least 1
     */
    default double memoryFactor()
    {
        return 4;
    }

    /**
     * Converts one record, as the splitter produced it, into JSON. Called by
     * the pipeline's worker threads, possibly for several records at once,
     * once for each record of a file in this converter's format.
     * <p>
     * On success the JSON is stored in place of the record and labelled with
     * {@link #mapping()}. On failure the record is stored as split, with the
     * exception's code and position, and the rest of the file is still
     * processed. Either way, the same input must give the same result every
     * time (see the interface's contract).
     * <p>
     * The pipeline clears the {@code record} array after a successful
     * conversion, because it may hold patient data, so the returned array must
     * be a new one and nothing may keep a reference to the input.
     *
     * @param record the record's bytes exactly as split, in the file's own
     *                character encoding; not changed by this method
     * @param config the connector's split settings, as passed to
     *               {@link #validate}, for converters whose output depends on
     *               them
     * @return the record as JSON, encoded in UTF-8, in a new array
     * @throws ConversionException if the record can't be converted, such as
     *                             malformed content or an invalid encoding;
     *                             it carries an error code and a position,
     *                             never content, so it is safe to log
     */
    byte[] convert(byte[] record, SplitConfig config) throws
            ConversionException;
}
