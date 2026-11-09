/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.util.Optional;

/**
 * Produces the records of one file, in order. Not thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface SplitSession extends AutoCloseable
{

    /**
     * The next record.
     *
     * @return the record, or empty when the file is finished
     * @throws IOException    if the file can't be read
     * @throws SplitException if the file can't be split further
     */
    Optional<SplitRecord> next() throws IOException, SplitException;

    /**
     * Tells whether the file, read to its end without a record, was of this
     * format but held nothing to cut: a container with nothing in it, such
     * as a CSV header alone or an X12 envelope without a transaction set.
     * Asked only once {@link #next()} has ended the file, and only when it
     * gave no record. The split stage then fails the item
     * ({@code SPLIT_NO_RECORDS}); otherwise it takes the whole item as one
     * record (STAGES.md §16). A splitter that can't tell says {@code false},
     * the default.
     *
     * @return true if the file held an empty container of this format
     */
    default boolean emptyContainer()
    {
        return false;
    }

    /**
     * Ends the session. Does nothing unless overridden.
     */
    @Override
    default void close()
    {
    }
}
