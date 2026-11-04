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
 * Produces the sections of one file, in order. Not thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface SplitSession extends AutoCloseable
{

    /**
     * The next section.
     *
     * @return the section, or empty when the file is finished
     * @throws IOException    if the file can't be read
     * @throws SplitException if the file can't be split further
     */
    Optional<Section> next() throws IOException, SplitException;

    /**
     * Ends the session. Does nothing unless overridden.
     */
    @Override
    default void close()
    {
    }
}
