package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.util.Optional;

/** Produces the sections of one file, in order. Not thread-safe. */
public interface SplitSession extends AutoCloseable {

    /** The next section, or empty when the file is finished. */
    Optional<Section> next() throws IOException, SplitException;

    @Override
    default void close() {}
}
