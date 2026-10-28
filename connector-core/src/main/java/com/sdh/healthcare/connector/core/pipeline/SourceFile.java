package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.util.Optional;

/** One original supplied by a connector, such as a file in the inbound directory. */
public interface SourceFile {

    SeekableByteSource open() throws IOException;

    /** The original name. PHI-sensitive: only ever stored encrypted (design §8.2). */
    Optional<String> originalName();

    /** Removes the original once it is safely archived. Must succeed if it is already gone. */
    void deleteOriginal() throws IOException;
}
