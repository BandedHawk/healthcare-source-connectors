package com.sdh.healthcare.connector.file.source;

import com.sdh.healthcare.connector.core.pipeline.SourceFile;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/** A file in the inbound directory. Its name is PHI-sensitive and never logged (design §7.5). */
public final class LocalSourceFile implements SourceFile {
    private final Path path;
    private final Path marker;

    public LocalSourceFile(Path path) {
        this(path, null);
    }

    /** @param marker the {@code .done} marker file, deleted together with the original (marker read-lock mode) */
    public LocalSourceFile(Path path, Path marker) {
        this.path = path;
        this.marker = marker;
    }

    @Override
    public SeekableByteSource open() throws IOException {
        return SeekableByteSource.open(path);
    }

    @Override
    public Optional<String> originalName() {
        return Optional.of(path.getFileName().toString());
    }

    @Override
    public void deleteOriginal() throws IOException {
        Files.deleteIfExists(path);
        if (marker != null) {
            Files.deleteIfExists(marker);
        }
    }

    @Override
    public String toString() {
        return "LocalSourceFile[name withheld]";
    }
}
