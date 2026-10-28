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

    public LocalSourceFile(Path path) {
        this.path = path;
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
    }

    @Override
    public String toString() {
        return "LocalSourceFile[name withheld]";
    }
}
