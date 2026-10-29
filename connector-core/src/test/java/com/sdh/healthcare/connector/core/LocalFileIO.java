package com.sdh.healthcare.connector.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;

/** Local-disk FileIO that can list, for tests that need real files (orphan cleanup, physical deletion). */
public class LocalFileIO implements FileIO, SupportsPrefixOperations {

    @Override
    public InputFile newInputFile(String path) {
        return org.apache.iceberg.Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        try {
            Files.createDirectories(Path.of(path).getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return org.apache.iceberg.Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
        try {
            Files.deleteIfExists(Path.of(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
        Path root = Path.of(prefix);
        List<FileInfo> out = new ArrayList<>();
        if (!Files.isDirectory(root)) {
            return out;
        }
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                out.add(new FileInfo(p.toString(), Files.size(p), Files.getLastModifiedTime(p).toMillis()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    @Override
    public void deletePrefix(String prefix) {
        try (Stream<Path> s = Files.walk(Path.of(prefix))) {
            for (Path p : s.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
