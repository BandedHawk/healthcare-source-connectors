package com.sdh.healthcare.connector.file.demo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Iceberg tables on local disk, for the in-process demo: the encrypted Parquet files and table metadata end up under
 * {@code target/demo/warehouse/}, where they can be looked at afterwards. Not for production (no listing, so no
 * orphan cleanup; the demo turns maintenance off).
 */
public class DemoLocalFileIO implements FileIO {

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
}
