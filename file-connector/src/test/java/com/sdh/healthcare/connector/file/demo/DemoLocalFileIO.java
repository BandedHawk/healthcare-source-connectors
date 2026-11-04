/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Iceberg tables on local disk, for the in-process demo: the encrypted
 * Parquet files and table metadata end up under
 * {@code target/demo/warehouse/}, where they can be looked at afterwards.
 * Not for production (no listing, so no orphan cleanup; the demo turns
 * maintenance off).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class DemoLocalFileIO implements FileIO
{

    /** Created by Iceberg by reflection. */
    public DemoLocalFileIO()
    {
    }

    /**
     * A local file to read.
     *
     * @param path the file's path
     * @return the file
     */
    @Override
    public InputFile newInputFile(String path)
    {
        // Iceberg's Files, not java.nio's: the two names clash.
        return org.apache.iceberg.Files.localInput(path);
    }

    /**
     * A local file to write, creating its directory.
     *
     * @param path the file's path
     * @return the file
     */
    @Override
    public OutputFile newOutputFile(String path)
    {
        try
        {
            Files.createDirectories(Path.of(path).getParent());
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return org.apache.iceberg.Files.localOutput(path);
    }

    /**
     * Deletes a local file if it exists.
     *
     * @param path the file's path
     */
    @Override
    public void deleteFile(String path)
    {
        try
        {
            Files.deleteIfExists(Path.of(path));
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }
}
