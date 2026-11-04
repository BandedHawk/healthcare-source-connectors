/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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

/**
 * Local-disk FileIO that can list, for tests that need real files (orphan
 * cleanup, physical deletion).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class LocalFileIO implements FileIO, SupportsPrefixOperations
{

    /**
     * Creates the FileIO.
     */
    public LocalFileIO()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputFile newInputFile(String path)
    {
        return org.apache.iceberg.Files.localInput(path);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creates the file's directory first.
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
     * {@inheritDoc}
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<FileInfo> listPrefix(String prefix)
    {
        Path root = Path.of(prefix);
        List<FileInfo> out = new ArrayList<>();
        if (!Files.isDirectory(root))
        {
            return out;
        }
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : paths.filter(Files::isRegularFile).toList())
            {
                out.add(new FileInfo(path.toString(), Files.size(path),
                        Files.getLastModifiedTime(path).toMillis()));
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return out;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deletePrefix(String prefix)
    {
        try (Stream<Path> paths = Files.walk(Path.of(prefix)))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
            {
                Files.deleteIfExists(path);
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }
}
