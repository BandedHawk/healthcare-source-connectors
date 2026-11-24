/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * An in-memory store that lists, as an object store does, but keeps no
 * versions: a deleted file is gone. Loaded by class name
 * ({@code io-impl}), so what it holds is shared, as
 * {@link InMemoryFileIO}'s is: each test uses a warehouse of its own.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class ListingFileIO extends InMemoryFileIO
        implements SupportsPrefixOperations
{

    /** Every location written, live or not. */
    private static final Set<String> WRITTEN = ConcurrentHashMap.newKeySet();

    /** Creates the store. */
    public ListingFileIO()
    {
    }

    @Override
    public void addFile(String location, byte[] contents)
    {
        WRITTEN.add(location);
        super.addFile(location, contents);
    }

    /**
     * A file, there or not: as an object store's, one that isn't there
     * answers that it doesn't exist, and fails only when read.
     *
     * @param location the file
     * @return it
     */
    @Override
    public InputFile newInputFile(String location)
    {
        if (fileExists(location))
        {
            return super.newInputFile(location);
        }
        return new InputFile()
        {
            @Override
            public long getLength()
            {
                throw new NotFoundException("No such file");
            }

            @Override
            public SeekableInputStream newStream()
            {
                throw new NotFoundException("No such file");
            }

            @Override
            public String location()
            {
                return location;
            }

            @Override
            public boolean exists()
            {
                return false;
            }
        };
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix)
    {
        List<FileInfo> files = new ArrayList<>();
        for (String location : WRITTEN)
        {
            if (location.startsWith(prefix) && fileExists(location))
            {
                files.add(new FileInfo(location,
                        newInputFile(location).getLength(), 0));
            }
        }
        return files;
    }

    @Override
    public void deletePrefix(String prefix)
    {
        for (FileInfo file : listPrefix(prefix))
        {
            deleteFile(file.location());
        }
    }
}
