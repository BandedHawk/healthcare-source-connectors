/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsRecoveryOperations;

/**
 * An in-memory store that lists, as S3 does, and keeps each deleted
 * file's last version, as a versioned S3 bucket does, so a test can bring
 * it back. Loaded by class name ({@code io-impl}), so what it holds is
 * shared, as {@link InMemoryFileIO}'s is: each test uses a warehouse of
 * its own.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class VersionedFileIO extends ListingFileIO
        implements SupportsRecoveryOperations
{

    /** The last version of each file deleted, by location. */
    private static final Map<String, byte[]> DELETED =
            new ConcurrentHashMap<>();

    /** Creates the store. */
    public VersionedFileIO()
    {
    }

    @Override
    public void deleteFile(String location)
    {
        if (fileExists(location))
        {
            try (InputStream in = newInputFile(location).newStream())
            {
                DELETED.put(location, in.readAllBytes());
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
        }
        super.deleteFile(location);
    }

    @Override
    public boolean recoverFile(String location)
    {
        byte[] last = DELETED.remove(location);
        if (last == null)
        {
            return false;
        }
        addFile(location, last);
        return true;
    }

    /**
     * Deletes a file and every version of it, as a lifecycle rule that
     * expires noncurrent versions does.
     *
     * @param location the file
     */
    public static void expire(String location)
    {
        ListingFileIO io = new ListingFileIO();
        if (io.fileExists(location))
        {
            io.deleteFile(location);
        }
        DELETED.remove(location);
    }

    /**
     * The files deleted under a prefix that can be brought back.
     *
     * @param prefix the prefix
     * @return their locations
     */
    public static Set<String> deletedUnder(String prefix)
    {
        Set<String> under = ConcurrentHashMap.newKeySet();
        DELETED.keySet().stream().filter(each -> each.startsWith(prefix))
                .forEach(under::add);
        return under;
    }

    /**
     * The files under a prefix, by location.
     *
     * @param prefix the prefix
     * @return them
     */
    public static List<String> under(String prefix)
    {
        List<String> files = new ArrayList<>();
        new ListingFileIO().listPrefix(prefix).forEach(
                (FileInfo file) -> files.add(file.location()));
        return files;
    }
}
