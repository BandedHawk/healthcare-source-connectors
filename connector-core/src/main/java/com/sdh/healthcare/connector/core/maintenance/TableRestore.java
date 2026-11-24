/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.io.SupportsRecoveryOperations;

/**
 * A table after its catalog was restored to an earlier point
 * (DESIGN.md §18): every file the catalog's metadata references looked
 * for, and one deleted since brought back from the store's versions; the
 * files nothing references counted, and kept; and, for a table that must
 * not go back (landing's), the newest metadata of the same table found in
 * its location, to point the catalog at.
 * <p>
 * It reads and names files by their locations only, which hold no PHI: a
 * table's files are named by UUIDs. It never reads a data file's rows.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TableRestore
{

    /**
     * The table property a restore's check sets: while it is there, the
     * table's maintenance removes no orphan, until an operator releases
     * them. Its value is when the check set it.
     */
    public static final String KEEP_ORPHANS = "sdh.restore.keep-orphans";

    /**
     * A metadata file's name: its version, a UUID, and an optional
     * compression, as Iceberg names them.
     */
    private static final Pattern METADATA = Pattern.compile(
            "(\\d+)-[0-9a-fA-F-]{36}(?:\\.[a-z0-9]+)?\\.metadata\\.json");

    /** Not created: holds the steps. */
    private TableRestore()
    {
    }

    /**
     * What a walk of a table's references found.
     *
     * @param table      the table
     * @param referenced how many files its metadata references
     * @param recovered  those brought back from the store's versions
     * @param missing    those missing and not brought back
     * @param complete   false if a manifest list or manifest was missing,
     *                   so the files it names weren't seen
     * @param orphans    the files under the table's location that nothing
     *                   references
     */
    public record Walk(String table, long referenced, List<String> recovered,
            List<String> missing, boolean complete, Orphans orphans)
    {

        /**
         * Copies the lists.
         */
        public Walk
        {
            recovered = List.copyOf(recovered);
            missing = List.copyOf(missing);
        }

        /**
         * Whether every file the table needs is there.
         *
         * @return true if none is missing
         */
        public boolean whole()
        {
            return missing.isEmpty() && complete;
        }
    }

    /**
     * The files under a table's location that nothing references.
     *
     * @param count  how many
     * @param bytes  their size
     * @param listed false if the store can't list, so they weren't counted
     */
    public record Orphans(long count, long bytes, boolean listed)
    {
    }

    /**
     * What looking for a table's newer metadata found.
     *
     * @param table  the table
     * @param from   the metadata the catalog points at
     * @param to     the newest metadata of the same table, if newer
     * @param rivals the newest metadata files when more than one has the
     *               newest version: an operator chooses
     * @param epoch  the lease epoch {@code to} records, 0 if none
     */
    public record Forward(String table, String from, Optional<String> to,
            List<String> rivals, long epoch)
    {

        /**
         * Copies the rivals.
         */
        public Forward
        {
            rivals = List.copyOf(rivals);
        }
    }

    /**
     * Walks every file the table's metadata references: its current
     * metadata file, the statistics, and, for every snapshot, its manifest
     * list, manifests, and data and delete files. Each one missing is
     * brought back from the store's versions when asked to repair and the
     * store can ({@link SupportsRecoveryOperations}: S3 with versioning).
     * Then counts the files under the table's location that nothing
     * references, or that the earlier metadata files do.
     *
     * @param table  the table, loaded
     * @param repair whether to bring back what is missing
     * @return what was found
     * @throws UncheckedIOException if a manifest can't be read
     */
    public static Walk walk(Table table, boolean repair)
    {
        FileIO io = table.io();
        String prefix = prefix(table.location());
        Map<String, Long> listed = list(io, prefix);
        Finder finder = new Finder(io, prefix, listed, repair);
        boolean complete = true;

        TableMetadata current = metadata(table);
        finder.need(current.metadataFileLocation());
        for (StatisticsFile statistics : table.statisticsFiles())
        {
            finder.need(statistics.path());
        }
        for (PartitionStatisticsFile statistics
                : table.partitionStatisticsFiles())
        {
            finder.need(statistics.path());
        }
        // Earlier metadata files are referenced, but not needed.
        current.previousFiles().forEach(entry ->
                finder.referenced.add(entry.file()));

        Set<String> manifests = new HashSet<>();
        for (Snapshot snapshot : table.snapshots())
        {
            String list = snapshot.manifestListLocation();
            if (list != null && !finder.need(list))
            {
                complete = false;
                continue;
            }
            List<ManifestFile> each;
            try
            {
                each = TableMaintenance.detached(snapshot).allManifests(io);
            }
            catch (RuntimeException unreadable)
            {
                // Listed, but not there to read: a store's listing may
                // show what a delete marker hides.
                finder.unreadable(list);
                complete = false;
                continue;
            }
            for (ManifestFile manifest : each)
            {
                if (!manifests.add(manifest.path()))
                {
                    continue;
                }
                if (!finder.need(manifest.path()))
                {
                    complete = false;
                    continue;
                }
                try
                {
                    complete &= files(table, manifest, finder);
                }
                catch (RuntimeException unreadable)
                {
                    finder.unreadable(manifest.path());
                    complete = false;
                }
            }
        }

        Orphans orphans = listed == null ? new Orphans(0, 0, false)
                : orphans(listed, finder.referenced);
        return new Walk(table.name(), finder.referenced.size(),
                finder.recovered, finder.missing, complete, orphans);
    }

    /**
     * Looks for the data or delete files a manifest names.
     *
     * @param table    the table
     * @param manifest the manifest, there
     * @param finder   what looks
     * @return true; the files' own absence is the finder's
     * @throws UncheckedIOException if the manifest can't be read
     */
    private static boolean files(Table table, ManifestFile manifest,
            Finder finder)
    {
        try
        {
            if (manifest.content() == ManifestContent.DATA)
            {
                try (CloseableIterable<String> paths = ManifestFiles
                        .readPaths(manifest, table.io(), table.specs()))
                {
                    paths.forEach(finder::need);
                }
            }
            else
            {
                try (ManifestReader<DeleteFile> deletes = ManifestFiles
                        .readDeleteManifest(manifest, table.io(),
                                table.specs()))
                {
                    deletes.forEach(delete -> finder.need(delete.location()));
                }
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return true;
    }

    /**
     * The newest metadata of the same table in its location, if it is newer
     * than what the catalog points at: the same table UUID, a higher
     * version, and no older a sequence number. More than one file of that
     * highest version are rivals, for an operator to choose between.
     *
     * @param table the table, loaded
     * @return what was found
     * @throws IllegalStateException if the store can't list
     */
    public static Forward newest(Table table)
    {
        FileIO io = table.io();
        if (!(io instanceof SupportsPrefixOperations listing))
        {
            throw new IllegalStateException("The table's store can't list,"
                    + " so its newer metadata can't be found");
        }
        TableMetadata current = metadata(table);
        String from = current.metadataFileLocation();
        long fromVersion = version(from).orElse(-1L);
        String under = prefix(table.location()) + "metadata/";

        long best = fromVersion;
        List<String> newest = new ArrayList<>();
        for (FileInfo file : listing.listPrefix(under))
        {
            String location = TableMaintenance.qualify(under, file.location());
            Optional<Long> version = version(location);
            if (version.isEmpty() || version.get() < best)
            {
                continue;
            }
            if (version.get() == fromVersion && location.equals(from))
            {
                continue;
            }
            TableMetadata candidate = TableMetadataParser.read(io, location);
            if (!candidate.uuid().equals(current.uuid())
                    || candidate.lastSequenceNumber()
                            < current.lastSequenceNumber())
            {
                continue;
            }
            if (version.get() > best)
            {
                best = version.get();
                newest.clear();
            }
            newest.add(location);
        }
        if (newest.isEmpty())
        {
            return new Forward(table.name(), from, Optional.empty(),
                    List.of(), 0);
        }
        if (newest.size() > 1)
        {
            return new Forward(table.name(), from, Optional.empty(), newest,
                    0);
        }
        TableMetadata tip = TableMetadataParser.read(io, newest.get(0));
        return new Forward(table.name(), from, Optional.of(newest.get(0)),
                List.of(), FencedCommitter.epochOf(tip.properties()));
    }

    /**
     * Points the catalog at a newer metadata file of the table: drops the
     * catalog's entry, keeping every file, and registers the table again
     * at that file. Run only while nothing writes the table.
     *
     * @param catalog  the catalog
     * @param id       the table
     * @param metadata the metadata file
     * @return the table, loaded at it
     */
    public static Table pointAt(Catalog catalog, TableIdentifier id,
            String metadata)
    {
        catalog.dropTable(id, false);
        return catalog.registerTable(id, metadata);
    }

    /**
     * Marks the table so its maintenance keeps every orphan until
     * {@link #release}.
     *
     * @param table the table
     * @param at    when
     */
    public static void keepOrphans(Table table, Instant at)
    {
        table.updateProperties().set(KEEP_ORPHANS, at.toString()).commit();
    }

    /**
     * Lets the table's maintenance remove orphans again.
     *
     * @param table the table
     * @return whether it was marked
     */
    public static boolean release(Table table)
    {
        if (!table.properties().containsKey(KEEP_ORPHANS))
        {
            return false;
        }
        table.updateProperties().remove(KEEP_ORPHANS).commit();
        return true;
    }

    /**
     * A metadata file's version, from its name.
     *
     * @param location the file
     * @return its version; empty if it isn't named as a metadata file
     */
    static Optional<Long> version(String location)
    {
        String name = location.substring(location.lastIndexOf('/') + 1);
        Matcher matcher = METADATA.matcher(name);
        return matcher.matches() ? Optional.of(Long.parseLong(
                matcher.group(1))) : Optional.empty();
    }

    /**
     * The table's metadata as loaded.
     *
     * @param table the table
     * @return it
     * @throws IllegalStateException if the table has no operations
     */
    private static TableMetadata metadata(Table table)
    {
        if (!(table instanceof HasTableOperations operations))
        {
            throw new IllegalStateException("The table's metadata can't be"
                    + " read");
        }
        return operations.operations().current();
    }

    /**
     * A location as a prefix, ending in a slash.
     *
     * @param location the location
     * @return it
     */
    private static String prefix(String location)
    {
        return location.endsWith("/") ? location : location + "/";
    }

    /**
     * Every file under a prefix, with its size, if the store can list.
     *
     * @param io     the store
     * @param prefix the prefix
     * @return them by full location; null if the store can't list
     */
    private static Map<String, Long> list(FileIO io, String prefix)
    {
        if (!(io instanceof SupportsPrefixOperations listing))
        {
            return null;
        }
        Map<String, Long> files = new HashMap<>();
        for (FileInfo file : listing.listPrefix(prefix))
        {
            files.put(canonical(TableMaintenance.qualify(prefix,
                    file.location())),
                    file.size());
        }
        return files;
    }

    /**
     * A location with each run of slashes in its path made one, as some
     * stores keep and list a key written with two (a partition path ending
     * in a slash, then a file's name).
     *
     * @param location the location
     * @return it, so written
     */
    static String canonical(String location)
    {
        int scheme = location.indexOf("://");
        int from = scheme < 0 ? 0 : scheme + 3;
        return location.substring(0, from)
                + location.substring(from).replaceAll("/{2,}", "/");
    }

    /**
     * The listed files nothing references.
     *
     * @param listed     the files listed
     * @param referenced the files referenced
     * @return them, counted
     */
    private static Orphans orphans(Map<String, Long> listed,
            Set<String> referenced)
    {
        long count = 0;
        long bytes = 0;
        Set<String> wanted = new HashSet<>();
        referenced.forEach(each -> wanted.add(canonical(each)));
        for (Map.Entry<String, Long> file : listed.entrySet())
        {
            if (!wanted.contains(file.getKey()))
            {
                count++;
                bytes += file.getValue();
            }
        }
        return new Orphans(count, bytes, true);
    }

    /**
     * Looks for referenced files, in the listing where it can, and brings
     * back those missing when asked.
     */
    private static final class Finder
    {

        /** The store. */
        private final FileIO io;

        /** The table's location, as a prefix. */
        private final String prefix;

        /** The files under it; null if the store can't list. */
        private final Map<String, Long> listed;

        /** Whether to bring back what is missing. */
        private final boolean repair;

        /** Every file referenced. */
        private final Set<String> referenced = new HashSet<>();

        /** Those brought back. */
        private final List<String> recovered = new ArrayList<>();

        /** Those missing and not brought back, in the order found. */
        private final List<String> missing = new ArrayList<>();

        /** The same, to look in. */
        private final Set<String> absent = new HashSet<>();

        /**
         * Makes a finder.
         *
         * @param io     the store
         * @param prefix the table's location, as a prefix
         * @param listed the files under it; null if the store can't list
         * @param repair whether to bring back what is missing
         */
        private Finder(FileIO io, String prefix, Map<String, Long> listed,
                boolean repair)
        {
            this.io = io;
            this.prefix = prefix;
            this.listed = listed;
            this.repair = repair;
        }

        /**
         * Notes a file as referenced and needed, and looks for it; brings
         * it back if it is missing and that was asked.
         *
         * @param location the file
         * @return whether it is there now
         */
        private boolean need(String location)
        {
            if (!referenced.add(location))
            {
                return !absent.contains(location);
            }
            if (there(location))
            {
                return true;
            }
            if (repair && io instanceof SupportsRecoveryOperations recovery
                    && recovery.recoverFile(location) && exists(location))
            {
                recovered.add(location);
                if (listed != null)
                {
                    listed.put(canonical(location),
                            io.newInputFile(location).getLength());
                }
                return true;
            }
            missing.add(location);
            absent.add(location);
            return false;
        }

        /**
         * Notes a file found but that can't be read: missing, as far as the
         * table is concerned.
         *
         * @param location the file
         */
        private void unreadable(String location)
        {
            if (location != null && absent.add(location))
            {
                missing.add(location);
            }
        }

        /**
         * Whether a file is there: in the listing, if the store listed and
         * it is under the table's location, or else as the store answers
         * when asked, which also finds one listed under another form of
         * its key.
         *
         * @param location the file
         * @return true if it is
         */
        private boolean there(String location)
        {
            if (listed != null && location.startsWith(prefix)
                    && listed.containsKey(canonical(location)))
            {
                return true;
            }
            return exists(location);
        }

        /**
         * Whether the store has a file. Some stores answer a missing one by
         * failing to open it rather than saying it isn't there.
         *
         * @param location the file
         * @return true if it has
         */
        private boolean exists(String location)
        {
            try
            {
                return io.newInputFile(location).exists();
            }
            catch (NotFoundException missing)
            {
                return false;
            }
        }
    }
}
