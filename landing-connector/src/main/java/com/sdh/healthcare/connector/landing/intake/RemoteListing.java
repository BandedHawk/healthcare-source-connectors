/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.util.AntPathMatcher;

/**
 * What a remote source picks up from its listing, by the same rules the
 * directory intake gives Camel's file component: hidden names are left
 * out, and so are markers, renamed items and the {@code move-to}
 * directory; the include pattern is matched against the path relative to
 * the source, Ant-style; items come oldest first. Paths are on the server,
 * with {@code /} between names.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RemoteListing
{

    /**
     * A file found on the server.
     *
     * @param ref      its path relative to the source
     * @param size     its size in bytes
     * @param modified its modification time in milliseconds
     * @param tag      its ETag or generation, on an object store; null
     *                 otherwise
     */
    record Entry(String ref, long size, long modified, String tag)
    {

        /**
         * A file with no tag.
         *
         * @param ref      its path relative to the source
         * @param size     its size in bytes
         * @param modified its modification time in milliseconds
         */
        Entry(String ref, long size, long modified)
        {
            this(ref, size, modified, null);
        }
    }

    /** The source's settings. */
    private final SourceSettings settings;

    /**
     * The replaces markers the last listing found, by the item they sit
     * beside.
     */
    private Map<String, ListedItem> replacesMarkers = Map.of();

    /** Matches the include pattern. */
    private final AntPathMatcher matcher = new AntPathMatcher("/");

    /** The move-to directory relative to the source, if it is inside. */
    private final String moveToInside;

    /**
     * Creates the rules for a source.
     *
     * @param settings the source's settings
     */
    RemoteListing(SourceSettings settings)
    {
        this.settings = settings;
        Path moveTo = settings.moveTo();
        this.moveToInside = moveTo != null
                && moveTo.startsWith(settings.path())
                ? settings.path().relativize(moveTo).toString() : null;
    }

    /**
     * Tells whether a name is hidden, and so never picked up or looked in.
     * Probe files are hidden.
     *
     * @param name a file or directory name
     * @return true if it starts with a dot
     */
    static boolean hidden(String name)
    {
        return name.startsWith(".");
    }

    /**
     * Tells whether to look in a subdirectory.
     *
     * @param ref the directory's path relative to the source
     * @return false if the source isn't recursive, or the directory is
     *         the {@code move-to} directory
     */
    boolean descend(String ref)
    {
        return settings.recursive() && !ref.equals(moveToInside);
    }

    /**
     * Tells whether a key from a flat listing of an object store is a file
     * a walk of the source would find: no name on its path is empty or
     * hidden, and each directory on it is one the walk would look in.
     *
     * @param ref the key relative to the source
     * @return true if the source looks at it
     */
    boolean inScope(String ref)
    {
        String[] names = ref.split("/", -1);
        StringBuilder directory = new StringBuilder();
        for (int i = 0; i < names.length; i++)
        {
            if (names[i].isEmpty() || hidden(names[i]))
            {
                return false;
            }
            if (i < names.length - 1)
            {
                if (i > 0)
                {
                    directory.append('/');
                }
                directory.append(names[i]);
                if (!descend(directory.toString()))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Picks the items from the files found.
     *
     * @param files every regular file found, with hidden ones left out
     * @return the items to offer, oldest first
     */
    List<ListedItem> select(List<Entry> files)
    {
        Map<String, ListedItem> replacing = new HashMap<>();
        for (Entry file : files)
        {
            if (file.ref().endsWith(SourceSettings.REPLACES_SUFFIX))
            {
                replacing.put(file.ref().substring(0, file.ref().length()
                        - SourceSettings.REPLACES_SUFFIX.length()),
                        new ListedItem(file.ref(), file.size(),
                                file.modified(), file.tag()));
            }
        }
        replacesMarkers = replacing;
        Set<String> markers = new HashSet<>();
        if (settings.marker())
        {
            for (Entry file : files)
            {
                if (file.ref().endsWith(SourceSettings.MARKER_SUFFIX))
                {
                    markers.add(file.ref());
                }
            }
        }
        List<Entry> picked = new ArrayList<>();
        for (Entry file : files)
        {
            if (wanted(file.ref())
                    && (!settings.marker() || markers.contains(
                            file.ref() + SourceSettings.MARKER_SUFFIX)))
            {
                picked.add(file);
            }
        }
        picked.sort(Comparator.comparingLong(Entry::modified)
                .thenComparing(Entry::ref));
        return picked.stream().map(file -> new ListedItem(file.ref(),
                file.size(), file.modified(), file.tag())).toList();
    }

    /**
     * The replaces marker the last listing found beside an item.
     *
     * @param itemRef the item's path relative to the source
     * @return the marker; empty if there was none
     */
    Optional<ListedItem> replacesMarker(String itemRef)
    {
        return Optional.ofNullable(replacesMarkers.get(itemRef));
    }

    /**
     * Tells whether a file is an item to pick up.
     *
     * @param ref its path relative to the source
     * @return false for a marker, a renamed item, or one the include
     *         pattern leaves out
     */
    private boolean wanted(String ref)
    {
        if (ref.endsWith(SourceSettings.REPLACES_SUFFIX))
        {
            return false;
        }
        if (settings.marker() && ref.endsWith(SourceSettings.MARKER_SUFFIX))
        {
            return false;
        }
        if (settings.after() == SourceSettings.After.RENAME
                && ref.endsWith(settings.renameSuffix()))
        {
            return false;
        }
        return matcher.match(settings.include(), ref);
    }

    /**
     * Joins a directory and a name.
     *
     * @param directory a path, or empty for the source itself
     * @param name      a name in it
     * @return the joined path
     */
    static String join(String directory, String name)
    {
        return directory.isEmpty() ? name
                : directory.endsWith("/") ? directory + name
                        : directory + "/" + name;
    }

    /**
     * The path on the server of an item, which must be inside the source.
     *
     * @param ref the item's path relative to the source
     * @return its path on the server
     * @throws IOException if the reference leads outside the source
     */
    String resolve(String ref) throws IOException
    {
        Path file = settings.path().resolve(ref).normalize();
        if (!file.startsWith(settings.path()) || file.equals(settings.path()))
        {
            throw new IOException("Item is outside the source");
        }
        return file.toString();
    }

    /**
     * Where {@code move} puts an item, which must be inside the
     * {@code move-to} directory.
     *
     * @param ref the item's path relative to the source
     * @return the target path on the server
     * @throws IOException if the reference leads outside it
     */
    String moveTarget(String ref) throws IOException
    {
        Path target = settings.moveTo().resolve(ref).normalize();
        if (!target.startsWith(settings.moveTo()))
        {
            throw new IOException("Item is outside the source");
        }
        return target.toString();
    }
}
