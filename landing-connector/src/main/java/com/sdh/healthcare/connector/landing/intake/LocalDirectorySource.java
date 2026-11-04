/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.UUID;

/**
 * A directory on a local or mounted file system (design §5.3.4). Items are
 * read in place; a read-write source has each item deleted, moved or
 * renamed once it is landed and announced, and its marker, if any,
 * deleted.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LocalDirectorySource implements IntakeSource
{

    /** What the startup probe's file name starts with: hidden, never listed. */
    static final String PROBE_PREFIX = ".sdh-probe-";

    /** The most names tried when a move or rename target is taken. */
    private static final int MAX_TARGET_TRIES = 1000;

    /** The source's settings. */
    private final SourceSettings settings;

    /**
     * Creates the source.
     *
     * @param settings the source's settings
     */
    public LocalDirectorySource(SourceSettings settings)
    {
        this.settings = settings;
    }

    @Override
    public InputStream open(ListedItem item, long offset) throws IOException
    {
        Path file = resolve(item);
        if (!sameVersion(file, item))
        {
            throw new ItemLander.ItemChangedException();
        }
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        try
        {
            channel.position(offset);
            return Channels.newInputStream(channel);
        }
        catch (IOException | RuntimeException failure)
        {
            channel.close();
            throw failure;
        }
    }

    @Override
    public void afterLanding(ListedItem item) throws IOException
    {
        if (settings.after() == SourceSettings.After.NONE)
        {
            return;
        }
        Path file = resolve(item);
        if (!sameVersion(file, item))
        {
            return;
        }
        switch (settings.after())
        {
            case DELETE -> Files.deleteIfExists(file);
            case MOVE ->
            {
                Path target = settings.moveTo().resolve(item.ref())
                        .normalize();
                if (!target.startsWith(settings.moveTo()))
                {
                    throw new IOException("Item is outside the source");
                }
                Files.createDirectories(target.getParent());
                move(file, target);
            }
            case RENAME -> move(file, file.resolveSibling(
                    file.getFileName() + settings.renameSuffix()));
            default -> throw new IllegalStateException(
                    "Unknown after-action");
        }
        if (settings.marker())
        {
            Files.deleteIfExists(file.resolveSibling(
                    file.getFileName() + SourceSettings.MARKER_SUFFIX));
        }
        Files.deleteIfExists(file.resolveSibling(
                file.getFileName() + SourceSettings.REPLACES_SUFFIX));
    }

    @Override
    public Optional<ListedItem> replacesMarker(ListedItem item)
            throws IOException
    {
        String ref = item.ref() + SourceSettings.REPLACES_SUFFIX;
        Path marker = resolve(new ListedItem(ref, 0, 0));
        if (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS))
        {
            return Optional.empty();
        }
        return Optional.of(new ListedItem(ref, Files.size(marker),
                Files.getLastModifiedTime(marker,
                        LinkOption.NOFOLLOW_LINKS).toMillis()));
    }

    @Override
    public boolean reachable()
    {
        return Files.isDirectory(settings.path())
                && Files.isReadable(settings.path());
    }

    @Override
    public void checkWritable() throws IntakeNotWritableException
    {
        if (!settings.writable())
        {
            return;
        }
        probe(settings.path(), "sdh.landing.source.path");
        if (settings.after() == SourceSettings.After.MOVE)
        {
            try
            {
                Files.createDirectories(settings.moveTo());
            }
            catch (IOException failure)
            {
                throw new IntakeNotWritableException(
                        "sdh.landing.source.move-to", failure);
            }
            probe(settings.moveTo(), "sdh.landing.source.move-to");
        }
    }

    /**
     * Creates and deletes a hidden file in a directory.
     *
     * @param directory the directory
     * @param setting   the setting that names it, for the message
     * @throws IntakeNotWritableException if either step fails
     */
    private static void probe(Path directory, String setting)
            throws IntakeNotWritableException
    {
        try
        {
            Path probe = Files.createFile(
                    directory.resolve(PROBE_PREFIX + UUID.randomUUID()));
            Files.delete(probe);
        }
        catch (IOException failure)
        {
            throw new IntakeNotWritableException(setting, failure);
        }
    }

    /**
     * The item's file, which must be inside the source.
     *
     * @param item the item
     * @return its path
     * @throws IOException if its reference leads outside the source
     */
    private Path resolve(ListedItem item) throws IOException
    {
        Path file = settings.path().resolve(item.ref()).normalize();
        if (!file.startsWith(settings.path()) || file.equals(settings.path()))
        {
            throw new IOException("Item is outside the source");
        }
        return file;
    }

    /**
     * Tells whether a file is still the version that was listed.
     *
     * @param file the file
     * @param item the item as listed
     * @return false if it has gone, or its size or time has changed
     * @throws IOException if its attributes can't be read
     */
    private static boolean sameVersion(Path file, ListedItem item)
            throws IOException
    {
        BasicFileAttributes attributes;
        try
        {
            attributes = Files.readAttributes(file,
                    BasicFileAttributes.class);
        }
        catch (NoSuchFileException gone)
        {
            return false;
        }
        return attributes.isRegularFile()
                && attributes.size() == item.size()
                && attributes.lastModifiedTime().toMillis() == item.version();
    }

    /**
     * Moves a file, atomically where the file system can, to the target
     * or, if that is taken, to the target with {@code .1}, {@code .2} and
     * so on added.
     *
     * @param file   the file
     * @param target where it should go
     * @throws IOException if it can't be moved
     */
    private static void move(Path file, Path target) throws IOException
    {
        Path free = target;
        for (int tries = 1; Files.exists(free); tries++)
        {
            if (tries > MAX_TARGET_TRIES)
            {
                throw new IOException("No free name to move the item to");
            }
            free = target.resolveSibling(target.getFileName() + "." + tries);
        }
        try
        {
            Files.move(file, free, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException crossesFileSystems)
        {
            Files.move(file, free);
        }
    }
}
