/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.BucketSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A prefix in an object store bucket (design §5.3.17), read through the
 * store's own SDK. The source's {@code path} and {@code move-to} are key
 * prefixes, {@code /} being the bucket's root, and {@code /} in a key
 * separates the names the listing rules see. Objects appear whole, so an
 * item is never read while it is still being written.
 *
 * <p>An item's version is its modification time and its tag (ETag or
 * generation). Reads are ranged, so a broken read resumes where it
 * stopped, and conditional on the tag, so a changed object is never read
 * as the version listed. {@code move} and {@code rename} copy, then
 * delete. Keys may hold PHI, so no key reaches a message or the log; the
 * SDKs' request logging is left off.
 *
 * <p>Subclasses give the store's calls; this class applies the listing
 * rules and the after-actions, and turns failures into
 * {@link IntakeUnavailableException}s.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public abstract class BucketSource implements RemoteSource
{

    /** Gives up looking for a free key to move an item to after this. */
    private static final int MAX_TARGET_TRIES = 1000;

    /**
     * An object found in the bucket.
     *
     * @param key      its key
     * @param size     its size in bytes
     * @param modified its modification time in milliseconds
     * @param tag      its ETag or generation
     */
    protected record Found(String key, long size, long modified, String tag)
    {
    }

    /** The source's settings. */
    protected final SourceSettings settings;

    /** The bucket's settings. */
    protected final BucketSettings bucket;

    /** What a listing picks up. */
    private final RemoteListing listing;

    /** The source's key prefix: empty for the root, else ending in /. */
    private final String prefix;

    /** Whether the last call to the store worked. */
    private volatile boolean reachable = true;

    /**
     * Creates the source.
     *
     * @param settings the source's settings, of a bucket type
     */
    protected BucketSource(SourceSettings settings)
    {
        this.settings = settings;
        this.bucket = settings.bucket();
        this.listing = new RemoteListing(settings);
        this.prefix = directory(settings.path());
    }

    /**
     * Lists the objects whose keys start with a prefix.
     *
     * @param keyPrefix the prefix; empty for the whole bucket
     * @param flat      whether to leave out keys with a {@code /} after
     *                  the prefix
     * @return the objects, in any order
     * @throws IOException if the store can't be listed
     */
    protected abstract List<Found> objects(String keyPrefix, boolean flat)
            throws IOException;

    /**
     * Looks an object up.
     *
     * @param key its key
     * @return the object; null if there is none
     * @throws IOException if the store can't be reached
     */
    protected abstract Found stat(String key) throws IOException;

    /**
     * Reads an object from an offset, if it still has the tag listed.
     *
     * @param key    its key
     * @param item   the item as listed
     * @param offset where to start, less than its size
     * @return its bytes from there
     * @throws ItemLander.ItemChangedException if it has gone or changed
     * @throws IOException                     if the store can't be read
     */
    protected abstract InputStream read(String key, ListedItem item,
            long offset) throws IOException;

    /**
     * Copies an object, if it still has a tag, to a key that is free.
     *
     * @param from   its key
     * @param tag    its tag
     * @param size   its size in bytes
     * @param target the key to copy it to
     * @return false if the object has gone or changed
     * @throws IOException if the copy fails
     */
    protected abstract boolean copy(String from, String tag, long size,
            String target) throws IOException;

    /**
     * Deletes an object, if it still has a tag, where the store can make
     * that a condition.
     *
     * @param key its key
     * @param tag its tag; null to delete whatever is there
     * @throws IOException if the delete fails
     */
    protected abstract void delete(String key, String tag)
            throws IOException;

    /**
     * Writes an empty object.
     *
     * @param key its key
     * @throws IOException if the write fails
     */
    protected abstract void putEmpty(String key) throws IOException;

    /**
     * Says why a call to the store failed.
     *
     * @param failure what the SDK threw
     * @return the problem
     */
    protected abstract IntakeProblem classify(Exception failure);

    /** Closes the SDK's client. */
    protected abstract void closeClient();

    /**
     * Bucket items are objects.
     *
     * @return {@link ItemKind#OBJECT}
     */
    @Override
    public ItemKind kind()
    {
        return ItemKind.OBJECT;
    }

    @Override
    public List<ListedItem> list() throws IOException
    {
        List<RemoteListing.Entry> files = new ArrayList<>();
        try
        {
            for (Found found : objects(prefix, !settings.recursive()))
            {
                if (!found.key().startsWith(prefix))
                {
                    continue;
                }
                String ref = found.key().substring(prefix.length());
                if (listing.inScope(ref))
                {
                    files.add(new RemoteListing.Entry(ref, found.size(),
                            found.modified(), found.tag()));
                }
            }
        }
        catch (IOException | RuntimeException failure)
        {
            throw failed(failure);
        }
        reachable = true;
        return listing.select(files);
    }

    @Override
    public InputStream open(ListedItem item, long offset) throws IOException
    {
        String key = key(listing.resolve(item.ref()));
        try
        {
            if (offset >= item.size())
            {
                // Nothing left to read; a ranged read would be refused.
                if (!same(stat(key), item))
                {
                    throw new ItemLander.ItemChangedException();
                }
                return InputStream.nullInputStream();
            }
            return new Watched(read(key, item, offset));
        }
        catch (ItemLander.ItemChangedException changed)
        {
            throw changed;
        }
        catch (IOException | RuntimeException failure)
        {
            throw failed(failure);
        }
    }

    @Override
    public void afterLanding(ListedItem item) throws IOException
    {
        if (settings.after() == SourceSettings.After.NONE)
        {
            return;
        }
        String key = key(listing.resolve(item.ref()));
        try
        {
            Found found = stat(key);
            if (!same(found, item))
            {
                return;
            }
            boolean done = switch (settings.after())
            {
                case DELETE -> true;
                case MOVE -> copy(key, found.tag(), found.size(),
                        free(key(listing.moveTarget(item.ref()))));
                case RENAME -> copy(key, found.tag(), found.size(),
                        free(key + settings.renameSuffix()));
                default -> throw new IllegalStateException(
                        "Unknown after-action");
            };
            if (!done)
            {
                return;
            }
            delete(key, found.tag());
            if (settings.marker())
            {
                String marker = key + SourceSettings.MARKER_SUFFIX;
                if (stat(marker) != null)
                {
                    delete(marker, null);
                }
            }
        }
        catch (IOException | RuntimeException failure)
        {
            throw failed(failure);
        }
    }

    @Override
    public boolean reachable()
    {
        return reachable;
    }

    @Override
    public void checkWritable() throws IOException
    {
        if (!settings.writable())
        {
            return;
        }
        probe(prefix, "sdh.landing.source.path");
        if (settings.after() == SourceSettings.After.MOVE)
        {
            probe(directory(settings.moveTo()),
                    "sdh.landing.source.move-to");
        }
    }

    @Override
    public void close()
    {
        closeClient();
    }

    /**
     * The key prefix for a directory setting.
     *
     * @param directory an absolute path, {@code /} being the bucket's root
     * @return the prefix: empty for the root, else ending in {@code /}
     */
    static String directory(Path directory)
    {
        String key = key(directory.toString());
        return key.isEmpty() ? key : key + "/";
    }

    /**
     * The key for an absolute path.
     *
     * @param path the path, {@code /} being the bucket's root
     * @return the key, without the leading {@code /}
     */
    static String key(String path)
    {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /**
     * Tells whether an object is still the version that was listed.
     *
     * @param found the object now; null if it has gone
     * @param item  the item as listed
     * @return true if it is there with the same size, time and tag
     */
    private static boolean same(Found found, ListedItem item)
    {
        return found != null && found.size() == item.size()
                && found.modified() == item.version()
                && found.tag().equals(item.tag());
    }

    /**
     * The target, or the target with {@code .1}, {@code .2} and so on
     * added, whichever is free.
     *
     * @param target where the item should go
     * @return a free key
     * @throws IOException if no key is free, or one can't be checked
     */
    private String free(String target) throws IOException
    {
        String free = target;
        for (int tries = 1; stat(free) != null; tries++)
        {
            if (tries > MAX_TARGET_TRIES)
            {
                throw new IOException("No free key to move the item to");
            }
            free = target + "." + tries;
        }
        return free;
    }

    /**
     * Writes and deletes a hidden object under a prefix.
     *
     * @param keyPrefix the prefix
     * @param setting   the setting that names it, for the message
     * @throws IntakeUnavailableException if the store can't be reached
     * @throws IntakeNotWritableException if it refuses either step
     */
    private void probe(String keyPrefix, String setting) throws IOException
    {
        String probe = keyPrefix + LocalDirectorySource.PROBE_PREFIX
                + UUID.randomUUID();
        try
        {
            putEmpty(probe);
            delete(probe, null);
        }
        catch (IOException | RuntimeException failure)
        {
            IntakeUnavailableException unavailable = failed(failure);
            if (unavailable.problem() == IntakeProblem.NETWORK)
            {
                throw unavailable;
            }
            throw new IntakeNotWritableException(setting, null);
        }
    }

    /**
     * Turns an SDK failure into one that says why, with no key.
     *
     * @param failure what the call threw
     * @return the exception to throw
     */
    private IntakeUnavailableException failed(Exception failure)
    {
        if (failure instanceof IntakeUnavailableException unavailable)
        {
            reachable = false;
            return unavailable;
        }
        reachable = false;
        return new IntakeUnavailableException(classify(failure), failure);
    }

    /**
     * An item's bytes; a failed read marks the store unreachable, so the
     * listing stops and the item is resumed later.
     */
    private final class Watched extends FilterInputStream
    {

        /**
         * Wraps the SDK's stream.
         *
         * @param input the stream
         */
        Watched(InputStream input)
        {
            super(input);
        }

        @Override
        public int read() throws IOException
        {
            try
            {
                return super.read();
            }
            catch (IOException | RuntimeException failure)
            {
                throw broken(failure);
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            try
            {
                return super.read(buffer, offset, length);
            }
            catch (IOException | RuntimeException failure)
            {
                throw broken(failure);
            }
        }

        /**
         * Closes the stream; a failure to close a broken one is ignored.
         */
        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            catch (IOException | RuntimeException failure)
            {
                // The read already failed, or there is nothing to keep.
            }
        }

        /**
         * Marks the store unreachable after a failed read.
         *
         * @param failure what the read threw
         * @return the exception to throw
         */
        private IOException broken(Exception failure)
        {
            reachable = false;
            return new IntakeUnavailableException(IntakeProblem.NETWORK,
                    failure);
        }
    }
}
