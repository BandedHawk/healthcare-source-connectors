/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.JSchHostKeyException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.SftpSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A directory on an SFTP server (design §5.3.4), through JSch. The host
 * key must be in the configured {@code known_hosts}; an unknown or changed
 * key is refused, and the file is read again at each connection, so an
 * operator can update it without a restart. JSch's own logging is off: it
 * would log names and paths.
 *
 * <p>SFTP gives modification times in whole seconds, so an item's version
 * is its time to the second.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SftpSource implements RemoteSource
{

    /** Gives up looking for a free name to move an item to after this. */
    private static final int MAX_TARGET_TRIES = 1000;

    /**
     * How many unanswered keep-alives end a connection. JSch uses the
     * keep-alive interval as the socket's timeout too, and gives up after
     * one more, so a stall is noticed after the interval times this plus
     * one.
     */
    private static final int SERVER_ALIVE_COUNT = 3;

    static
    {
        JSch.setLogger(null);
    }

    /** The source's settings. */
    private final SourceSettings settings;

    /** The server's settings. */
    private final SftpSettings server;

    /** What a listing picks up. */
    private final RemoteListing listing;

    /** The SSH session; null when not connected. */
    private Session session;

    /** The SFTP channel on it; null when not connected. */
    private ChannelSftp channel;

    /** Whether the last call to the server worked. */
    private volatile boolean reachable = true;

    /**
     * Creates the source; nothing connects until it is first used.
     *
     * @param settings the source's settings, of type SFTP
     */
    public SftpSource(SourceSettings settings)
    {
        this.settings = settings;
        this.server = settings.sftp();
        this.listing = new RemoteListing(settings);
    }

    @Override
    public synchronized List<ListedItem> list() throws IOException
    {
        ChannelSftp sftp = connect();
        List<RemoteListing.Entry> files = new ArrayList<>();
        try
        {
            walk(sftp, settings.path().toString(), "", files);
        }
        catch (SftpException failure)
        {
            throw failed(failure);
        }
        reachable = true;
        return listing.select(files);
    }

    /**
     * Collects the regular files in a directory, and in its subdirectories
     * if the source is recursive. Links are never followed.
     *
     * @param sftp      the channel
     * @param directory the directory on the server
     * @param ref       its path relative to the source; empty for the source
     * @param files     where the files go
     * @throws SftpException if the directory can't be listed
     */
    private void walk(ChannelSftp sftp, String directory, String ref,
            List<RemoteListing.Entry> files) throws SftpException
    {
        List<String> subdirectories = new ArrayList<>();
        sftp.ls(directory, entry ->
        {
            String name = entry.getFilename();
            SftpATTRS attributes = entry.getAttrs();
            if (name.equals(".") || name.equals("..")
                    || RemoteListing.hidden(name) || attributes.isLink())
            {
                return ChannelSftp.LsEntrySelector.CONTINUE;
            }
            String child = RemoteListing.join(ref, name);
            if (attributes.isDir())
            {
                if (listing.descend(child))
                {
                    subdirectories.add(name);
                }
            }
            else if (attributes.isReg())
            {
                files.add(new RemoteListing.Entry(child, attributes.getSize(),
                        modified(attributes)));
            }
            return ChannelSftp.LsEntrySelector.CONTINUE;
        });
        for (String name : subdirectories)
        {
            try
            {
                walk(sftp, RemoteListing.join(directory, name),
                        RemoteListing.join(ref, name), files);
            }
            catch (SftpException failure)
            {
                if (failure.id != ChannelSftp.SSH_FX_NO_SUCH_FILE)
                {
                    throw failure;
                }
                // Removed while being listed: nothing in it to pick up.
            }
        }
    }

    @Override
    public synchronized InputStream open(ListedItem item, long offset)
            throws IOException
    {
        ChannelSftp sftp = connect();
        String path = listing.resolve(item.ref());
        try
        {
            SftpATTRS attributes = attributes(sftp, path);
            if (attributes == null || !sameVersion(attributes, item))
            {
                throw new ItemLander.ItemChangedException();
            }
            return new Watched(sftp.get(path, null, offset));
        }
        catch (SftpException failure)
        {
            throw failed(failure);
        }
    }

    @Override
    public synchronized void afterLanding(ListedItem item) throws IOException
    {
        if (settings.after() == SourceSettings.After.NONE)
        {
            return;
        }
        ChannelSftp sftp = connect();
        String path = listing.resolve(item.ref());
        try
        {
            SftpATTRS attributes = attributes(sftp, path);
            if (attributes == null || !sameVersion(attributes, item))
            {
                return;
            }
            switch (settings.after())
            {
                case DELETE -> sftp.rm(path);
                case MOVE ->
                {
                    String target = listing.moveTarget(item.ref());
                    makeDirectories(sftp, Path.of(target).getParent());
                    sftp.rename(path, free(sftp, target));
                }
                case RENAME -> sftp.rename(path,
                        free(sftp, path + settings.renameSuffix()));
                default -> throw new IllegalStateException(
                        "Unknown after-action");
            }
            if (settings.marker())
            {
                String marker = path + SourceSettings.MARKER_SUFFIX;
                if (attributes(sftp, marker) != null)
                {
                    sftp.rm(marker);
                }
            }
        }
        catch (SftpException failure)
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
    public synchronized void checkWritable() throws IOException
    {
        if (!settings.writable())
        {
            return;
        }
        ChannelSftp sftp = connect();
        probe(sftp, settings.path().toString(), "sdh.landing.source.path");
        if (settings.after() == SourceSettings.After.MOVE)
        {
            try
            {
                makeDirectories(sftp, settings.moveTo());
            }
            catch (SftpException failure)
            {
                throw new IntakeNotWritableException(
                        "sdh.landing.source.move-to", null);
            }
            probe(sftp, settings.moveTo().toString(),
                    "sdh.landing.source.move-to");
        }
    }

    @Override
    public synchronized void close()
    {
        disconnect();
    }

    /**
     * The open channel, connecting first if there is none or it has
     * dropped.
     *
     * @return the channel
     * @throws IntakeUnavailableException if the server can't be reached or
     *                                    logged into, or its host key
     *                                    isn't the pinned one
     */
    private ChannelSftp connect() throws IntakeUnavailableException
    {
        if (channel != null && channel.isConnected() && session.isConnected())
        {
            return channel;
        }
        disconnect();
        try
        {
            JSch jsch = new JSch();
            jsch.setKnownHosts(server.knownHosts().toString());
            if (server.privateKey() != null)
            {
                jsch.addIdentity(server.privateKey().toString(),
                        server.privateKeyPassphrase() == null ? null
                                : server.privateKeyPassphrase()
                                        .getBytes(StandardCharsets.UTF_8));
            }
            Session opened = jsch.getSession(server.user(), server.host(),
                    server.port());
            opened.setConfig("StrictHostKeyChecking", "yes");
            opened.setConfig("PreferredAuthentications",
                    server.password() == null ? "publickey"
                            : server.privateKey() == null ? "password"
                                    : "publickey,password");
            if (server.password() != null)
            {
                opened.setPassword(server.password());
            }
            opened.setServerAliveInterval((int) Math.max(1,
                    server.readTimeout().toMillis()
                            / (SERVER_ALIVE_COUNT + 1)));
            opened.setServerAliveCountMax(SERVER_ALIVE_COUNT);
            opened.connect((int) server.connectTimeout().toMillis());
            session = opened;
            ChannelSftp sftp = (ChannelSftp) opened.openChannel("sftp");
            sftp.connect((int) server.connectTimeout().toMillis());
            channel = sftp;
            return sftp;
        }
        catch (JSchException failure)
        {
            throw failed(failure);
        }
    }

    /** Closes the channel and session, if open. */
    private void disconnect()
    {
        if (channel != null)
        {
            channel.disconnect();
            channel = null;
        }
        if (session != null)
        {
            session.disconnect();
            session = null;
        }
    }

    /**
     * Turns a library failure into one that says why, with no path, and
     * drops the connection if it may be broken.
     *
     * @param failure what JSch threw
     * @return the exception to throw
     */
    private IntakeUnavailableException failed(Exception failure)
    {
        IntakeProblem problem = classify(failure);
        reachable = false;
        if (problem == IntakeProblem.NETWORK)
        {
            disconnect();
        }
        return new IntakeUnavailableException(problem, failure);
    }

    /**
     * Says why an SFTP call failed.
     *
     * @param failure what JSch threw
     * @return the problem
     */
    static IntakeProblem classify(Exception failure)
    {
        if (failure instanceof JSchHostKeyException)
        {
            return IntakeProblem.HOST_KEY;
        }
        if (failure instanceof JSchException)
        {
            String message = String.valueOf(failure.getMessage());
            return message.startsWith("Auth fail")
                    || message.startsWith("Auth cancel")
                    || message.startsWith("invalid privatekey")
                    ? IntakeProblem.AUTH : IntakeProblem.NETWORK;
        }
        if (failure instanceof SftpException sftp)
        {
            return switch (sftp.id)
            {
                case ChannelSftp.SSH_FX_PERMISSION_DENIED ->
                    IntakeProblem.PERMISSION;
                case ChannelSftp.SSH_FX_NO_SUCH_FILE ->
                    IntakeProblem.NOT_FOUND;
                default -> IntakeProblem.NETWORK;
            };
        }
        return IntakeProblem.NETWORK;
    }

    /**
     * A file's attributes, without following a link.
     *
     * @param sftp the channel
     * @param path the file on the server
     * @return its attributes; null if it doesn't exist
     * @throws SftpException if they can't be read
     */
    private static SftpATTRS attributes(ChannelSftp sftp, String path)
            throws SftpException
    {
        try
        {
            return sftp.lstat(path);
        }
        catch (SftpException failure)
        {
            if (failure.id == ChannelSftp.SSH_FX_NO_SUCH_FILE)
            {
                return null;
            }
            throw failure;
        }
    }

    /**
     * Tells whether a file is still the version that was listed.
     *
     * @param attributes the file's attributes now
     * @param item       the item as listed
     * @return false if it isn't a regular file, or its size or time has
     *         changed
     */
    private static boolean sameVersion(SftpATTRS attributes, ListedItem item)
    {
        return attributes.isReg() && attributes.getSize() == item.size()
                && modified(attributes) == item.version();
    }

    /**
     * A file's modification time.
     *
     * @param attributes its attributes
     * @return the time in milliseconds, to the second
     */
    private static long modified(SftpATTRS attributes)
    {
        return Integer.toUnsignedLong(attributes.getMTime()) * 1000L;
    }

    /**
     * Creates a directory and any missing parents.
     *
     * @param sftp      the channel
     * @param directory the directory on the server
     * @throws SftpException if one can't be created
     */
    private static void makeDirectories(ChannelSftp sftp, Path directory)
            throws SftpException
    {
        if (directory == null || attributes(sftp, directory.toString()) != null)
        {
            return;
        }
        makeDirectories(sftp, directory.getParent());
        sftp.mkdir(directory.toString());
    }

    /**
     * The target, or the target with {@code .1}, {@code .2} and so on
     * added, whichever is free. SFTP's rename never replaces a file, so a
     * name taken in between makes the rename fail rather than overwrite.
     *
     * @param sftp   the channel
     * @param target where the item should go
     * @return a free path
     * @throws SftpException if a name can't be checked
     * @throws IOException   if no name is free
     */
    private static String free(ChannelSftp sftp, String target)
            throws SftpException, IOException
    {
        String free = target;
        for (int tries = 1; attributes(sftp, free) != null; tries++)
        {
            if (tries > MAX_TARGET_TRIES)
            {
                throw new IOException("No free name to move the item to");
            }
            free = target + "." + tries;
        }
        return free;
    }

    /**
     * Creates and deletes a hidden file in a directory.
     *
     * @param sftp      the channel
     * @param directory the directory on the server
     * @param setting   the setting that names it, for the message
     * @throws IntakeNotWritableException if either step fails
     */
    private static void probe(ChannelSftp sftp, String directory,
            String setting) throws IntakeNotWritableException
    {
        String probe = RemoteListing.join(directory,
                LocalDirectorySource.PROBE_PREFIX + UUID.randomUUID());
        try
        {
            sftp.put(new ByteArrayInputStream(new byte[0]), probe);
            sftp.rm(probe);
        }
        catch (SftpException failure)
        {
            throw new IntakeNotWritableException(setting, null);
        }
    }

    /**
     * An item's bytes, which drop the connection if a read fails, so the
     * next call makes a new one.
     */
    private final class Watched extends FilterInputStream
    {

        /**
         * Wraps the channel's stream.
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
            catch (IOException failure)
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
            catch (IOException failure)
            {
                throw broken(failure);
            }
        }

        /**
         * Closes the stream. After the connection has dropped, JSch fails
         * to close it cleanly; there is nothing left to close, so that is
         * ignored and the connection dropped.
         */
        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            catch (IOException failure)
            {
                synchronized (SftpSource.this)
                {
                    disconnect();
                }
            }
        }

        /**
         * Drops the connection after a failed read.
         *
         * @param failure what the read threw
         * @return the exception to throw
         */
        private IOException broken(IOException failure)
        {
            synchronized (SftpSource.this)
            {
                reachable = false;
                disconnect();
            }
            return new IntakeUnavailableException(IntakeProblem.NETWORK,
                    failure);
        }
    }
}
