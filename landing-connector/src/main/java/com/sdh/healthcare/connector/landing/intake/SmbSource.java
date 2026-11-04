/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.security.bc.BCSecurityProvider;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.common.SMBRuntimeException;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.hierynomus.smbj.share.Share;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.SmbSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;

/**
 * A directory on an SMB share (design §5.3.4), through smbj. Only SMB 3 is
 * offered, and the session must be encrypted: smbj treats encryption as a
 * preference, so after logging in the source checks that every message
 * will be encrypted, and refuses the share otherwise. A guest or anonymous
 * session, which Samba can give for a bad login, is refused too.
 *
 * <p>Paths are within the share, {@code /} being its root.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SmbSource implements RemoteSource
{

    /** Gives up looking for a free name to move an item to after this. */
    private static final int MAX_TARGET_TRIES = 1000;

    /** The most asked for in one read. */
    private static final int MAX_READ = 1 << 20;

    /** The source's settings. */
    private final SourceSettings settings;

    /** The share's settings. */
    private final SmbSettings server;

    /** What a listing picks up. */
    private final RemoteListing listing;

    /** The client; null when not connected. */
    private SMBClient client;

    /** The connection; null when not connected. */
    private Connection connection;

    /** The share; null when not connected. */
    private DiskShare share;

    /** Whether the last call to the server worked. */
    private volatile boolean reachable = true;

    /**
     * Creates the source; nothing connects until it is first used.
     *
     * @param settings the source's settings, of type SMB
     */
    public SmbSource(SourceSettings settings)
    {
        this.settings = settings;
        this.server = settings.smb();
        this.listing = new RemoteListing(settings);
    }

    @Override
    public synchronized List<ListedItem> list() throws IOException
    {
        DiskShare disk = connect();
        List<RemoteListing.Entry> files = new ArrayList<>();
        try
        {
            walk(disk, onShare(settings.path()), "", files, true);
        }
        catch (SMBRuntimeException failure)
        {
            throw failed(failure);
        }
        reachable = true;
        return listing.select(files);
    }

    /**
     * Collects the regular files in a directory, and in its subdirectories
     * if the source is recursive. Reparse points (links, junctions) are
     * never followed.
     *
     * @param disk      the share
     * @param directory the directory within the share
     * @param ref       its path relative to the source; empty for the source
     * @param files     where the files go
     * @param top       whether it is the source itself, which must exist
     */
    private void walk(DiskShare disk, String directory, String ref,
            List<RemoteListing.Entry> files, boolean top)
    {
        List<FileIdBothDirectoryInformation> entries;
        try
        {
            entries = disk.list(directory);
        }
        catch (SMBApiException failure)
        {
            if (!top && classify(failure) == IntakeProblem.NOT_FOUND)
            {
                // Removed while being listed: nothing in it to pick up.
                return;
            }
            throw failure;
        }
        for (FileIdBothDirectoryInformation entry : entries)
        {
            String name = entry.getFileName();
            long attributes = entry.getFileAttributes();
            if (name.equals(".") || name.equals("..")
                    || RemoteListing.hidden(name)
                    || has(attributes,
                            FileAttributes.FILE_ATTRIBUTE_REPARSE_POINT))
            {
                continue;
            }
            String child = RemoteListing.join(ref, name);
            if (has(attributes, FileAttributes.FILE_ATTRIBUTE_DIRECTORY))
            {
                if (listing.descend(child))
                {
                    walk(disk, RemoteListing.join(directory, name), child,
                            files, false);
                }
            }
            else
            {
                files.add(new RemoteListing.Entry(child, entry.getEndOfFile(),
                        entry.getLastWriteTime().toEpochMillis()));
            }
        }
    }

    @Override
    public synchronized InputStream open(ListedItem item, long offset)
            throws IOException
    {
        DiskShare disk = connect();
        String path = onShare(Path.of(listing.resolve(item.ref())));
        File file;
        try
        {
            file = disk.openFile(path, EnumSet.of(AccessMask.GENERIC_READ),
                    null, SMB2ShareAccess.ALL,
                    SMB2CreateDisposition.FILE_OPEN, null);
        }
        catch (SMBApiException failure)
        {
            if (classify(failure) == IntakeProblem.NOT_FOUND)
            {
                throw new ItemLander.ItemChangedException();
            }
            throw failed(failure);
        }
        catch (SMBRuntimeException failure)
        {
            throw failed(failure);
        }
        try
        {
            FileAllInformation information = file.getFileInformation();
            if (information.getStandardInformation().isDirectory()
                    || information.getStandardInformation().getEndOfFile()
                            != item.size()
                    || information.getBasicInformation().getLastWriteTime()
                            .toEpochMillis() != item.version())
            {
                throw new ItemLander.ItemChangedException();
            }
            int chunk = Math.min(MAX_READ,
                    connection.getNegotiatedProtocol().getMaxReadSize());
            return new Reader(file, offset, chunk);
        }
        catch (IOException | RuntimeException failure)
        {
            closeQuietly(file);
            if (failure instanceof SMBRuntimeException smb)
            {
                throw failed(smb);
            }
            throw failure;
        }
    }

    @Override
    public synchronized void afterLanding(ListedItem item) throws IOException
    {
        if (settings.after() == SourceSettings.After.NONE)
        {
            return;
        }
        DiskShare disk = connect();
        Path local = Path.of(listing.resolve(item.ref()));
        String path = onShare(local);
        try
        {
            if (!sameVersion(disk, path, item))
            {
                return;
            }
            switch (settings.after())
            {
                case DELETE -> disk.rm(path);
                case MOVE ->
                {
                    Path target = Path.of(listing.moveTarget(item.ref()));
                    makeDirectories(disk, target.getParent());
                    rename(disk, path, free(disk, onShare(target)));
                }
                case RENAME -> rename(disk, path,
                        free(disk, path + settings.renameSuffix()));
                default -> throw new IllegalStateException(
                        "Unknown after-action");
            }
            if (settings.marker())
            {
                String marker = path + SourceSettings.MARKER_SUFFIX;
                if (disk.fileExists(marker))
                {
                    disk.rm(marker);
                }
            }
            String replaces = path + SourceSettings.REPLACES_SUFFIX;
            if (disk.fileExists(replaces))
            {
                disk.rm(replaces);
            }
        }
        catch (SMBRuntimeException failure)
        {
            throw failed(failure);
        }
    }

    @Override
    public Optional<ListedItem> replacesMarker(ListedItem item)
    {
        return listing.replacesMarker(item.ref());
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
        DiskShare disk = connect();
        probe(disk, onShare(settings.path()), "sdh.landing.source.path");
        if (settings.after() == SourceSettings.After.MOVE)
        {
            try
            {
                makeDirectories(disk, settings.moveTo());
            }
            catch (SMBRuntimeException failure)
            {
                throw new IntakeNotWritableException(
                        "sdh.landing.source.move-to", null);
            }
            probe(disk, onShare(settings.moveTo()),
                    "sdh.landing.source.move-to");
        }
    }

    @Override
    public synchronized void close()
    {
        disconnect();
    }

    /**
     * The open share, connecting first if there is none or it has dropped.
     *
     * @return the share
     * @throws IntakeUnavailableException if the server can't be reached or
     *                                    logged into, won't encrypt, or the
     *                                    share isn't there
     */
    private DiskShare connect() throws IntakeUnavailableException
    {
        if (share != null && share.isConnected())
        {
            return share;
        }
        disconnect();
        try
        {
            SmbConfig config = SmbConfig.builder()
                    .withDialects(SMB2Dialect.SMB_3_1_1,
                            SMB2Dialect.SMB_3_0_2, SMB2Dialect.SMB_3_0)
                    .withEncryptData(true)
                    .withDfsEnabled(false)
                    .withSecurityProvider(new BCSecurityProvider())
                    .withSocketFactory(new TimedSocketFactory(
                            (int) server.connectTimeout().toMillis()))
                    .withTimeout(server.readTimeout().toMillis(),
                            TimeUnit.MILLISECONDS)
                    .build();
            client = new SMBClient(config);
            connection = client.connect(server.host(), server.port());
            Session session = connection.authenticate(
                    new AuthenticationContext(server.user(),
                            server.password().toCharArray(), server.domain()));
            if (session.isGuest() || session.isAnonymous())
            {
                throw new IntakeUnavailableException(IntakeProblem.AUTH, null);
            }
            if (!session.shouldEncryptData())
            {
                throw new IntakeUnavailableException(IntakeProblem.ENCRYPTION,
                        null);
            }
            Share connected = session.connectShare(server.share());
            if (!(connected instanceof DiskShare disk))
            {
                closeQuietly(connected);
                throw new IntakeUnavailableException(IntakeProblem.NOT_FOUND,
                        null);
            }
            share = disk;
            return disk;
        }
        catch (IntakeUnavailableException failure)
        {
            reachable = false;
            disconnect();
            throw failure;
        }
        catch (IOException | SMBRuntimeException failure)
        {
            IntakeUnavailableException unavailable = failed(failure);
            disconnect();
            throw unavailable;
        }
    }

    /** Closes the share, the connection and the client, if open. */
    private void disconnect()
    {
        closeQuietly(share);
        share = null;
        if (connection != null)
        {
            try
            {
                connection.close(true);
            }
            catch (IOException | RuntimeException ignored)
            {
                // Already broken: nothing left to close cleanly.
            }
            connection = null;
        }
        if (client != null)
        {
            client.close();
            client = null;
        }
    }

    /**
     * Turns a library failure into one that says why, with no path, and
     * drops the connection if it may be broken.
     *
     * @param failure what smbj threw
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
     * Says why an SMB call failed.
     *
     * @param failure what smbj threw
     * @return the problem
     */
    static IntakeProblem classify(Exception failure)
    {
        if (failure instanceof SMBApiException api)
        {
            NtStatus status = api.getStatus();
            if (status == null)
            {
                return IntakeProblem.NETWORK;
            }
            return switch (status)
            {
                case STATUS_LOGON_FAILURE, STATUS_ACCOUNT_DISABLED,
                        STATUS_PASSWORD_EXPIRED -> IntakeProblem.AUTH;
                case STATUS_ACCESS_DENIED -> IntakeProblem.PERMISSION;
                case STATUS_BAD_NETWORK_NAME, STATUS_OBJECT_NAME_NOT_FOUND,
                        STATUS_OBJECT_PATH_NOT_FOUND, STATUS_NO_SUCH_FILE,
                        STATUS_NOT_FOUND -> IntakeProblem.NOT_FOUND;
                default -> IntakeProblem.NETWORK;
            };
        }
        // Transport failures and timeouts: the connection is gone.
        return IntakeProblem.NETWORK;
    }

    /**
     * A path within the share, as smbj takes it.
     *
     * @param path an absolute path, {@code /} being the share's root
     * @return the path without its leading {@code /}; empty for the root
     */
    private static String onShare(Path path)
    {
        return path.toString().substring(1);
    }

    /**
     * Tells whether a file is still the version that was listed.
     *
     * @param disk the share
     * @param path the file within the share
     * @param item the item as listed
     * @return false if it has gone, isn't a file, or its size or time has
     *         changed
     */
    private static boolean sameVersion(DiskShare disk, String path,
            ListedItem item)
    {
        if (!disk.fileExists(path))
        {
            return false;
        }
        FileAllInformation information = disk.getFileInformation(path);
        return !information.getStandardInformation().isDirectory()
                && information.getStandardInformation().getEndOfFile()
                        == item.size()
                && information.getBasicInformation().getLastWriteTime()
                        .toEpochMillis() == item.version();
    }

    /**
     * Renames a file within the share, never replacing another.
     *
     * @param disk   the share
     * @param path   the file
     * @param target its new path
     */
    private static void rename(DiskShare disk, String path, String target)
    {
        try (File file = disk.openFile(path,
                EnumSet.of(AccessMask.DELETE, AccessMask.GENERIC_READ), null,
                SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null))
        {
            file.rename(target, false);
        }
    }

    /**
     * Creates a directory and any missing parents.
     *
     * @param disk      the share
     * @param directory the directory, {@code /} being the share's root
     */
    private static void makeDirectories(DiskShare disk, Path directory)
    {
        if (directory == null || directory.getNameCount() == 0
                || disk.folderExists(onShare(directory)))
        {
            return;
        }
        makeDirectories(disk, directory.getParent());
        disk.mkdir(onShare(directory));
    }

    /**
     * The target, or the target with {@code .1}, {@code .2} and so on
     * added, whichever is free. The rename never replaces a file, so a
     * name taken in between makes it fail rather than overwrite.
     *
     * @param disk   the share
     * @param target where the item should go
     * @return a free path
     * @throws IOException if no name is free
     */
    private static String free(DiskShare disk, String target)
            throws IOException
    {
        String free = target;
        for (int tries = 1; disk.fileExists(free) || disk.folderExists(free);
                tries++)
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
     * @param disk      the share
     * @param directory the directory within the share
     * @param setting   the setting that names it, for the message
     * @throws IntakeNotWritableException if either step fails
     */
    private static void probe(DiskShare disk, String directory,
            String setting) throws IntakeNotWritableException
    {
        String probe = RemoteListing.join(directory,
                LocalDirectorySource.PROBE_PREFIX + UUID.randomUUID());
        try
        {
            disk.openFile(probe, EnumSet.of(AccessMask.GENERIC_WRITE),
                    EnumSet.of(FileAttributes.FILE_ATTRIBUTE_HIDDEN),
                    SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_CREATE,
                    null).close();
            disk.rm(probe);
        }
        catch (SMBRuntimeException failure)
        {
            throw new IntakeNotWritableException(setting, null);
        }
    }

    /**
     * Tells whether an attribute is set.
     *
     * @param attributes the attributes, as smbj gives them
     * @param attribute  the attribute
     * @return true if it is set
     */
    private static boolean has(long attributes, FileAttributes attribute)
    {
        return (attributes & attribute.getValue()) != 0;
    }

    /**
     * Closes a share or file, ignoring failures: the connection may
     * already be gone.
     *
     * @param closeable what to close; may be null
     */
    private static void closeQuietly(AutoCloseable closeable)
    {
        if (closeable == null)
        {
            return;
        }
        try
        {
            closeable.close();
        }
        catch (Exception ignored)
        {
            // Already broken: nothing left to close cleanly.
        }
    }

    /**
     * Makes sockets that give up connecting after a time, which smbj's
     * default doesn't.
     */
    private static final class TimedSocketFactory extends SocketFactory
    {

        /** How long to wait for a connection, in milliseconds. */
        private final int timeout;

        /**
         * Creates the factory.
         *
         * @param timeout how long to wait for a connection, in milliseconds
         */
        TimedSocketFactory(int timeout)
        {
            this.timeout = timeout;
        }

        @Override
        public Socket createSocket()
        {
            return new Socket();
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException
        {
            Socket socket = new Socket();
            try
            {
                socket.connect(new InetSocketAddress(host, port), timeout);
                return socket;
            }
            catch (IOException failure)
            {
                socket.close();
                throw failure;
            }
        }

        @Override
        public Socket createSocket(String host, int port,
                InetAddress localAddress, int localPort) throws IOException
        {
            return createSocket(host, port);
        }

        @Override
        public Socket createSocket(InetAddress address, int port)
                throws IOException
        {
            return createSocket(address.getHostAddress(), port);
        }

        @Override
        public Socket createSocket(InetAddress address, int port,
                InetAddress localAddress, int localPort) throws IOException
        {
            return createSocket(address.getHostAddress(), port);
        }
    }

    /**
     * An item's bytes, read from an offset. A failed read drops the
     * connection, so the next call makes a new one.
     */
    private final class Reader extends InputStream
    {

        /** The open file. */
        private final File file;

        /** The most asked for in one read. */
        private final int chunk;

        /** Where the next read starts. */
        private long position;

        /** Whether the end has been reached. */
        private boolean ended;

        /**
         * Starts reading a file.
         *
         * @param file   the open file
         * @param offset where to start
         * @param chunk  the most to ask for in one read
         */
        Reader(File file, long offset, int chunk)
        {
            this.file = file;
            this.position = offset;
            this.chunk = chunk;
        }

        @Override
        public int read() throws IOException
        {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            if (length == 0)
            {
                return 0;
            }
            if (ended)
            {
                return -1;
            }
            int read;
            try
            {
                read = file.read(buffer, position, offset,
                        Math.min(length, chunk));
            }
            catch (SMBRuntimeException failure)
            {
                synchronized (SmbSource.this)
                {
                    reachable = false;
                    disconnect();
                }
                throw new IntakeUnavailableException(IntakeProblem.NETWORK,
                        failure);
            }
            if (read < 0)
            {
                ended = true;
                return -1;
            }
            position += read;
            return read;
        }

        @Override
        public void close()
        {
            closeQuietly(file);
        }
    }
}
