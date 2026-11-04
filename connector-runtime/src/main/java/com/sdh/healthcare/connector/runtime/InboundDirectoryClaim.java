/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Properties;

/**
 * Ties an inbound directory to one node (design §9.5). The lease stops a
 * second instance with the same lease name; this stops a second node
 * configured with a different lease name, catalog or namespace on the same
 * directory, which would otherwise pick up the same files.
 * <p>
 * The claim is a hidden file, {@value #FILE}, in the inbound directory,
 * recording which node owns it. It holds no PHI. It is created atomically
 * ({@code CREATE_NEW}), so of two nodes starting together exactly one wins.
 * It stays after shutdown: the directory still belongs to its node, which
 * may have a file in progress there. To hand a directory to another node,
 * stop the old one and delete the file.
 * <p>
 * {@link #claim} runs at startup, after the lease is taken; {@link #verify}
 * runs before every file, so a running node stops if its claim is
 * replaced. A deleted claim is created again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboundDirectoryClaim
{

    /** The claim file's name. */
    public static final String FILE = ".sdh-owner";

    /** The claim file's format version. */
    private static final String VERSION = "1";

    /**
     * How many times to read a claim another node may still be writing
     * before treating it as unreadable.
     */
    private static final int READ_ATTEMPTS = 10;

    /** The pause between those reads. */
    private static final long READ_PAUSE_MS = 50;

    /**
     * A node's identity: everything that, if different, makes it a
     * different node. No PHI.
     *
     * @param connectorId its ID
     * @param leaseName   its lease's name
     * @param catalogName its catalog's name
     * @param namespace   its namespace
     */
    public record Owner(String connectorId, String leaseName,
            String catalogName, String namespace)
    {

        /**
         * The identity as the claim file's properties.
         *
         * @return the properties, with the format version
         */
        Properties toProperties()
        {
            Properties properties = new Properties();
            properties.setProperty("version", VERSION);
            properties.setProperty("connector-id", connectorId);
            properties.setProperty("lease-name", leaseName);
            properties.setProperty("catalog-name", catalogName);
            properties.setProperty("namespace", namespace);
            return properties;
        }

        /**
         * Reads an identity from a claim file's properties.
         *
         * @param properties the properties
         * @return the identity
         */
        static Owner of(Properties properties)
        {
            return new Owner(properties.getProperty("connector-id"),
                    properties.getProperty("lease-name"),
                    properties.getProperty("catalog-name"),
                    properties.getProperty("namespace"));
        }

        /**
         * Describes the identity, without PHI, for logs and the startup
         * error.
         *
         * @return the description
         */
        public String describe()
        {
            return "connector-id=" + connectorId + " lease-name=" + leaseName
                    + " catalog-name=" + catalogName + " namespace="
                    + namespace;
        }
    }

    /**
     * The directory belongs to another node, or its claim can't be read.
     * Stops the node.
     */
    public static final class ClaimedException extends RuntimeException
    {

        /** The node that holds the claim; null if it couldn't be read. */
        private final transient Owner holder;

        /**
         * Creates the exception, without a stack trace.
         *
         * @param message what happened, without PHI
         * @param holder  the node that holds the claim; null if unknown
         */
        ClaimedException(String message, Owner holder)
        {
            super(message, null, false, false);
            this.holder = holder;
        }

        /**
         * The node that holds the claim.
         *
         * @return it; null if the claim file couldn't be read
         */
        public Owner holder()
        {
            return holder;
        }

        /**
         * The error code to report.
         *
         * @return {@link ErrorCode#INBOUND_DIR_CLAIMED}
         */
        public ErrorCode code()
        {
            return ErrorCode.INBOUND_DIR_CLAIMED;
        }
    }

    /** The claim file. */
    private final Path file;

    /** This node's identity. */
    private final Owner owner;

    /**
     * Creates a claim.
     *
     * @param file  the claim file
     * @param owner this node's identity
     */
    private InboundDirectoryClaim(Path file, Owner owner)
    {
        this.file = file;
        this.owner = owner;
    }

    /**
     * Claims a directory for a node, or confirms an existing claim is its
     * own.
     *
     * @param directory the inbound directory
     * @param owner     the node's identity
     * @return the claim
     * @throws IOException      if the claim file can't be read or written
     * @throws ClaimedException if another node holds the directory
     */
    public static InboundDirectoryClaim claim(Path directory, Owner owner)
            throws IOException
    {
        InboundDirectoryClaim claim =
                new InboundDirectoryClaim(directory.resolve(FILE), owner);
        claim.verify();
        return claim;
    }

    /**
     * Checks that the directory still belongs to this node.
     *
     * @throws IOException      if the claim file can't be read or written
     * @throws ClaimedException if it now belongs to another node
     */
    public void verify() throws IOException
    {
        Owner holder = readOrCreate();
        if (!holder.equals(owner))
        {
            throw new ClaimedException("The inbound directory belongs to"
                    + " another connector (" + holder.describe() + "). Only"
                    + " one connector may use a directory. If that connector"
                    + " is retired, stop it and delete " + FILE
                    + " in the inbound directory.", holder);
        }
    }

    /**
     * This node's identity.
     *
     * @return the identity
     */
    public Owner owner()
    {
        return owner;
    }

    /**
     * Reads the claim, creating it for this node if there is none.
     *
     * @return the node that holds it
     * @throws IOException      if it can't be read or written
     * @throws ClaimedException if it stays incomplete
     */
    private Owner readOrCreate() throws IOException
    {
        for (int attempt = 1; ; attempt++)
        {
            Properties properties = new Properties();
            try (Reader reader =
                    Files.newBufferedReader(file, StandardCharsets.UTF_8))
            {
                properties.load(reader);
            }
            catch (NoSuchFileException missing)
            {
                if (create())
                {
                    return owner;
                }
                continue; // another node created it first: read theirs
            }
            if (VERSION.equals(properties.getProperty("version"))
                    && complete(properties))
            {
                return Owner.of(properties);
            }
            // Empty or partial: possibly still being written by whoever
            // created it.
            if (attempt >= READ_ATTEMPTS)
            {
                throw new ClaimedException("The inbound directory's " + FILE
                        + " can't be read. Delete it if no other connector"
                        + " uses this directory.", null);
            }
            try
            {
                Thread.sleep(READ_PAUSE_MS);
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "interrupted while reading the directory claim",
                        interrupted);
            }
        }
    }

    /**
     * Tells whether a claim file has every identity property.
     *
     * @param properties its properties
     * @return true if none is missing
     */
    private static boolean complete(Properties properties)
    {
        return properties.getProperty("connector-id") != null
                && properties.getProperty("lease-name") != null
                && properties.getProperty("catalog-name") != null
                && properties.getProperty("namespace") != null;
    }

    /**
     * Writes the claim, if there is none, and forces it to disk.
     *
     * @return false if another process created it first
     * @throws IOException if it can't be written
     */
    private boolean create() throws IOException
    {
        StringWriter text = new StringWriter();
        Properties properties = owner.toProperties();
        properties.setProperty("claimed-at", Instant.now().toString());
        properties.store(text, "Owner of this inbound directory (design 9.5)."
                + " Delete only to hand it to another connector.");
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(file,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
                OutputStream out = Channels.newOutputStream(channel))
        {
            out.write(bytes);
            out.flush();
            channel.force(true);
        }
        catch (FileAlreadyExistsException taken)
        {
            return false;
        }
        return true;
    }
}
