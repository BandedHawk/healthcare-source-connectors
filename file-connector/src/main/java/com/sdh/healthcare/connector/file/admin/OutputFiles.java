/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Writes decrypted output safely: owner-only permissions, never over an
 * existing file, and only renamed into place once complete. If writing or
 * verification fails, the partial file is deleted, so no unverified
 * plaintext remains.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class OutputFiles
{

    /** Not instantiable. */
    private OutputFiles()
    {
    }

    /** Writes the content of an output file. */
    @FunctionalInterface
    interface Writer
    {

        /**
         * Writes the content.
         *
         * @param out where to write it
         * @throws Exception if writing or verification fails
         */
        void write(OutputStream out) throws Exception;
    }

    /**
     * The output directory named by {@code --to}.
     *
     * @param argument the option's value
     * @return the absolute, normalised directory
     * @throws AdminArgs.UsageException if it isn't an existing, writable
     *         directory
     */
    static Path directory(String argument)
    {
        Path directory = Path.of(argument).toAbsolutePath().normalize();
        if (!Files.isDirectory(directory))
        {
            throw new AdminArgs.UsageException(
                    "--to must be an existing directory");
        }
        if (!Files.isWritable(directory))
        {
            throw new AdminArgs.UsageException("--to is not writable");
        }
        return directory;
    }

    /**
     * The last path element of an archived name, so a stored name can never
     * write outside the directory.
     *
     * @param original the archived name
     * @return the safe file name
     * @throws IllegalStateException if nothing usable is left
     */
    static String safeName(String original)
    {
        String name = original.replace('\\', '/');
        name = name.substring(name.lastIndexOf('/') + 1)
                .replaceAll("[\\p{Cntrl}]", "_");
        if (name.isEmpty() || name.equals(".") || name.equals("..")
                || name.startsWith("."))
        {
            throw new IllegalStateException(
                    "The archived original name is not usable as a file name");
        }
        return name;
    }

    /**
     * Writes {@code directory/name} via a hidden temporary file.
     *
     * @param directory the output directory
     * @param name the file name
     * @param writer writes the content
     * @return the number of bytes written
     * @throws Exception if the file exists, the name escapes the directory,
     *         or writing fails
     */
    static long writeNew(Path directory, String name, Writer writer)
            throws Exception
    {
        Path target = directory.resolve(name);
        if (!target.getParent().equals(directory))
        {
            throw new IllegalStateException(
                    "Output name escapes the directory");
        }
        if (Files.exists(target))
        {
            throw new FileAlreadyExistsException(
                    "Output file already exists: " + target);
        }
        Path partial = directory.resolve("." + name + ".partial");
        boolean posix = FileSystems.getDefault().supportedFileAttributeViews()
                .contains("posix");
        FileAttribute<?>[] attributes = posix
                ? new FileAttribute<?>[] {PosixFilePermissions
                        .asFileAttribute(PosixFilePermissions
                                .fromString("rw-------"))}
                : new FileAttribute<?>[0];
        boolean done = false;
        try (FileChannel channel = FileChannel.open(partial,
                Set.of(StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE), attributes))
        {
            Counting out = new Counting(Channels.newOutputStream(channel));
            writer.write(out);
            out.flush();
            channel.force(true);
            if (Files.exists(target))
            {
                throw new FileAlreadyExistsException(
                        "Output file already exists: " + target);
            }
            Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE);
            done = true;
            return out.count;
        }
        finally
        {
            if (!done)
            {
                Files.deleteIfExists(partial);
            }
        }
    }

    /** Counts the bytes written through it. */
    private static final class Counting extends FilterOutputStream
    {

        /** The bytes written so far. */
        long count;

        /**
         * Wraps a stream.
         *
         * @param out the real stream
         */
        Counting(OutputStream out)
        {
            super(out);
        }

        /**
         * Writes and counts one byte.
         *
         * @param value the byte
         * @throws IOException if writing fails
         */
        @Override
        public void write(int value) throws IOException
        {
            out.write(value);
            count++;
        }

        /**
         * Writes and counts bytes.
         *
         * @param bytes the bytes
         * @param offset the first to write
         * @param length how many to write
         * @throws IOException if writing fails
         */
        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            out.write(bytes, offset, length);
            count += length;
        }
    }
}
