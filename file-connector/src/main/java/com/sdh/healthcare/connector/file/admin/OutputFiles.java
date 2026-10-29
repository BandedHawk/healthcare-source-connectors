package com.sdh.healthcare.connector.file.admin;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Writes decrypted output safely: owner-only permissions, never over an existing file, and only renamed into place
 * once complete. If writing or verification fails, the partial file is deleted, so no unverified plaintext remains.
 */
final class OutputFiles {
    private OutputFiles() {}

    @FunctionalInterface
    interface Writer {
        void write(OutputStream out) throws Exception;
    }

    static Path directory(String arg) {
        Path dir = Path.of(arg).toAbsolutePath().normalize();
        if (!Files.isDirectory(dir)) {
            throw new AdminArgs.UsageException("--to must be an existing directory");
        }
        if (!Files.isWritable(dir)) {
            throw new AdminArgs.UsageException("--to is not writable");
        }
        return dir;
    }

    /** The last path element of an archived name, so a stored name can never write outside the directory. */
    static String safeName(String original) {
        String name = original.replace('\\', '/');
        name = name.substring(name.lastIndexOf('/') + 1).replaceAll("[\\p{Cntrl}]", "_");
        if (name.isEmpty() || name.equals(".") || name.equals("..") || name.startsWith(".")) {
            throw new IllegalStateException("The archived original name is not usable as a file name");
        }
        return name;
    }

    /** Writes {@code dir/name} via a hidden temporary file. Returns the number of bytes written. */
    static long writeNew(Path dir, String name, Writer writer) throws Exception {
        Path target = dir.resolve(name);
        if (!target.getParent().equals(dir)) {
            throw new IllegalStateException("Output name escapes the directory");
        }
        if (Files.exists(target)) {
            throw new FileAlreadyExistsException("Output file already exists: " + target);
        }
        Path tmp = dir.resolve("." + name + ".partial");
        boolean posix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
        FileAttribute<?>[] attrs = posix
                ? new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))}
                : new FileAttribute<?>[0];
        boolean done = false;
        try (FileChannel ch = FileChannel.open(tmp, java.util.Set.of(StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE), attrs)) {
            Counting out = new Counting(java.nio.channels.Channels.newOutputStream(ch));
            writer.write(out);
            out.flush();
            ch.force(true);
            if (Files.exists(target)) {
                throw new FileAlreadyExistsException("Output file already exists: " + target);
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            done = true;
            return out.count;
        } finally {
            if (!done) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    private static final class Counting extends FilterOutputStream {
        long count;

        Counting(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }
    }
}
