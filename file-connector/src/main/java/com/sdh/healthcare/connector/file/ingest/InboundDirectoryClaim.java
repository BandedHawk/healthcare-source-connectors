package com.sdh.healthcare.connector.file.ingest;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
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
 * Ties an inbound directory to one connector (design §9.5). The lease stops a second instance with the same lease
 * name; this stops a second connector configured with a different lease name, catalog or namespace on the same
 * directory, which would otherwise pick up the same files.
 *
 * <p>The claim is a hidden file, {@value #FILE}, in the inbound directory, recording which connector owns it. It
 * holds no PHI. It is created atomically ({@code CREATE_NEW}), so of two connectors starting together exactly one
 * wins. It stays after shutdown: the directory still belongs to its connector, which may have a file in progress
 * there. To hand a directory to another connector, stop the old one and delete the file.
 *
 * <p>{@link #claim} runs at startup, after the lease is taken; {@link #verify} runs before every file, so a running
 * connector stops if its claim is replaced. A deleted claim is recreated.
 */
public final class InboundDirectoryClaim {
    public static final String FILE = ".sdh-owner";
    private static final String VERSION = "1";
    /** How long to wait for a claim another connector is still writing before treating it as unreadable. */
    private static final int READ_ATTEMPTS = 10;
    private static final long READ_PAUSE_MS = 50;

    /** The connector's identity: everything that, if different, makes it a different connector. No PHI. */
    public record Owner(String connectorId, String leaseName, String catalogName, String namespace) {
        Properties toProperties() {
            Properties p = new Properties();
            p.setProperty("version", VERSION);
            p.setProperty("connector-id", connectorId);
            p.setProperty("lease-name", leaseName);
            p.setProperty("catalog-name", catalogName);
            p.setProperty("namespace", namespace);
            return p;
        }

        static Owner of(Properties p) {
            return new Owner(p.getProperty("connector-id"), p.getProperty("lease-name"),
                    p.getProperty("catalog-name"), p.getProperty("namespace"));
        }

        /** PHI-free description for logs and the startup error. */
        public String describe() {
            return "connector-id=" + connectorId + " lease-name=" + leaseName + " catalog-name=" + catalogName
                    + " namespace=" + namespace;
        }
    }

    /** The directory belongs to another connector, or its claim can't be read. Stops the connector. */
    public static final class ClaimedException extends RuntimeException {
        private final transient Owner holder;

        ClaimedException(String message, Owner holder) {
            super(message, null, false, false);
            this.holder = holder;
        }

        /** The connector that holds the claim, or null if the claim file couldn't be read. */
        public Owner holder() {
            return holder;
        }

        public ErrorCode code() {
            return ErrorCode.INBOUND_DIR_CLAIMED;
        }
    }

    private final Path file;
    private final Owner owner;

    private InboundDirectoryClaim(Path file, Owner owner) {
        this.file = file;
        this.owner = owner;
    }

    /** Claims {@code dir} for {@code owner}, or confirms an existing claim is ours. */
    public static InboundDirectoryClaim claim(Path dir, Owner owner) throws IOException {
        InboundDirectoryClaim c = new InboundDirectoryClaim(dir.resolve(FILE), owner);
        c.verify();
        return c;
    }

    /** Throws {@link ClaimedException} if the directory now belongs to another connector. */
    public void verify() throws IOException {
        Owner holder = readOrCreate();
        if (!holder.equals(owner)) {
            throw new ClaimedException("The inbound directory belongs to another connector (" + holder.describe()
                    + "). Only one connector may use a directory. If that connector is retired, stop it and delete "
                    + FILE + " in the inbound directory.", holder);
        }
    }

    public Owner owner() {
        return owner;
    }

    private Owner readOrCreate() throws IOException {
        for (int attempt = 1; ; attempt++) {
            Properties p = new Properties();
            try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                p.load(r);
            } catch (NoSuchFileException e) {
                if (create()) {
                    return owner;
                }
                continue; // another connector created it first: read theirs
            }
            if (VERSION.equals(p.getProperty("version")) && complete(p)) {
                return Owner.of(p);
            }
            // Empty or partial: possibly still being written by whoever created it.
            if (attempt >= READ_ATTEMPTS) {
                throw new ClaimedException("The inbound directory's " + FILE + " can't be read. Delete it if no "
                        + "other connector uses this directory.", null);
            }
            try {
                Thread.sleep(READ_PAUSE_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while reading the directory claim", e);
            }
        }
    }

    private static boolean complete(Properties p) {
        return p.getProperty("connector-id") != null && p.getProperty("lease-name") != null
                && p.getProperty("catalog-name") != null && p.getProperty("namespace") != null;
    }

    /** Writes the claim if none exists. False if another process created it first. */
    private boolean create() throws IOException {
        StringWriter w = new StringWriter();
        Properties p = owner.toProperties();
        p.setProperty("claimed-at", Instant.now().toString());
        p.store(w, "Owner of this inbound directory (design 9.5). Delete only to hand it to another connector.");
        byte[] bytes = w.toString().getBytes(StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                OutputStream out = java.nio.channels.Channels.newOutputStream(ch)) {
            out.write(bytes);
            out.flush();
            ch.force(true);
        } catch (FileAlreadyExistsException e) {
            return false;
        }
        return true;
    }
}
