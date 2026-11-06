/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.crypto.RecoveryEnvelope;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * {@code decrypt-fallback}: decrypts a local fallback file
 * {@code failed/<itemId>.enc} (design §11.3) with the recovery private key.
 * Needs no catalog, lease or KMS: it is for when those were what failed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class DecryptFallbackCommand implements AdminCommand
{

    /** The environment variable holding the recovery key's passphrase. */
    static final String PASSPHRASE_ENV = "SDH_RECOVERY_PASSPHRASE";

    /** The command's help. */
    static final String USAGE = """
            decrypt-fallback --file <failed/id.enc>
                    --key <recovery-private.pem> --to <dir> [--original-name]
                Decrypts a locally encrypted failed file into <dir> as <id>.
                The key is an encrypted PKCS#8 PEM; its passphrase comes
                from $SDH_RECOVERY_PASSPHRASE.
                --original-name name the output after the original file
                                instead of <id>""";

    /** The environment, for the passphrase. */
    private final Map<String, String> env;

    /**
     * Creates the command.
     *
     * @param env the environment
     */
    DecryptFallbackCommand(Map<String, String> env)
    {
        this.env = env;
    }

    @Override
    public String name()
    {
        return "decrypt-fallback";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("file", "key", "to", "original-name");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("original-name");
    }

    @Override
    public void run(AdminArgs arguments, AdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        Path file = Path.of(arguments.required("file"));
        Path keyFile = Path.of(arguments.required("key"));
        Path directory = OutputFiles.directory(arguments.required("to"));
        String passphrase = env.get(PASSPHRASE_ENV);
        if (passphrase == null || passphrase.isEmpty())
        {
            throw new AdminArgs.UsageException("$" + PASSPHRASE_ENV
                    + " must hold the recovery key's passphrase");
        }
        if (!Files.isRegularFile(file))
        {
            throw new AdminArgs.UsageException("--file does not exist");
        }
        UUID itemId = peekItemId(file);
        audit.add(Attr.ITEM_ID, itemId);
        char[] passphraseChars = passphrase.toCharArray();
        PrivateKey key;
        try
        {
            key = KeyMaterial.loadPrivateKey(keyFile, passphraseChars);
        }
        finally
        {
            Arrays.fill(passphraseChars, '\0');
        }

        RecoveryEnvelope.Metadata[] metadata = new RecoveryEnvelope.Metadata[1];
        long written = OutputFiles.writeNew(directory, itemId.toString(),
                output ->
                {
                    try (InputStream in = Files.newInputStream(file))
                    {
                        metadata[0] = RecoveryEnvelope.decrypt(in, key, output);
                    }
                });
        audit.add(Attr.BYTES, written);
        if (!metadata[0].itemId().equals(itemId)
                || metadata[0].size() != written)
        {
            Files.deleteIfExists(directory.resolve(itemId.toString()));
            throw new IOException(
                    "Fallback file is inconsistent with its own header");
        }
        String where = directory.resolve(itemId.toString()).toString();
        if (arguments.flag("original-name"))
        {
            String name = OutputFiles.safeName(metadata[0].originalName()
                    .orElseThrow(() -> new IllegalStateException(
                            "The fallback file holds no original name")));
            Path target = directory.resolve(name);
            if (Files.exists(target))
            {
                throw new FileAlreadyExistsException(
                        "Output file already exists: " + target
                        + " (the decrypted file was kept as " + where + ")");
            }
            Files.move(directory.resolve(itemId.toString()), target,
                    StandardCopyOption.ATOMIC_MOVE);
            where = directory + " under its original name";
        }
        out.printf("decrypted %d bytes of file %s, integrity verified,"
                + " to %s%n", written, itemId, where);
    }

    /**
     * The file ID from the envelope header (it is in clear there), so the
     * output can be named before decrypting.
     *
     * @param file the fallback file
     * @return the file ID
     * @throws IOException if the file isn't a recovery envelope or can't be
     *         read
     */
    static UUID peekItemId(Path file) throws IOException
    {
        try (DataInputStream in =
                new DataInputStream(Files.newInputStream(file)))
        {
            byte[] magic = in.readNBytes(6);
            if (!Arrays.equals(magic,
                    "SDHFB1".getBytes(StandardCharsets.US_ASCII)))
            {
                throw new IOException("Not a recovery envelope (expected an"
                        + " SDHFB1 failed/<id>.enc file)");
            }
            return new UUID(in.readLong(), in.readLong());
        }
    }
}
