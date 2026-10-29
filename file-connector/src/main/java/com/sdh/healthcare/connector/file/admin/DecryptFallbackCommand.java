package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.crypto.RecoveryEnvelope;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
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
import java.util.Set;
import java.util.UUID;

/**
 * {@code decrypt-fallback}: decrypts a local fallback file {@code failed/<fileId>.enc} (design §11.3) with the
 * recovery private key. Needs no catalog, lease or KMS: it is for when those were what failed.
 */
final class DecryptFallbackCommand implements AdminCommand {
    static final String PASSPHRASE_ENV = "SDH_RECOVERY_PASSPHRASE";
    static final String USAGE = """
            decrypt-fallback --file <failed/id.enc> --key <recovery-private.pem> --to <dir> [--original-name]
                Decrypts a locally encrypted failed file into <dir> as <id>. The key is an encrypted PKCS#8 PEM;
                its passphrase comes from $SDH_RECOVERY_PASSPHRASE.
                --original-name name the output after the original file instead of <id>""";

    private final java.util.Map<String, String> env;

    DecryptFallbackCommand(java.util.Map<String, String> env) {
        this.env = env;
    }

    @Override
    public String name() {
        return "decrypt-fallback";
    }

    @Override
    public Set<String> options() {
        return Set.of("file", "key", "to", "original-name");
    }

    @Override
    public Set<String> flags() {
        return Set.of("original-name");
    }

    @Override
    public void run(AdminArgs a, AdminContext ctx, AuditRecord audit, PrintStream out) throws Exception {
        Path file = Path.of(a.required("file"));
        Path keyFile = Path.of(a.required("key"));
        Path dir = OutputFiles.directory(a.required("to"));
        String passphrase = env.get(PASSPHRASE_ENV);
        if (passphrase == null || passphrase.isEmpty()) {
            throw new AdminArgs.UsageException("$" + PASSPHRASE_ENV + " must hold the recovery key's passphrase");
        }
        if (!Files.isRegularFile(file)) {
            throw new AdminArgs.UsageException("--file does not exist");
        }
        UUID fileId = peekFileId(file);
        audit.add(Attr.FILE_ID, fileId);
        char[] pass = passphrase.toCharArray();
        PrivateKey key;
        try {
            key = KeyMaterial.loadPrivateKey(keyFile, pass);
        } finally {
            Arrays.fill(pass, '\0');
        }

        RecoveryEnvelope.Metadata[] meta = new RecoveryEnvelope.Metadata[1];
        long written = OutputFiles.writeNew(dir, fileId.toString(), o -> {
            try (InputStream in = Files.newInputStream(file)) {
                meta[0] = RecoveryEnvelope.decrypt(in, key, o);
            }
        });
        audit.add(Attr.BYTES, written);
        if (!meta[0].fileId().equals(fileId) || meta[0].size() != written) {
            Files.deleteIfExists(dir.resolve(fileId.toString()));
            throw new IOException("Fallback file is inconsistent with its own header");
        }
        String where = dir.resolve(fileId.toString()).toString();
        if (a.flag("original-name")) {
            String name = OutputFiles.safeName(meta[0].originalName()
                    .orElseThrow(() -> new IllegalStateException("The fallback file holds no original name")));
            Path target = dir.resolve(name);
            if (Files.exists(target)) {
                throw new FileAlreadyExistsException("Output file already exists: " + target
                        + " (the decrypted file was kept as " + where + ")");
            }
            Files.move(dir.resolve(fileId.toString()), target, StandardCopyOption.ATOMIC_MOVE);
            where = dir + " under its original name";
        }
        out.printf("decrypted %d bytes of file %s, integrity verified, to %s%n", written, fileId, where);
    }

    /** The file ID from the envelope header (it is in clear there), so the output can be named before decrypting. */
    static UUID peekFileId(Path file) throws IOException {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
            byte[] magic = in.readNBytes(6);
            if (!Arrays.equals(magic, "SDHFB1".getBytes(StandardCharsets.US_ASCII))) {
                throw new IOException("Not a recovery envelope (expected an SDHFB1 failed/<id>.enc file)");
            }
            return new UUID(in.readLong(), in.readLong());
        }
    }
}
