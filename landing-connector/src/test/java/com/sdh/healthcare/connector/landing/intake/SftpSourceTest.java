/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import org.apache.sshd.common.config.keys.PublicKeyEntry;
import org.apache.sshd.common.config.keys.writer.openssh.OpenSSHKeyPairResourceWriter;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link SftpSource} against an SFTP server in the test, which
 * can be stopped part-way through a read and given other keys.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SftpSourceTest
{

    /** The account on the test server. */
    static final String USER = "sdh";

    /** Holds the server's files, the keys and known_hosts. */
    @TempDir
    Path root;

    /** The server's files; {@code /} on the server. */
    Path served;

    /** The server's host key. */
    KeyPair hostKey;

    /** The key the server accepts for {@link #USER}. */
    KeyPair userKey;

    /** The test server; null while stopped. */
    SshServer server;

    /** The port the server listens on. */
    int port;

    /** The source under test; closed after each test. */
    SftpSource source;

    /**
     * Makes the keys and starts the server.
     *
     * @throws Exception if it can't start
     */
    @BeforeEach
    void setUp() throws Exception
    {
        served = Files.createDirectories(root.resolve("served"));
        Files.createDirectories(served.resolve("in"));
        hostKey = ecKey();
        userKey = ecKey();
        start(0);
    }

    /** Closes the source and stops the server. */
    @AfterEach
    void tearDown() throws Exception
    {
        if (source != null)
        {
            source.close();
        }
        stop();
    }

    /**
     * Starts the server, accepting {@link #userKey} for {@link #USER}.
     *
     * @param onPort the port; 0 for any free one
     * @throws Exception if it can't start
     */
    void start(int onPort) throws Exception
    {
        server = SshServer.setUpDefaultServer();
        server.setHost("127.0.0.1");
        server.setPort(onPort);
        server.setKeyPairProvider(KeyPairProvider.wrap(hostKey));
        server.setPublickeyAuthenticator((user, key, session) ->
                USER.equals(user) && key.equals(userKey.getPublic()));
        server.setPasswordAuthenticator((user, password, session) ->
                USER.equals(user) && "s3cret".equals(password));
        server.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
        server.setFileSystemFactory(new VirtualFileSystemFactory(served));
        server.start();
        port = server.getPort();
    }

    /** Stops the server and drops its sessions. */
    void stop() throws Exception
    {
        if (server != null)
        {
            server.stop(true);
            server = null;
        }
    }

    /**
     * An EC P-256 key pair.
     *
     * @return the key pair
     * @throws Exception if it can't be made
     */
    static KeyPair ecKey() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Writes a private key in OpenSSH's format, as {@code ssh-keygen}
     * does by default.
     *
     * @param key  the key pair
     * @param name the file's name
     * @return the file
     * @throws Exception if it can't be written
     */
    Path keyFile(KeyPair key, String name) throws Exception
    {
        Path file = root.resolve(name);
        try (OutputStream output = Files.newOutputStream(file))
        {
            OpenSSHKeyPairResourceWriter.INSTANCE.writePrivateKey(key,
                    "sdh-test", null, output);
        }
        return file;
    }

    /**
     * Writes a known_hosts file holding a host key for the server.
     *
     * @param key the host key to pin
     * @return the file
     * @throws Exception if it can't be written
     */
    Path knownHosts(PublicKey key) throws Exception
    {
        return Files.writeString(root.resolve("known_hosts"),
                "[127.0.0.1]:" + port + " " + PublicKeyEntry.toString(key)
                        + "\n");
    }

    /**
     * Settings for the source, reading {@code /in} on the server.
     *
     * @param access   the access mode
     * @param after    the after-action, or null for the default
     * @param ready    {@code settled} or {@code marker}
     * @param key      the key to log in with, or null for the password
     * @param pinned   the host key to pin
     * @return the checked settings
     * @throws Exception if a file can't be written
     */
    SourceSettings settings(String access, String after, String ready,
            KeyPair key, PublicKey pinned) throws Exception
    {
        LandingProperties.Sftp sftp = new LandingProperties.Sftp("127.0.0.1",
                port, USER, key == null ? null : keyFile(key, "id_ecdsa"),
                null, key == null ? "s3cret" : null, knownHosts(pinned),
                Duration.ofSeconds(5), Duration.ofSeconds(5));
        return SourceSettings.of(new LandingProperties.Source(Path.of("/in"),
                access, after, Path.of("/done"), ".landed", ready,
                Duration.ZERO, "**/*", true, Duration.ofSeconds(1), "sftp",
                Duration.ofMinutes(5), sftp, null));
    }

    /**
     * A read-only source that logs in with the user's key.
     *
     * @return the source
     * @throws Exception if its settings can't be written
     */
    SftpSource readOnly() throws Exception
    {
        source = new SftpSource(settings("read-only", null, "settled",
                userKey, hostKey.getPublic()));
        return source;
    }

    /**
     * Writes a file on the server with a given modification time.
     *
     * @param ref     its path under {@code /in}
     * @param content its content
     * @param seconds its modification time, in seconds since the epoch
     * @return the file
     * @throws Exception if it can't be written
     */
    Path write(String ref, byte[] content, long seconds) throws Exception
    {
        Path file = served.resolve("in").resolve(ref);
        Files.createDirectories(file.getParent());
        Files.write(file, content);
        Files.setLastModifiedTime(file,
                FileTime.from(Instant.ofEpochSecond(seconds)));
        return file;
    }

    /**
     * Lists oldest first, in subdirectories, and leaves out hidden files;
     * reads an item from an offset.
     *
     * @throws Exception if the test fails
     */
    @Test
    void listsOldestFirstAndReadsFromAnOffset() throws Exception
    {
        write("b.csv", "bbbb".getBytes(UTF_8), 2_000);
        write("sub/a.csv", "0123456789".getBytes(UTF_8), 1_000);
        write(".hidden", "x".getBytes(UTF_8), 500);
        write(".dir/c.csv", "x".getBytes(UTF_8), 500);

        List<ListedItem> items = readOnly().list();
        assertThat(items).containsExactly(
                new ListedItem("sub/a.csv", 10, 1_000_000),
                new ListedItem("b.csv", 4, 2_000_000));
        assertThat(source.reachable()).isTrue();
        try (InputStream input = source.open(items.get(0), 3))
        {
            assertThat(new String(input.readAllBytes(), UTF_8))
                    .isEqualTo("3456789");
        }
    }

    /** Logs in with a password when no key is set. */
    @Test
    void logsInWithAPassword() throws Exception
    {
        write("a.csv", "a".getBytes(UTF_8), 1_000);
        source = new SftpSource(settings("read-only", null, "settled", null,
                hostKey.getPublic()));
        assertThat(source.list()).hasSize(1);
    }

    /**
     * A host key that isn't the pinned one is refused, and nothing is
     * listed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesAHostKeyThatIsntPinned() throws Exception
    {
        write("a.csv", "a".getBytes(UTF_8), 1_000);
        source = new SftpSource(settings("read-only", null, "settled",
                userKey, ecKey().getPublic()));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.HOST_KEY));
        assertThat(source.reachable()).isFalse();
    }

    /** A key the server doesn't accept is reported as an auth failure. */
    @Test
    void reportsRejectedCredentials() throws Exception
    {
        source = new SftpSource(settings("read-only", null, "settled",
                ecKey(), hostKey.getPublic()));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.AUTH));
    }

    /**
     * A missing source directory, and one the account can't read, are
     * reported as such; the messages hold no path.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reportsAMissingOrUnreadableDirectory() throws Exception
    {
        readOnly();
        Path in = served.resolve("in");
        Files.setPosixFilePermissions(in,
                PosixFilePermissions.fromString("---------"));
        try
        {
            assertThatThrownBy(source::list)
                    .isInstanceOfSatisfying(IntakeUnavailableException.class,
                            failure ->
                            {
                                assertThat(failure.problem())
                                        .isEqualTo(IntakeProblem.PERMISSION);
                                assertThat(failure.getMessage())
                                        .doesNotContain("/in");
                            });
        }
        finally
        {
            Files.setPosixFilePermissions(in,
                    PosixFilePermissions.fromString("rwx------"));
        }
        Files.delete(in);
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.NOT_FOUND));
    }

    /**
     * When the server goes away part-way through a read, the read fails as
     * a network problem; once it is back, the source connects again and
     * reads from where it left off.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reconnectsAfterTheServerDropsPartWay() throws Exception
    {
        byte[] content = new byte[8 << 20];
        new Random(1).nextBytes(content);
        write("big.bin", content, 1_000);
        ListedItem item = readOnly().list().get(0);
        byte[] first = new byte[1024];
        try (InputStream input = source.open(item, 0))
        {
            assertThat(input.readNBytes(first, 0, first.length))
                    .isEqualTo(first.length);
            stop();
            assertThatThrownBy(() -> input.readAllBytes())
                    .isInstanceOfSatisfying(IntakeUnavailableException.class,
                            failure -> assertThat(failure.problem())
                                    .isEqualTo(IntakeProblem.NETWORK));
        }
        assertThat(source.reachable()).isFalse();
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.NETWORK));

        start(port);
        assertThat(source.list()).containsExactly(item);
        try (InputStream input = source.open(item, first.length))
        {
            byte[] rest = input.readAllBytes();
            assertThat(rest).hasSize(content.length - first.length);
            assertThat(rest[0]).isEqualTo(content[first.length]);
        }
        assertThat(source.reachable()).isTrue();
    }

    /**
     * An item that changed or went after it was listed isn't opened.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesAnItemThatChanged() throws Exception
    {
        Path file = write("a.csv", "first".getBytes(UTF_8), 1_000);
        ListedItem item = readOnly().list().get(0);
        write("a.csv", "second!".getBytes(UTF_8), 1_000);
        assertThatThrownBy(() -> source.open(item, 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        Files.delete(file);
        assertThatThrownBy(() -> source.open(item, 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
    }

    /**
     * A read-write source moves a landed item, keeping its path and never
     * overwriting, deletes its marker, and leaves the move-to directory
     * out of listings; only marked items are offered.
     *
     * @throws Exception if the test fails
     */
    @Test
    void movesLandedItemsAndTheirMarkers() throws Exception
    {
        source = new SftpSource(settings("read-write", "move", "marker",
                userKey, hostKey.getPublic()));
        source.checkWritable();
        assertThat(served.resolve("done")).isDirectory();
        write("sub/a.csv", "a".getBytes(UTF_8), 1_000);
        write("sub/a.csv.done", new byte[0], 1_000);
        write("b.csv", "b".getBytes(UTF_8), 1_000);
        Files.createDirectories(served.resolve("done/sub"));
        Files.writeString(served.resolve("done/sub/a.csv"), "taken");

        List<ListedItem> items = source.list();
        assertThat(items).extracting(ListedItem::ref)
                .containsExactly("sub/a.csv");
        source.afterLanding(items.get(0));
        assertThat(served.resolve("in/sub/a.csv")).doesNotExist();
        assertThat(served.resolve("in/sub/a.csv.done")).doesNotExist();
        assertThat(served.resolve("done/sub/a.csv")).hasContent("taken");
        assertThat(served.resolve("done/sub/a.csv.1")).hasContent("a");
        assertThat(served.resolve("in")).isDirectoryNotContaining(
                path -> path.getFileName().toString().startsWith(".sdh-"));
    }

    /**
     * Renames and deletes landed items, and leaves renamed ones out of
     * listings; an item that changed since it was listed is left alone.
     *
     * @throws Exception if the test fails
     */
    @Test
    void renamesOrDeletesLandedItems() throws Exception
    {
        source = new SftpSource(settings("read-write", "rename", "settled",
                userKey, hostKey.getPublic()));
        write("a.csv", "a".getBytes(UTF_8), 1_000);
        ListedItem item = source.list().get(0);
        source.afterLanding(item);
        assertThat(served.resolve("in/a.csv.landed")).hasContent("a");
        assertThat(source.list()).isEmpty();
        source.close();

        source = new SftpSource(settings("read-write", "delete", "settled",
                userKey, hostKey.getPublic()));
        write("b.csv", "b".getBytes(UTF_8), 1_000);
        ListedItem listed = source.list().get(0);
        write("b.csv", "bb".getBytes(UTF_8), 1_000);
        source.afterLanding(listed);
        assertThat(served.resolve("in/b.csv")).hasContent("bb");
        source.afterLanding(source.list().stream()
                .filter(each -> each.ref().equals("b.csv")).findFirst()
                .orElseThrow());
        assertThat(served.resolve("in/b.csv")).doesNotExist();
    }

    /**
     * The write probe fails on a directory the account can't write to.
     *
     * @throws Exception if the test fails
     */
    @Test
    void probeFailsOnADirectoryItCantWrite() throws Exception
    {
        source = new SftpSource(settings("read-write", "delete", "settled",
                userKey, hostKey.getPublic()));
        Path in = served.resolve("in");
        Files.setPosixFilePermissions(in,
                PosixFilePermissions.fromString("r-x------"));
        try
        {
            assertThatThrownBy(source::checkWritable)
                    .isInstanceOf(IntakeNotWritableException.class);
        }
        finally
        {
            Files.setPosixFilePermissions(in,
                    PosixFilePermissions.fromString("rwx------"));
        }
    }
}
