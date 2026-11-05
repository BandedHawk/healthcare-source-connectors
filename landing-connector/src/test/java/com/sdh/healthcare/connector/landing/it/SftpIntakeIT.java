/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import com.sdh.healthcare.connector.landing.intake.IntakeUnavailableException;
import com.sdh.healthcare.connector.landing.intake.ListedItem;
import com.sdh.healthcare.connector.landing.intake.SftpSource;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import org.apache.sshd.common.config.keys.PublicKeyEntry;
import org.apache.sshd.common.config.keys.writer.openssh.OpenSSHKeyPairResourceWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * {@link SftpSource} against a real OpenSSH server, reached through
 * Toxiproxy so the connection can be cut or stalled (design §5.3.4): the
 * failure cases of step 4 of §5.3.10.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SftpIntakeIT
{

    /** The OpenSSH server image. */
    static final String SFTP_IMAGE =
            "linuxserver/openssh-server:10.3_p1-r1-ls239";

    /** The Toxiproxy image. */
    static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";

    /** The account on the server. */
    static final String USER = "sdh";

    /** The proxy's listening port inside its container. */
    static final int PROXY_PORT = 8666;

    /** The directory the intake reads, on the server. */
    static final String DROP = "/config/drop";

    /** Joins the server and the proxy. */
    static Network network;

    /** The OpenSSH server. */
    static GenericContainer<?> sftp;

    /** Toxiproxy, in front of the server. */
    static ToxiproxyContainer toxiproxy;

    /** The proxy to the server. */
    static Proxy proxy;

    /** The account's key. */
    static KeyPair userKey;

    /** Holds the key and known_hosts files. */
    static Path files;

    /** The account's key file. */
    static Path keyFile;

    /** known_hosts with the server's real host key. */
    static Path knownHosts;

    /** The source under test; closed after each test. */
    SftpSource source;

    /**
     * Starts the server and the proxy, and pins the server's host key.
     *
     * @throws Exception if they can't start
     */
    @BeforeAll
    static void startServers() throws Exception
    {
        files = Files.createTempDirectory("sdh-sftp-it");
        userKey = ecKey();
        keyFile = files.resolve("id_ecdsa");
        try (OutputStream output = Files.newOutputStream(keyFile))
        {
            OpenSSHKeyPairResourceWriter.INSTANCE.writePrivateKey(userKey,
                    "sdh-it", null, output);
        }
        network = Network.newNetwork();
        sftp = new GenericContainer<>(SFTP_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("sftp")
                .withEnv("PUID", "1000")
                .withEnv("PGID", "1000")
                .withEnv("USER_NAME", USER)
                .withEnv("PUBLIC_KEY",
                        PublicKeyEntry.toString(userKey.getPublic()))
                .withEnv("PASSWORD_ACCESS", "false")
                .withEnv("LOG_STDOUT", "true")
                .waitingFor(Wait.forLogMessage(".*\\[ls\\.io-init\\] done.*",
                        1).withStartupTimeout(Duration.ofMinutes(2)));
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network);
        sftp.start();
        toxiproxy.start();
        proxy = new ToxiproxyClient(toxiproxy.getHost(),
                toxiproxy.getControlPort()).createProxy("sftp",
                        "0.0.0.0:" + PROXY_PORT, "sftp:2222");
        knownHosts = knownHosts(sftp.execInContainer("sh", "-c",
                "cat /config/ssh_host_keys/ssh_host_ed25519_key.pub")
                .getStdout().trim());
    }

    /** Stops the server and the proxy. */
    @AfterAll
    static void stopServers()
    {
        if (toxiproxy != null)
        {
            toxiproxy.stop();
        }
        if (sftp != null)
        {
            sftp.stop();
        }
        if (network != null)
        {
            network.close();
        }
    }

    /**
     * Empties the server's directories and heals the proxy.
     *
     * @throws Exception if the server can't be reached
     */
    @BeforeEach
    void clean() throws Exception
    {
        proxy.enable();
        for (var toxic : proxy.toxics().getAll())
        {
            toxic.remove();
        }
        root("rm -rf /config/drop /config/done && mkdir -p " + DROP
                + " && chown -R 1000:1000 " + DROP);
    }

    /** Closes the source. */
    @AfterEach
    void close()
    {
        if (source != null)
        {
            source.close();
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
     * A known_hosts file pinning a host key for the proxy's address.
     *
     * @param publicKey the key, as {@code ssh-keygen} writes it
     * @return the file
     * @throws Exception if it can't be written
     */
    static Path knownHosts(String publicKey) throws Exception
    {
        String[] parts = publicKey.split(" ");
        return Files.writeString(Files.createTempFile(files, "known", ""),
                "[" + toxiproxy.getHost() + "]:"
                        + toxiproxy.getMappedPort(PROXY_PORT) + " " + parts[0]
                        + " " + parts[1] + "\n");
    }

    /**
     * Runs a shell command on the server as root.
     *
     * @param command the command
     * @throws Exception if it fails
     */
    static void root(String command) throws Exception
    {
        var result = sftp.execInContainer("sh", "-c", command);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
    }

    /**
     * Puts a file on the server, owned by the account.
     *
     * @param path    its path on the server
     * @param content its content
     * @throws Exception if it can't be written
     */
    static void put(String path, byte[] content) throws Exception
    {
        root("mkdir -p \"$(dirname " + path + ")\"");
        sftp.copyFileToContainer(Transferable.of(content), path);
        root("chown -R 1000:1000 " + DROP);
    }

    /**
     * Settings for a source on the server through the proxy.
     *
     * @param access      the access mode
     * @param after       the after-action, or null for the default
     * @param key         the key file to log in with
     * @param pinned      the known_hosts file
     * @param readTimeout how long a connection may stall
     * @return the checked settings
     */
    static SourceSettings settings(String access, String after, Path key,
            Path pinned, Duration readTimeout)
    {
        return SourceSettings.of(new LandingProperties.Source(Path.of(DROP),
                access, after, Path.of("/config/done"), ".landed", "settled",
                Duration.ZERO, "**/*", true, Duration.ofSeconds(1), "sftp",
                Duration.ofMinutes(5), new LandingProperties.Sftp(
                        toxiproxy.getHost(),
                        toxiproxy.getMappedPort(PROXY_PORT), USER, key, null,
                        null, pinned, Duration.ofSeconds(5), readTimeout),
                null, null, null));
    }

    /**
     * A source that logs in with the account's key.
     *
     * @param access the access mode
     * @param after  the after-action, or null for the default
     * @return the source
     */
    SftpSource source(String access, String after)
    {
        source = new SftpSource(settings(access, after, keyFile, knownHosts,
                Duration.ofSeconds(4)));
        return source;
    }

    /**
     * Lists, reads from an offset, and moves a landed item on a real
     * server; the write probe works and leaves nothing behind.
     *
     * @throws Exception if the test fails
     */
    @Test
    void listsReadsAndMovesOnARealServer() throws Exception
    {
        put(DROP + "/sub/a.csv", "0123456789".getBytes(UTF_8));
        SftpSource moving = source("read-write", "move");
        moving.checkWritable();
        ListedItem item = moving.list().get(0);
        assertThat(item.ref()).isEqualTo("sub/a.csv");
        try (InputStream input = moving.open(item, 4))
        {
            assertThat(new String(input.readAllBytes(), UTF_8))
                    .isEqualTo("456789");
        }
        moving.afterLanding(item);
        assertThat(sftp.execInContainer("cat", "/config/done/sub/a.csv")
                .getStdout()).isEqualTo("0123456789");
        assertThat(sftp.execInContainer("ls", "-A", DROP).getStdout())
                .doesNotContain(".sdh-probe");
        assertThat(moving.list()).isEmpty();
    }

    /**
     * A read-write source is claimed through the server: the claim is made
     * by a hard link, so a second create is refused; another node is
     * refused; the claim and its staging file are never listed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void claimsTheSourceOnARealServer() throws Exception
    {
        put(DROP + "/a.csv", "1".getBytes(UTF_8));
        SftpSource claiming = source("read-write", "delete");
        InboundDirectoryClaim.Owner first = new InboundDirectoryClaim.Owner(
                "landing-a", "lease-a", "cat", "ns");
        InboundDirectoryClaim.Owner second = new InboundDirectoryClaim.Owner(
                "landing-b", "lease-b", "cat", "ns");
        InboundDirectoryClaim claim = InboundDirectoryClaim.claim(
                claiming.claimFile().orElseThrow(), first);
        claim.verify();
        assertThat(sftp.execInContainer("cat", DROP + "/"
                + InboundDirectoryClaim.FILE).getStdout())
                .contains("connector-id=landing-a");
        assertThat(sftp.execInContainer("ls", "-A", DROP).getStdout())
                .doesNotContain(".sdh-probe");
        assertThat(claiming.claimFile().orElseThrow().create(new byte[0]))
                .isFalse();
        assertThatThrownBy(() -> InboundDirectoryClaim.claim(
                claiming.claimFile().orElseThrow(), second))
                .isInstanceOf(InboundDirectoryClaim.ClaimedException.class);
        assertThat(claiming.list()).extracting(ListedItem::ref)
                .containsExactly("a.csv");
        assertThat(source("read-only", null).claimFile()).isEmpty();
    }

    /**
     * A connection cut part-way through an item fails the read as a network
     * problem; when the network is back, the item reads on from where it
     * stopped.
     *
     * @throws Exception if the test fails
     */
    @Test
    void resumesAnItemAfterTheConnectionIsCut() throws Exception
    {
        byte[] content = new byte[16 << 20];
        new Random(4).nextBytes(content);
        put(DROP + "/big.bin", content);
        SftpSource reading = source("read-only", null);
        ListedItem item = reading.list().get(0);
        byte[] first = new byte[1 << 20];
        try (InputStream input = reading.open(item, 0))
        {
            assertThat(input.readNBytes(first, 0, first.length))
                    .isEqualTo(first.length);
            proxy.disable();
            assertThatThrownBy(input::readAllBytes)
                    .isInstanceOfSatisfying(IntakeUnavailableException.class,
                            failure -> assertThat(failure.problem())
                                    .isEqualTo(IntakeProblem.NETWORK));
        }
        assertThatThrownBy(reading::list)
                .isInstanceOf(IntakeUnavailableException.class);
        proxy.enable();
        assertThat(reading.list()).containsExactly(item);
        try (InputStream input = reading.open(item, first.length))
        {
            byte[] rest = input.readAllBytes();
            assertThat(rest).hasSize(content.length - first.length);
            assertThat(Arrays.equals(rest, 0, rest.length, content,
                    first.length, content.length)).isTrue();
        }
    }

    /**
     * A connection that stalls without closing is dropped after about the
     * read timeout, instead of hanging.
     *
     * @throws Exception if the test fails
     */
    @Test
    void dropsAStalledConnection() throws Exception
    {
        byte[] content = new byte[16 << 20];
        new Random(5).nextBytes(content);
        put(DROP + "/big.bin", content);
        SftpSource reading = source("read-only", null);
        ListedItem item = reading.list().get(0);
        try (InputStream input = reading.open(item, 0))
        {
            input.readNBytes(1 << 20);
            proxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);
            long started = System.nanoTime();
            assertThatThrownBy(input::readAllBytes)
                    .isInstanceOf(IntakeUnavailableException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofSeconds(20));
        }
        proxy.toxics().get("stall").remove();
        assertThat(reading.list()).containsExactly(item);
    }

    /**
     * After the server restarts, the source connects again with the same
     * pinned host key.
     *
     * @throws Exception if the test fails
     */
    @Test
    void connectsAgainAfterTheServerRestarts() throws Exception
    {
        put(DROP + "/a.csv", "a".getBytes(UTF_8));
        SftpSource reading = source("read-only", null);
        assertThat(reading.list()).hasSize(1);
        sftp.getDockerClient().restartContainerCmd(sftp.getContainerId())
                .exec();
        long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
        while (true)
        {
            try
            {
                assertThat(reading.list()).hasSize(1);
                break;
            }
            catch (IntakeUnavailableException notYet)
            {
                assertThat(notYet.problem()).isEqualTo(IntakeProblem.NETWORK);
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(500);
            }
        }
        assertThat(reading.reachable()).isTrue();
    }

    /**
     * A key the server doesn't accept, and a host key that isn't the
     * pinned one, are told apart.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reportsAWrongKeyAndAChangedHostKey() throws Exception
    {
        Path otherKey = files.resolve("other_ecdsa");
        try (OutputStream output = Files.newOutputStream(otherKey))
        {
            OpenSSHKeyPairResourceWriter.INSTANCE.writePrivateKey(ecKey(),
                    "other", null, output);
        }
        source = new SftpSource(settings("read-only", null, otherKey,
                knownHosts, Duration.ofSeconds(4)));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.AUTH));
        source.close();

        String otherHost = PublicKeyEntry.toString(ecKey().getPublic());
        source = new SftpSource(settings("read-only", null, keyFile,
                knownHosts(otherHost), Duration.ofSeconds(4)));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.HOST_KEY));
    }
}
