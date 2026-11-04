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
import com.sdh.healthcare.connector.landing.intake.SmbSource;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
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
 * {@link SmbSource} against real Samba servers, reached through Toxiproxy
 * so the connection can be cut or stalled (design §5.3.4): one requires
 * encryption, as a share holding PHI should; the other has it off, and
 * must be refused.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SmbIntakeIT
{

    /** The Samba image. */
    static final String SAMBA_IMAGE =
            "servercontainers/samba:smbd-only-a3.24.1-s4.23.8-r0";

    /** The account on the servers. */
    static final String USER = "sdh";

    /** Its password. */
    static final String PASSWORD = "s3cret-sdh";

    /** The share's directory inside the containers. */
    static final String SHARE_PATH = "/shares/intake";

    /** The proxy to the encrypting server listens here. */
    static final int ENCRYPTED_PORT = 8666;

    /** The proxy to the plain server listens here. */
    static final int PLAIN_PORT = 8667;

    /** Joins the servers and the proxy. */
    static Network network;

    /** A server that requires encryption. */
    static GenericContainer<?> encrypted;

    /** A server with encryption off. */
    static GenericContainer<?> plain;

    /** Toxiproxy, in front of both. */
    static ToxiproxyContainer toxiproxy;

    /** The proxy to the encrypting server. */
    static Proxy proxy;

    /** The source under test; closed after each test. */
    SmbSource source;

    /**
     * A Samba server with one share for the account.
     *
     * @param alias   its name on the network
     * @param encrypt the {@code server smb encrypt} setting
     * @return the container, not started
     */
    static GenericContainer<?> samba(String alias, String encrypt)
    {
        return new GenericContainer<>(SAMBA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withEnv("ACCOUNT_" + USER, PASSWORD)
                .withEnv("UID_" + USER, "1000")
                .withEnv("SAMBA_GLOBAL_STANZA", "server smb encrypt = "
                        + encrypt + ";server min protocol = SMB3"
                        + ";map to guest = never")
                .withEnv("SAMBA_VOLUME_CONFIG_intake", "[intake]; path = "
                        + SHARE_PATH + "; valid users = " + USER
                        + "; guest ok = no; read only = no; browseable = yes")
                .withExposedPorts(445)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Starts the servers and the proxy.
     *
     * @throws Exception if they can't start
     */
    @BeforeAll
    static void startServers() throws Exception
    {
        network = Network.newNetwork();
        encrypted = samba("smb", "required");
        plain = samba("plain-smb", "off");
        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
                .withNetwork(network);
        encrypted.start();
        plain.start();
        toxiproxy.start();
        ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(),
                toxiproxy.getControlPort());
        proxy = client.createProxy("smb", "0.0.0.0:" + ENCRYPTED_PORT,
                "smb:445");
        client.createProxy("plain-smb", "0.0.0.0:" + PLAIN_PORT,
                "plain-smb:445");
    }

    /** Stops the servers and the proxy. */
    @AfterAll
    static void stopServers()
    {
        for (GenericContainer<?> container : new GenericContainer<?>[] {
                toxiproxy, plain, encrypted})
        {
            if (container != null)
            {
                container.stop();
            }
        }
        if (network != null)
        {
            network.close();
        }
    }

    /**
     * Empties the share and heals the proxy.
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
        for (GenericContainer<?> server : new GenericContainer<?>[] {
                encrypted, plain})
        {
            run(server, "rm -rf " + SHARE_PATH + " && mkdir -p " + SHARE_PATH
                    + "/drop && chown -R 1000:1000 " + SHARE_PATH);
        }
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
     * Runs a shell command on a server as root.
     *
     * @param server  the server
     * @param command the command
     * @throws Exception if it fails
     */
    static void run(GenericContainer<?> server, String command)
            throws Exception
    {
        var result = server.execInContainer("sh", "-c", command);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
    }

    /**
     * Puts a file in the encrypting server's share, owned by the account.
     *
     * @param path    its path within the share
     * @param content its content
     * @throws Exception if it can't be written
     */
    static void put(String path, byte[] content) throws Exception
    {
        String full = SHARE_PATH + path;
        run(encrypted, "mkdir -p \"$(dirname " + full + ")\"");
        encrypted.copyFileToContainer(Transferable.of(content), full);
        run(encrypted, "chown -R 1000:1000 " + SHARE_PATH);
    }

    /**
     * Settings for a source on a share through the proxy.
     *
     * @param port     the proxy port: which server
     * @param share    the share's name
     * @param password the password to log in with
     * @param access   the access mode
     * @param after    the after-action, or null for the default
     * @return the checked settings
     */
    static SourceSettings settings(int port, String share, String password,
            String access, String after)
    {
        return SourceSettings.of(new LandingProperties.Source(
                Path.of("/drop"), access, after, Path.of("/done"), ".landed",
                "settled", Duration.ZERO, "**/*", true, Duration.ofSeconds(1),
                "smb", Duration.ofMinutes(5), null, new LandingProperties.Smb(
                        toxiproxy.getHost(), toxiproxy.getMappedPort(port),
                        share, "", USER, password, Duration.ofSeconds(5),
                        Duration.ofSeconds(4)), null, null));
    }

    /**
     * A source on the encrypting server's share.
     *
     * @param access the access mode
     * @param after  the after-action, or null for the default
     * @return the source
     */
    SmbSource source(String access, String after)
    {
        source = new SmbSource(settings(ENCRYPTED_PORT, "intake", PASSWORD,
                access, after));
        return source;
    }

    /**
     * Lists, reads from an offset, and moves a landed item on an
     * encrypted share; the write probe works and leaves nothing behind.
     *
     * @throws Exception if the test fails
     */
    @Test
    void listsReadsAndMovesOnAnEncryptedShare() throws Exception
    {
        put("/drop/sub/a.csv", "0123456789".getBytes(UTF_8));
        SmbSource moving = source("read-write", "move");
        moving.checkWritable();
        ListedItem item = moving.list().get(0);
        assertThat(item.ref()).isEqualTo("sub/a.csv");
        assertThat(item.size()).isEqualTo(10);
        try (InputStream input = moving.open(item, 4))
        {
            assertThat(new String(input.readAllBytes(), UTF_8))
                    .isEqualTo("456789");
        }
        moving.afterLanding(item);
        assertThat(encrypted.execInContainer("cat",
                SHARE_PATH + "/done/sub/a.csv").getStdout())
                .isEqualTo("0123456789");
        assertThat(encrypted.execInContainer("ls", "-A",
                SHARE_PATH + "/drop").getStdout()).doesNotContain(".sdh-probe");
        assertThat(moving.list()).isEmpty();
    }

    /**
     * A share whose server won't encrypt is refused before anything is
     * listed.
     */
    @Test
    void refusesAShareWithoutEncryption()
    {
        source = new SmbSource(settings(PLAIN_PORT, "intake", PASSWORD,
                "read-only", null));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.ENCRYPTION));
    }

    /** A wrong password and a missing share are told apart. */
    @Test
    void reportsAWrongPasswordAndAMissingShare()
    {
        source = new SmbSource(settings(ENCRYPTED_PORT, "intake", "wrong",
                "read-only", null));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.AUTH));
        source.close();
        source = new SmbSource(settings(ENCRYPTED_PORT, "missing", PASSWORD,
                "read-only", null));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.NOT_FOUND));
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
        new Random(6).nextBytes(content);
        put("/drop/big.bin", content);
        SmbSource reading = source("read-only", null);
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
            assertThat(Arrays.equals(rest, 0, rest.length, content,
                    first.length, content.length)).isTrue();
        }
    }

    /**
     * A connection that stalls without closing fails the read after about
     * the read timeout, instead of hanging.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsAStalledRead() throws Exception
    {
        byte[] content = new byte[16 << 20];
        new Random(7).nextBytes(content);
        put("/drop/big.bin", content);
        SmbSource reading = source("read-only", null);
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
     * After the server restarts, the source connects again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void connectsAgainAfterTheServerRestarts() throws Exception
    {
        put("/drop/a.csv", "a".getBytes(UTF_8));
        SmbSource reading = source("read-only", null);
        assertThat(reading.list()).hasSize(1);
        encrypted.getDockerClient()
                .restartContainerCmd(encrypted.getContainerId()).exec();
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
}
