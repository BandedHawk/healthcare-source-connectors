/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.failure.FailureReason;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The drain check (STAGES.md §14.4a): which releases are let in, how,
 * and what is kept of them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DrainGateTest
{

    /** The stage. */
    static final String STAGE = "convert";

    /** The releases, on this test's tables. */
    StageReleases releases;

    /** The items part way, as the test sets them. */
    final List<PartWay.Items> parts = new ArrayList<>();

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Creates the tests; JUnit runs each test on a new instance. */
    DrainGateTest()
    {
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        releases = new StageReleases(StageTestSupport.database(),
                StageTestSupport.freshSchema(), STAGE,
                Duration.ofSeconds(10));
        releases.createTablesIfMissing();
        releases.createTablesIfMissing();
    }

    /**
     * A gate for a release, with no ready port.
     *
     * @param versions the release's versions
     * @return the gate
     */
    DrainGate gate(String... versions)
    {
        return new DrainGate(STAGE, StageReleases.Release.of(
                List.of(versions)), releases, first -> List.copyOf(parts),
                Duration.ofMillis(20), -1);
    }

    /**
     * Items part way under a version.
     *
     * @param version the version
     * @param count   how many
     */
    void partWay(String version, long count)
    {
        parts.add(new PartWay.Items(version, count,
                List.of(UUID.randomUUID())));
    }

    /**
     * Lets a release in, as a pod of it would.
     *
     * @param versions the release's versions
     * @return how it was let in
     */
    DrainGate.How letIn(String... versions)
    {
        try (DrainGate gate = gate(versions))
        {
            DrainGate.Decision decision = gate.decide();
            assertThat(gate.awaitLetIn()).isTrue();
            return decision.how();
        }
    }

    /**
     * A digest stands for the versions whatever their order, and changes
     * with any of them.
     */
    @Test
    void aReleaseIsItsVersionsInAnyOrder()
    {
        assertThat(StageReleases.Release.of(List.of("b", "a")))
                .isEqualTo(StageReleases.Release.of(List.of("a", "b", "a")));
        assertThat(StageReleases.Release.of(List.of("a", "b")).versions())
                .containsExactly("a", "b");
        assertThat(StageReleases.Release.of(List.of("a")).digest())
                .hasSize(16)
                .isNotEqualTo(StageReleases.Release.of(List.of("a", "b"))
                        .digest());
    }

    /**
     * The first release is let in, then is current; restarting a pod of it
     * keeps nothing more.
     */
    @Test
    void theFirstReleaseIsLetInThenCurrent()
    {
        assertThat(letIn("v1")).isEqualTo(DrainGate.How.FIRST);
        assertThat(letIn("v1")).isEqualTo(DrainGate.How.CURRENT);
        assertThat(releases.history(10)).singleElement()
                .satisfies(kept ->
                {
                    assertThat(kept.how()).isEqualTo("first");
                    assertThat(kept.release().versions())
                            .containsExactly("v1");
                });
        assertThat(audit.names()).containsExactly("stage.version_changed");
        assertThat(audit.attributes("stage.version_changed"))
                .containsEntry("sdh.let_in_by", "first")
                .containsEntry("sdh.stage", STAGE);
    }

    /**
     * A new release is let in once nothing is part way under a version it
     * wouldn't stamp; items part way under versions it stamps too don't
     * hold it.
     */
    @Test
    void aNewReleaseWaitsForTheStageToDrain()
    {
        letIn("v1", "none");
        partWay("v1", 2);
        partWay("none", 1);
        try (DrainGate gate = gate("v2", "none"))
        {
            DrainGate.Decision held = gate.decide();
            assertThat(held.letIn()).isFalse();
            assertThat(held.blocking()).extracting(PartWay.Items::version)
                    .containsExactly("v1");
            assertThat(held.blockingCount()).isEqualTo(2);
        }
        parts.removeIf(items -> items.version().equals("v1"));
        assertThat(letIn("v2", "none")).isEqualTo(DrainGate.How.DRAINED);
        assertThat(releases.currentAndPrevious()[0].release().versions())
                .containsExactly("none", "v2");
    }

    /**
     * A release whose new versions are each one some item is part way
     * under, as during a rollout already begun, is let in at once.
     */
    @Test
    void aReleaseInFlightIsLetIn()
    {
        letIn("v1");
        partWay("v1", 3);
        partWay("v2", 1);
        assertThat(letIn("v2")).isEqualTo(DrainGate.How.IN_FLIGHT);
    }

    /**
     * The release before the current one is let in: a rollback, or an old
     * pod restarted during a rollout.
     */
    @Test
    void thePreviousReleaseIsLetIn()
    {
        letIn("v1");
        letIn("v2");
        partWay("v2", 4);
        assertThat(letIn("v1")).isEqualTo(DrainGate.How.PREVIOUS);
        // It is current again; v2 the one before it.
        assertThat(releases.currentAndPrevious()[1].release().versions())
                .containsExactly("v2");
    }

    /**
     * A release held back waits, auditing what holds it once while that
     * doesn't change, and is let in once accepted.
     *
     * @throws Exception if the wait fails
     */
    @Test
    void aHeldReleaseWaitsUntilAccepted() throws Exception
    {
        letIn("v1");
        partWay("v1", 2);
        StageReleases.Release v2 = StageReleases.Release.of(List.of("v2"));
        try (DrainGate gate = gate("v2"))
        {
            CompletableFuture<Boolean> waited =
                    CompletableFuture.supplyAsync(gate::awaitLetIn);
            Thread.sleep(150);
            assertThat(waited).isNotDone();
            assertThat(audit.names()).containsOnlyOnce(
                    "stage.version_blocked");
            assertThat(audit.attributes("stage.version_blocked"))
                    .containsEntry("sdh.count", 2L)
                    .containsEntry("sdh.version_count", 1L);

            StageReleases.Accepted accepted = releases.accept(v2, "ops",
                    id -> FailureReason.seal(StageTestSupport.CORRECTION_KEYS,
                            StageTestSupport.CORRECTION_KEK, id, 0, 1,
                            "an urgent fix"));
            assertThat(waited.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(releases.accepted(v2)).get()
                    .extracting(StageReleases.Accepted::acceptId)
                    .isEqualTo(accepted.acceptId());
        }
        assertThat(releases.history(1)).singleElement()
                .extracting(StageReleases.LetIn::how).isEqualTo("forced");
    }

    /**
     * Closing the gate stops the wait, and nothing is let in.
     *
     * @throws Exception if the wait fails
     */
    @Test
    void closingStopsTheWait() throws Exception
    {
        letIn("v1");
        partWay("v1", 1);
        DrainGate gate = gate("v2");
        CompletableFuture<Boolean> waited =
                CompletableFuture.supplyAsync(gate::awaitLetIn);
        Thread.sleep(50);
        gate.close();
        assertThat(waited.get(5, TimeUnit.SECONDS)).isFalse();
        assertThat(releases.history(10)).hasSize(1);
    }

    /**
     * The ready port opens once let in, takes a probe's connection, and
     * closes with the gate.
     *
     * @throws IOException if the probe can't connect
     */
    @Test
    void theReadyPortOpensOnceLetIn() throws IOException
    {
        int port;
        try (DrainGate gate = new DrainGate(STAGE,
                StageReleases.Release.of(List.of("v1")), releases,
                first -> List.of(), Duration.ofMillis(20), 0))
        {
            assertThat(gate.readyPort()).isEqualTo(-1);
            assertThat(gate.awaitLetIn()).isTrue();
            port = gate.readyPort();
            assertThat(port).isPositive();
            for (int probe = 0; probe < 3; probe++)
            {
                try (Socket socket = new Socket())
                {
                    socket.connect(new InetSocketAddress("localhost", port),
                            1000);
                }
            }
        }
        try (Socket socket = new Socket())
        {
            assertThat(connects(socket, port)).isFalse();
        }
    }

    /**
     * Whether a socket connects to a local port.
     *
     * @param socket the socket
     * @param port   the port
     * @return whether it connected
     */
    static boolean connects(Socket socket, int port)
    {
        try
        {
            socket.connect(new InetSocketAddress("localhost", port), 1000);
            return true;
        }
        catch (IOException refused)
        {
            return false;
        }
    }
}
