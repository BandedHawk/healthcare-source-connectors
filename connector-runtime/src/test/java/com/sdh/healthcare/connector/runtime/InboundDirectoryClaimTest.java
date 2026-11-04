/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim.ClaimedException;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim.Owner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * One node per inbound directory (design §9.5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class InboundDirectoryClaimTest
{

    /** The first node's identity. */
    static final Owner FIRST =
            new Owner("file-connector", "file-connector", "sdh", "sdh_ingest");

    /** How many nodes start together in the race. */
    static final int RACERS = 16;

    /** The inbound directory. */
    @TempDir
    Path directory;

    /**
     * The first node claims the directory, without its path in the claim,
     * and keeps it across restarts.
     *
     * @throws Exception if the claim can't be written
     */
    @Test
    void firstConnectorClaimsTheDirectoryAndKeepsItAcrossRestarts()
            throws Exception
    {
        InboundDirectoryClaim.claim(directory, FIRST);
        Path file = directory.resolve(InboundDirectoryClaim.FILE);
        // Hidden: never picked up as input.
        assertThat(file.getFileName().toString()).startsWith(".");
        assertThat(Files.readString(file))
                .contains("lease-name=file-connector", "namespace=sdh_ingest")
                .doesNotContain(directory.toString());
        // A restart of the same node is fine.
        InboundDirectoryClaim.claim(directory, FIRST).verify();
    }

    /**
     * Any difference in identity makes another node.
     *
     * @throws Exception if the claim can't be written
     */
    @Test
    void anyDifferenceInIdentityIsAnotherConnector() throws Exception
    {
        InboundDirectoryClaim.claim(directory, FIRST);
        for (Owner other : List.of(
                new Owner("file-connector", "other-lease", "sdh",
                        "sdh_ingest"),
                new Owner("other", "file-connector", "sdh", "sdh_ingest"),
                new Owner("file-connector", "file-connector", "other",
                        "sdh_ingest"),
                new Owner("file-connector", "file-connector", "sdh",
                        "other_ns")))
        {
            assertThatThrownBy(() ->
                    InboundDirectoryClaim.claim(directory, other))
                    .isInstanceOfSatisfying(ClaimedException.class,
                            claimed -> assertThat(claimed.holder())
                                    .isEqualTo(FIRST))
                    .hasMessageContaining("lease-name=file-connector")
                    .hasMessageNotContaining(directory.toString());
        }
    }

    /**
     * Of many nodes starting together, exactly one wins.
     *
     * @throws Exception if a claim fails other than by being taken
     */
    @Test
    void exactlyOneOfManyConnectorsStartingTogetherWins() throws Exception
    {
        ExecutorService pool = Executors.newFixedThreadPool(RACERS);
        try
        {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < RACERS; i++)
            {
                Owner owner = new Owner("c" + i, "lease-" + i, "sdh",
                        "sdh_ingest");
                Callable<Boolean> claim = () ->
                {
                    go.await();
                    try
                    {
                        InboundDirectoryClaim.claim(directory, owner);
                        return true;
                    }
                    catch (ClaimedException taken)
                    {
                        return false;
                    }
                };
                results.add(pool.submit(claim));
            }
            go.countDown();
            int winners = 0;
            for (Future<Boolean> result : results)
            {
                winners += result.get() ? 1 : 0;
            }
            assertThat(winners).isEqualTo(1);
        }
        finally
        {
            pool.shutdownNow();
        }
    }

    /**
     * A running node creates a deleted claim again, and stops when another
     * node has taken it.
     *
     * @throws Exception if the claim can't be written
     */
    @Test
    void aRunningConnectorStopsWhenItsClaimIsTakenAndRecreatesADeletedOne()
            throws Exception
    {
        InboundDirectoryClaim first =
                InboundDirectoryClaim.claim(directory, FIRST);
        Path file = directory.resolve(InboundDirectoryClaim.FILE);

        Files.delete(file);
        first.verify();
        assertThat(file).exists();

        // Someone deletes it and another node starts: the running one finds
        // out before its next file.
        Files.delete(file);
        Owner second = new Owner("other", "other-lease", "sdh", "sdh_ingest");
        InboundDirectoryClaim.claim(directory, second);
        assertThatThrownBy(first::verify).isInstanceOfSatisfying(
                ClaimedException.class,
                claimed -> assertThat(claimed.holder()).isEqualTo(second));
    }

    /**
     * A claim that can't be read is refused.
     *
     * @throws Exception if the file can't be written
     */
    @Test
    void anUnreadableClaimIsRefused() throws Exception
    {
        Files.writeString(directory.resolve(InboundDirectoryClaim.FILE),
                "not a claim\n");
        assertThatThrownBy(() -> InboundDirectoryClaim.claim(directory, FIRST))
                .isInstanceOfSatisfying(ClaimedException.class,
                        claimed -> assertThat(claimed.holder()).isNull())
                .hasMessageContaining("can't be read");
    }
}
