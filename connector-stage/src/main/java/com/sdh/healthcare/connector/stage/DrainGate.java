/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lets a starting pod's release in, or holds it, before the pod takes any
 * work (STAGES.md §14.4a). Once let in, the pod opens its ready port, a
 * plain TCP port that its {@code readinessProbe} checks, so a pod held
 * back isn't ready and a rolling update waits on it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DrainGate implements AutoCloseable
{

    /** How many item IDs a decision gives for each version. */
    public static final int FIRST_ITEMS = 5;

    /** The default time between checks while held. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(1);

    /** The default ready port. */
    public static final int DEFAULT_READY_PORT = 8081;

    /** The log. */
    private static final Logger LOG = LoggerFactory.getLogger(DrainGate.class);

    /** How a release was let in. */
    public enum How
    {

        /** The first release, with no release kept before it. */
        FIRST,

        /** The stage's current release. */
        CURRENT,

        /**
         * The release current before the current one: a rollback, or an old
         * pod restarted during a rollout.
         */
        PREVIOUS,

        /**
         * Each of its versions the current release hasn't is one some item
         * is part way under.
         */
        IN_FLIGHT,

        /** No item is part way under a version it wouldn't stamp. */
        DRAINED,

        /** An operator accepted it while the stage wasn't drained. */
        FORCED;

        /**
         * The name kept and audited.
         *
         * @return the name, in lower case
         */
        public String wireName()
        {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * What the check found.
     *
     * @param how      how the release is let in; null if it is held
     * @param blocking the items part way under versions it wouldn't stamp,
     *                 by version; empty if it is let in without looking
     */
    public record Decision(How how, List<PartWay.Items> blocking)
    {

        /**
         * Copies the items.
         */
        public Decision
        {
            blocking = List.copyOf(blocking);
        }

        /**
         * Whether the release is let in.
         *
         * @return true if it is
         */
        public boolean letIn()
        {
            return how != null;
        }

        /**
         * How many items hold the release back.
         *
         * @return the count
         */
        public long blockingCount()
        {
            return blocking.stream().mapToLong(PartWay.Items::count).sum();
        }
    }

    /** The stage. */
    private final String stage;

    /** This pod's release. */
    private final StageReleases.Release release;

    /** The releases let in and accepted. */
    private final StageReleases releases;

    /** Finds the items part way. */
    private final PartWay partWay;

    /** The time between checks while held. */
    private final Duration interval;

    /** The ready port; 0 for any free one; below 0 for none. */
    private final int readyPort;

    /** Counted down when the gate is closed, to stop waiting. */
    private final CountDownLatch closed = new CountDownLatch(1);

    /** The ready port's socket, once open; null before. */
    private volatile ServerSocket ready;

    /**
     * Creates a gate.
     *
     * @param stage     the stage
     * @param release   this pod's release
     * @param releases  the releases let in and accepted
     * @param partWay   finds the items part way
     * @param interval  the time between checks while held
     * @param readyPort the port to open once let in; 0 for any free one;
     *                  below 0 for none
     */
    public DrainGate(String stage, StageReleases.Release release,
            StageReleases releases, PartWay partWay, Duration interval,
            int readyPort)
    {
        this.stage = stage;
        this.release = release;
        this.releases = releases;
        this.partWay = partWay;
        this.interval = interval;
        this.readyPort = readyPort;
    }

    /**
     * Opens a stage's gate, creating the release tables if missing, with
     * the stage's settings: {@code sdh.stage.ready-port} and
     * {@code sdh.stage.version-check-interval}.
     *
     * @param dataSource the lease database
     * @param settings   the stage's settings
     * @param stage      the stage
     * @param versions   the versions this pod stamps
     * @param partWay    finds the stage's items part way
     * @return the gate
     * @throws IllegalStateException    if the tables can't be created
     * @throws IllegalArgumentException if the interval isn't positive
     */
    public static DrainGate open(DataSource dataSource,
            StageProperties settings, String stage,
            Collection<String> versions, PartWay partWay)
    {
        Duration interval = settings.versionCheckInterval();
        if (interval == null || !interval.isPositive())
        {
            throw new IllegalArgumentException(StageProperties.PREFIX
                    + ".version-check-interval must be positive");
        }
        StageReleases releases = new StageReleases(dataSource,
                settings.registrySchema(), stage, Duration.ofSeconds(10));
        releases.createTablesIfMissing();
        return new DrainGate(stage, StageReleases.Release.of(versions),
                releases, partWay, interval, settings.readyPort());
    }

    /**
     * This pod's release.
     *
     * @return the release
     */
    public StageReleases.Release release()
    {
        return release;
    }

    /**
     * Checks this pod's release, changing nothing (STAGES.md §14.4a, rule
     * 3).
     *
     * @return what was found
     * @throws IllegalStateException if the database can't be reached
     */
    public Decision decide()
    {
        StageReleases.LetIn[] both = releases.currentAndPrevious();
        StageReleases.LetIn current = both[0];
        StageReleases.LetIn previous = both[1];
        if (current == null)
        {
            return new Decision(How.FIRST, List.of());
        }
        if (current.release().digest().equals(release.digest()))
        {
            return new Decision(How.CURRENT, List.of());
        }
        if (previous != null
                && previous.release().digest().equals(release.digest()))
        {
            return new Decision(How.PREVIOUS, List.of());
        }
        List<PartWay.Items> parts = partWay.partWay(FIRST_ITEMS);
        Set<String> inFlight = new HashSet<>();
        for (PartWay.Items items : parts)
        {
            inFlight.add(items.version());
        }
        Set<String> changed = new HashSet<>(release.versions());
        changed.removeAll(current.release().versions());
        if (inFlight.containsAll(changed))
        {
            return new Decision(How.IN_FLIGHT, List.of());
        }
        List<PartWay.Items> blocking = parts.stream()
                .filter(items -> !release.versions().contains(
                        items.version()))
                .toList();
        if (blocking.isEmpty())
        {
            return new Decision(How.DRAINED, List.of());
        }
        if (releases.accepted(release).isPresent())
        {
            return new Decision(How.FORCED, blocking);
        }
        return new Decision(null, blocking);
    }

    /**
     * Waits until this pod's release is let in, checking again every
     * interval; then records it, if it wasn't current, and opens the
     * ready port. While held, it audits {@code stage.version_blocked}
     * whenever what holds it changes.
     *
     * @return true once let in; false if the gate was closed first
     * @throws UncheckedIOException if the ready port can't be opened
     */
    public boolean awaitLetIn()
    {
        String held = null;
        while (true)
        {
            Decision decision;
            try
            {
                decision = decide();
            }
            catch (IllegalStateException unreachable)
            {
                LOG.warn("stage.version_check_failed stage={}", stage);
                decision = null;
            }
            if (decision != null && decision.letIn())
            {
                admit(decision);
                return true;
            }
            if (decision != null)
            {
                String what = decision.blockingCount() + "/"
                        + decision.blocking().size();
                if (!what.equals(held))
                {
                    Telemetry.get().auditWarn("stage.version_blocked",
                            Fields.of(Attr.STAGE, stage)
                                    .with(Attr.RELEASE, release.digest())
                                    .with(Attr.COUNT,
                                            decision.blockingCount())
                                    .with(Attr.VERSION_COUNT,
                                            decision.blocking().size()));
                    held = what;
                }
            }
            try
            {
                if (closed.await(interval.toMillis(), TimeUnit.MILLISECONDS))
                {
                    return false;
                }
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Records a release let in, audits it and opens the ready port.
     *
     * @param decision how it was let in
     */
    private void admit(Decision decision)
    {
        if (decision.how() != How.CURRENT)
        {
            releases.letIn(release, decision.how().wireName());
            Telemetry.get().audit("stage.version_changed",
                    Fields.of(Attr.STAGE, stage)
                            .with(Attr.RELEASE, release.digest())
                            .with(Attr.LET_IN_BY, decision.how().wireName())
                            .with(Attr.VERSION_COUNT,
                                    release.versions().size())
                            .with(Attr.COUNT, decision.blockingCount()));
        }
        LOG.info("stage.version_let_in stage={} release={} how={}", stage,
                release.digest(), decision.how().wireName());
        openReadyPort();
    }

    /**
     * Opens the ready port, which takes each connection and closes it at
     * once.
     *
     * @throws UncheckedIOException if it can't be opened
     */
    private void openReadyPort()
    {
        if (readyPort < 0 || closed.getCount() == 0)
        {
            return;
        }
        try
        {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(readyPort));
            ready = socket;
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        Thread accepter = new Thread(() ->
        {
            ServerSocket socket = ready;
            while (!socket.isClosed())
            {
                try (Socket probe = socket.accept())
                {
                    // A probe only connects.
                }
                catch (IOException closedOrFailed)
                {
                    // Closed: the loop ends. Else the next probe is taken.
                }
            }
        }, "stage-ready-port");
        accepter.setDaemon(true);
        accepter.start();
    }

    /**
     * The ready port's number, once open.
     *
     * @return the port; -1 if it isn't open
     */
    public int readyPort()
    {
        ServerSocket socket = ready;
        return socket == null ? -1 : socket.getLocalPort();
    }

    /** Stops waiting, and closes the ready port. */
    @Override
    public void close()
    {
        closed.countDown();
        ServerSocket socket = ready;
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException ignored)
            {
                // closing
            }
        }
    }
}
