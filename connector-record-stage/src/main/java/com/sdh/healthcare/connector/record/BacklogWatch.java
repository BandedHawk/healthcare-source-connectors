/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.stage.StageRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notes, in the registry, each time a record older than the reconciler's
 * grace comes off the queue: a backlog is still draining, so the records
 * the reconciler would take for lost are most likely still queued
 * (STAGES.md §9). Every pod watches what it receives, so a partition still
 * draining on any of them holds the reconciler back.
 * <p>
 * A record's age is from the commit of the source table's snapshot its
 * message names, by this pod's clock against the stage before's; a
 * difference of seconds against a grace of minutes doesn't matter. A
 * snapshot no longer in the table's metadata, once it has just been read
 * again, was expired, so is older than any grace. Nothing here fails a
 * delivery: a failure to look or to note is logged, and the next record
 * tries again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class BacklogWatch
{

    /** How often each pod notes a backlog, at most. */
    static final Duration NOTE_EVERY = Duration.ofMinutes(1);

    /** How often the table's metadata is read again, at most. */
    static final Duration REFRESH_EVERY = Duration.ofSeconds(10);

    /** The most snapshots whose commit times are kept. */
    static final int CACHED = 1024;

    /** The logger. */
    private static final Logger LOG =
            LoggerFactory.getLogger(BacklogWatch.class);

    /** The source table: its own instance, refreshed here alone. */
    private final Table source;

    /** Where the backlog is noted. */
    private final StageRegistry registry;

    /** How old a record must be to count as backlog. */
    private final Duration grace;

    /** The time. */
    private final Clock clock;

    /** Each snapshot's commit time, the most recently used last. */
    private final Map<Long, Instant> committed =
            new LinkedHashMap<>(16, 0.75f, true)
            {
                /** Kept with the map. */
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(
                        Map.Entry<Long, Instant> eldest)
                {
                    return size() > CACHED;
                }
            };

    /** When the table's metadata was last read again. */
    private Instant refreshed = Instant.MIN;

    /** When this pod last noted a backlog. */
    private Instant noted = Instant.MIN;

    /**
     * Creates the watch.
     *
     * @param source   the source table; not the reconciler's instance,
     *                 which it refreshes itself
     * @param registry where the backlog is noted
     * @param grace    the reconciler's grace
     * @param clock    the time
     */
    public BacklogWatch(Table source, StageRegistry registry,
            Duration grace, Clock clock)
    {
        this.source = source;
        this.registry = registry;
        this.grace = grace;
        this.clock = clock;
    }

    /**
     * Looks at a record that came off the queue, and notes a backlog if it
     * is older than the grace.
     *
     * @param record the record's message
     */
    void received(RecordInStorage record)
    {
        if (record.storage() == null)
        {
            return;
        }
        try
        {
            Optional<Instant> at = committedAt(record.storage().snapshotId());
            Instant now = clock.instant();
            if (at.isPresent() && at.get().isBefore(now.minus(grace))
                    && due(now))
            {
                registry.noteBacklog();
            }
        }
        catch (RuntimeException failure)
        {
            LOG.warn("stage.backlog_not_noted reason={}",
                    failure.getClass().getSimpleName(), failure);
        }
    }

    /**
     * Whether this pod is due to note a backlog again, and if so, that it
     * has.
     *
     * @param now the time
     * @return whether to note it now
     */
    private synchronized boolean due(Instant now)
    {
        if (now.isBefore(noted.plus(NOTE_EVERY)))
        {
            return false;
        }
        noted = now;
        return true;
    }

    /**
     * When a snapshot of the source table was committed.
     *
     * @param snapshotId the snapshot
     * @return its commit time; {@link Instant#EPOCH} if it was expired;
     *         empty if it isn't known yet and the metadata was read again
     *         too recently to look
     */
    synchronized Optional<Instant> committedAt(long snapshotId)
    {
        Instant known = committed.get(snapshotId);
        if (known != null)
        {
            return Optional.of(known);
        }
        Snapshot snapshot = source.snapshot(snapshotId);
        if (snapshot == null)
        {
            Instant now = clock.instant();
            if (now.isBefore(refreshed.plus(REFRESH_EVERY)))
            {
                return Optional.empty();
            }
            source.refresh();
            refreshed = now;
            snapshot = source.snapshot(snapshotId);
        }
        // Not there even now: the stage before sends a record's message
        // only once its snapshot is committed, so it was expired, long ago.
        Instant at = snapshot == null ? Instant.EPOCH
                : Instant.ofEpochMilli(snapshot.timestampMillis());
        committed.put(snapshotId, at);
        return Optional.of(at);
    }
}
