/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.checkpoint.LeasedCommitter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Appends the data files of items split at once on this pod to their
 * table together, in one commit (docs/PERFORMANCE.md §3.1). One thread
 * commits: while it does, the files that arrive wait, and go in its next
 * commit. So a file sent while nothing else is committing is committed at
 * once, with no wait added, and under load each commit takes every file
 * that is waiting, up to {@link #MAX_FILES}.
 * <p>
 * Each file comes with its item's lease. A file whose lease is lost by
 * the time of the commit is left out, and its sender is told; the commit
 * goes ahead only while every other file's lease is held. A commit that
 * fails fails for every file in it, and each item tries again as it would
 * after its own commit failed: it first checks whether the commit had in
 * fact gone through.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class GroupAppender implements AutoCloseable
{

    /** The most files one commit appends. */
    public static final int MAX_FILES = 500;

    /** Logs how many files each commit took, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(GroupAppender.class);

    /**
     * A file to append.
     *
     * @param file  the data file
     * @param lease its item's lease, which must be held at the commit
     * @param done  completes with the snapshot that holds the file
     */
    private record Request(DataFile file, LeaseToken lease,
            CompletableFuture<Snapshot> done)
    {
    }

    /** The table the files are appended to. */
    private final Table table;

    /** The files waiting for the next commit. */
    private final LinkedBlockingQueue<Request> waiting =
            new LinkedBlockingQueue<>();

    /** The thread that commits. */
    private final Thread committer;

    /** Whether the appender is closed. */
    private volatile boolean closed;

    /**
     * Creates an appender and starts its thread.
     *
     * @param table the table the files are appended to
     */
    public GroupAppender(Table table)
    {
        this.table = table;
        this.committer = Thread.ofPlatform().name("sdh-append").daemon(true)
                .start(this::run);
    }

    /**
     * Appends a data file, with others waiting, and waits for the commit.
     *
     * @param file  the data file
     * @param lease its item's lease
     * @return the snapshot the commit made, which holds the file
     * @throws InterruptedException if interrupted while waiting; the file
     *                              may be committed all the same
     * @throws Exception            what the commit threw, such as a lost
     *                              lease or a storage failure
     */
    public Snapshot append(DataFile file, LeaseToken lease) throws Exception
    {
        if (closed)
        {
            throw new IllegalStateException("The appender is closed");
        }
        Request request =
                new Request(file, lease, new CompletableFuture<>());
        waiting.add(request);
        try
        {
            return request.done().get();
        }
        catch (ExecutionException failed)
        {
            Throwable cause = failed.getCause();
            if (cause instanceof Exception exception)
            {
                throw exception;
            }
            if (cause instanceof Error error)
            {
                throw error;
            }
            throw new IllegalStateException(cause);
        }
    }

    /** Commits the waiting files, a group at a time, until closed. */
    private void run()
    {
        List<Request> group = new ArrayList<>();
        while (!closed)
        {
            try
            {
                group.add(waiting.take());
            }
            catch (InterruptedException interrupted)
            {
                break;
            }
            waiting.drainTo(group, MAX_FILES - 1);
            commit(group);
            group.clear();
        }
        for (Request left = waiting.poll(); left != null;
                left = waiting.poll())
        {
            left.done().completeExceptionally(
                    new IllegalStateException("The appender is closed"));
        }
    }

    /**
     * Commits a group of files whose leases are held, and tells each
     * sender how it went.
     *
     * @param group the files
     */
    private void commit(List<Request> group)
    {
        List<Request> held = new ArrayList<>();
        for (Request request : group)
        {
            try
            {
                request.lease().checkHeld();
                held.add(request);
            }
            catch (RuntimeException | Error lost)
            {
                request.done().completeExceptionally(lost);
            }
        }
        if (held.isEmpty())
        {
            return;
        }
        try
        {
            Snapshot snapshot = new LeasedCommitter(allOf(held)).commit(table,
                    transaction ->
                    {
                        AppendFiles append = transaction.newFastAppend();
                        held.forEach(request ->
                                append.appendFile(request.file()));
                        append.commit();
                    });
            LOG.debug("append.committed files={}", held.size());
            held.forEach(request -> request.done().complete(snapshot));
        }
        catch (RuntimeException | Error failure)
        {
            held.forEach(request ->
                    request.done().completeExceptionally(failure));
        }
    }

    /**
     * A lease that is held only while every one of a group's is.
     *
     * @param group the files, with their leases
     * @return the lease
     */
    private static LeaseToken allOf(List<Request> group)
    {
        return new LeaseToken()
        {
            /**
             * {@inheritDoc}
             * <p>
             * A group has no one epoch; nothing that commits a group asks
             * for one.
             */
            @Override
            public long epoch()
            {
                throw new UnsupportedOperationException(
                        "A group of leases has no one epoch");
            }

            @Override
            public void checkHeld()
            {
                group.forEach(request -> request.lease().checkHeld());
            }
        };
    }

    /**
     * Stops the commit thread. Files still waiting are failed, and their
     * items tried again later from their checkpoints.
     */
    @Override
    public void close()
    {
        closed = true;
        committer.interrupt();
        try
        {
            committer.join(30_000);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        for (Request left = waiting.poll(); left != null;
                left = waiting.poll())
        {
            left.done().completeExceptionally(
                    new IllegalStateException("The appender is closed"));
        }
    }
}
