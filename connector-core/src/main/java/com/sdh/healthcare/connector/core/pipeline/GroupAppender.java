/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
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

    /**
     * Commits tried before a group fails, each with Iceberg's own retries
     * inside it.
     */
    static final int ATTEMPTS = 10;

    /** Logs how many files each commit took, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(GroupAppender.class);

    /**
     * A file to append.
     *
     * @param file     the data file
     * @param lease    its item's lease, which must be held at the commit
     * @param done     completes with the snapshot that holds the file
     * @param queuedAt when it was sent, from {@link System#nanoTime()}
     */
    private record Request(DataFile file, LeaseToken lease,
            CompletableFuture<Snapshot> done, long queuedAt)
    {
    }

    /** The table the files are appended to. */
    private final Table table;

    /** The files waiting for the next commit. */
    private final LinkedBlockingQueue<Request> waiting =
            new LinkedBlockingQueue<>();

    /** What commits a group. */
    private final Committer committer;

    /** The thread that commits. */
    private final Thread thread;

    /** Whether the appender is closed. */
    private volatile boolean closed;

    /**
     * Commits a pod's group of files.
     */
    public interface Committer extends AutoCloseable
    {

        /**
         * Commits a group of files to a table.
         *
         * @param table  the table
         * @param files  the files
         * @param leases each one's item lease, held a moment ago
         * @return a snapshot that holds the files
         * @throws LeaseLostException if a lease is lost before the commit
         * @throws RuntimeException   if the commit fails; the files may
         *                            have been committed only if this is
         *                            Iceberg's
         *                            {@code CommitStateUnknownException}
         */
        Snapshot commit(Table table, List<DataFile> files,
                List<LeaseToken> leases);

        /**
         * Settles, for an item leased again, any of its files that an
         * earlier lease holder sent and that aren't settled yet, so that
         * none is committed after this returns: called before the new
         * holder checks what the table holds. Nothing to do for a
         * committer that doesn't keep files beyond its pod.
         *
         * @param table the table
         * @param lease the item's new lease
         */
        default void settle(Table table, LeaseToken lease)
        {
        }

        /**
         * Lets go of what the committer holds, once the appender's thread
         * has stopped. Nothing by default.
         */
        @Override
        default void close()
        {
        }
    }

    /**
     * Creates an appender that commits its pod's files itself, and starts
     * its thread.
     *
     * @param table the table the files are appended to
     */
    public GroupAppender(Table table)
    {
        this(table, new Alone());
    }

    /**
     * Creates an appender and starts its thread.
     *
     * @param table     the table the files are appended to
     * @param committer what commits a group
     */
    public GroupAppender(Table table, Committer committer)
    {
        this.table = table;
        this.committer = committer;
        this.thread = Thread.ofPlatform().name("sdh-append").daemon(true)
                .start(this::run);
    }

    /**
     * Settles any files of an item that an earlier holder of its lease
     * sent ({@link Committer#settle}).
     *
     * @param lease the item's new lease
     */
    public void settle(LeaseToken lease)
    {
        committer.settle(table, lease);
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
                new Request(file, lease, new CompletableFuture<>(),
                        System.nanoTime());
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
     * sender how it went, timing the wait and the commit.
     *
     * @param group the files
     */
    private void commit(List<Request> group)
    {
        // How long each waited for the commit before, and how long this
        // one takes (docs/PERFORMANCE.md §10).
        Telemetry telemetry = Telemetry.get();
        long started = System.nanoTime();
        for (Request request : group)
        {
            telemetry.phaseDuration(ItemProcessor.STAGE, "commit_queue",
                    started - request.queuedAt());
        }
        try
        {
            commitHeld(group);
        }
        finally
        {
            telemetry.phaseDuration(ItemProcessor.STAGE, "commit_group",
                    System.nanoTime() - started);
        }
    }

    /**
     * Commits the files of a group whose leases are still held.
     *
     * @param group the requests
     */
    private void commitHeld(List<Request> group)
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
            Snapshot snapshot = committer.commit(table,
                    held.stream().map(Request::file).toList(),
                    held.stream().map(Request::lease).toList());
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
     * Commits a pod's files itself: the pods' commits conflict when they
     * meet.
     */
    static final class Alone implements Committer
    {

        /** Creates the committer. */
        Alone()
        {
        }

        /**
         * {@inheritDoc}
         * <p>
         * One fast append on the table itself, not in a transaction
         * (docs/PERFORMANCE.md §10, revision (dl)). Every split pod appends
         * to the table, so a commit often finds another's has gone in
         * first: Iceberg then reads the table again, puts the files on top
         * and tries again, as the table's {@code commit.retry.num-retries}
         * says, and this tries again on top of that. A transaction also
         * read, after each commit, the manifest list of every snapshot
         * added since it began, to find files to clean up (an append has
         * none), and read the table once more before and after.
         * <p>
         * Every lease is checked before each attempt; Iceberg's own
         * retries, inside the commit, check none.
         *
         * @return the table's snapshot just after the commit, which holds
         *         the files (another pod's, made just after, may be the one)
         * @throws org.apache.iceberg.exceptions.CommitFailedException if
         *         every attempt conflicts
         */
        @Override
        public Snapshot commit(Table table, List<DataFile> files,
                List<LeaseToken> leases)
        {
            for (int attempt = 1; ; attempt++)
            {
                leases.forEach(LeaseToken::checkHeld);
                AppendFiles append = table.newFastAppend();
                files.forEach(append::appendFile);
                try
                {
                    append.commit();
                    return table.currentSnapshot();
                }
                catch (CommitFailedException conflict)
                {
                    LOG.warn("append.conflict attempt={} table={}", attempt,
                            table.name());
                    if (attempt >= ATTEMPTS)
                    {
                        throw conflict;
                    }
                }
            }
        }
    }

    /**
     * Stops the commit thread. Files still waiting are failed, and their
     * items tried again later from their checkpoints.
     */
    @Override
    public void close()
    {
        closed = true;
        thread.interrupt();
        try
        {
            thread.join(30_000);
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
        committer.close();
    }
}
