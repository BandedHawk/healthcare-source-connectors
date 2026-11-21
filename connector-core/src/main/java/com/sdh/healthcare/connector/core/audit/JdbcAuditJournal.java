/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import com.sdh.healthcare.connector.core.telemetry.AuditJournal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The durable audit journal (HIPAA.md §3.1, audit controls): every audit
 * event of a process, written to the lease database as one hash-chained
 * series of entries ({@link AuditEntry}), whose head the stage's key seals
 * at intervals and at the end ({@link AuditSeal}). Each process starts a
 * chain of its own, so no two processes ever write the same chain.
 * <p>
 * <b>Nothing is dropped.</b> Events are numbered and chained as they are
 * appended, and written in batches by a thread of the journal's own, over
 * a connection pool of its own, so a stage's threads holding the stage's
 * connections can't keep it from writing. While the database can't be
 * written the batch is kept and tried again; once the queue is full,
 * {@link #append} waits, so a stage that can't keep its audit trail stops
 * rather than going on without it. What only a process that dies can lose
 * is the tail not yet written; its chain is then left without a close,
 * which {@link AuditTrail#verify} reports.
 * <p>
 * A seal costs a signature: by the pod's own key, the KMS signing once an
 * hour (docs/PERFORMANCE.md §3.5), or by the KMS each
 * {@link Settings#sealInterval} with pod keys off. Sealing that fails, as
 * when the KMS is down, is tried again at the next interval; the entries
 * are kept either way. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class JdbcAuditJournal implements AuditJournal, AutoCloseable
{

    /** The console log, for the journal's own troubles: no event data. */
    private static final Logger LOG =
            LoggerFactory.getLogger(JdbcAuditJournal.class);

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT_SECONDS = 30;

    /** How long the writer waits for an event before looking up. */
    private static final long POLL_MILLIS = 200;

    /** How long {@link #append} waits before saying, again, that it waits. */
    private static final long WAIT_NOTICE_SECONDS = 30;

    /** The longest wait between attempts to write. */
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /**
     * How long closing waits when the last write had failed: a moment for
     * one last try.
     */
    private static final Duration FAILING_CLOSE_WAIT = Duration.ofSeconds(1);

    /**
     * Signs a chain's head.
     */
    @FunctionalInterface
    public interface Sealer
    {

        /**
         * Seals an entry: signs the chain, its number and its hash.
         *
         * @param chainId the chain
         * @param seq     the entry's number
         * @param hash    the entry's hash
         * @param at      when
         * @return the seal, as text
         * @throws Exception if it can't be signed
         */
        String seal(UUID chainId, long seq, byte[] hash, Instant at)
                throws Exception;
    }

    /**
     * How the journal works.
     *
     * @param capacity     the most events waiting to be written before
     *                     {@link #append} waits
     * @param batchSize    the most entries written in one transaction
     * @param sealInterval how often the chain's head is sealed while it
     *                     grows
     * @param flushWait    how long {@link #flush} waits at most
     * @param closeWait    how long {@link #close} waits at most for what is
     *                     left to be written
     */
    public record Settings(int capacity, int batchSize, Duration sealInterval,
            Duration flushWait, Duration closeWait)
    {

        /**
         * 50,000 events, batches of 500, sealed each minute; flushes wait
         * 5 seconds, closing 10.
         */
        public static final Settings DEFAULT = new Settings(50_000, 500,
                Duration.ofMinutes(1), Duration.ofSeconds(5),
                Duration.ofSeconds(10));

        /**
         * Checks the settings.
         *
         * @throws IllegalArgumentException if a size isn't positive, or the
         *                                  seal interval is under a second
         */
        public Settings
        {
            if (capacity < 1 || batchSize < 1)
            {
                throw new IllegalArgumentException(
                        "The audit journal's capacity and batch size must be"
                                + " at least 1");
            }
            if (sealInterval.compareTo(Duration.ofSeconds(1)) < 0)
            {
                throw new IllegalArgumentException(
                        "The audit journal's seal interval must be at least"
                                + " 1 second");
            }
        }
    }

    /** Connections to the database: a pool of the journal's own. */
    private final DataSource dataSource;

    /** The tables. */
    private final AuditTables tables;

    /** What the chain is of, such as {@code convert/<connector ID>}. */
    private final String source;

    /** The pod, or host, the process runs on. */
    private final String pod;

    /** Seals the chain's head; null to leave it unsealed. */
    private final Sealer sealer;

    /** How the journal works. */
    private final Settings settings;

    /** Tells the time. */
    private final Clock clock;

    /** The chain this process writes. */
    private final UUID chainId = UUID.randomUUID();

    /** Entries appended and not yet taken by the writer. */
    private final BlockingQueue<AuditEntry> queue;

    /** The number of the last entry appended; guarded by this. */
    private long appended;

    /** The hash of the last entry appended; guarded by this. */
    private byte[] lastHash = AuditEntry.GENESIS;

    /** The number of the last entry written; guarded by {@link #written}. */
    private long writtenSeq;

    /** The hash of the last entry written; guarded by {@link #written}. */
    private byte[] writtenHash = AuditEntry.GENESIS;

    /** Guards what has been written, and is told when more has. */
    private final Object written = new Object();

    /** The number of the last entry sealed; the writer's only. */
    private long sealedSeq;

    /** When the head was last sealed, or tried; the writer's only. */
    private Instant lastSealTry;

    /** Writes the entries; null until started. */
    private Thread writer;

    /**
     * Set when the journal is closing: append nothing more. Guarded by
     * this.
     */
    private boolean closing;

    /** Whether the last write failed: the database is out of reach. */
    private volatile boolean failing;

    /**
     * Creates the journal; {@link #start} starts it.
     *
     * @param dataSource connections to the database: a pool of the
     *                   journal's own, of one connection
     * @param tables     the tables
     * @param source     what the chain is of, such as
     *                   {@code convert/<connector ID>}
     * @param pod        the pod, or host, the process runs on
     * @param sealer     seals the chain's head; null to leave it unsealed,
     *                   as for an operator command, which has no stage key
     * @param settings   how it works
     * @param clock      tells the time
     */
    public JdbcAuditJournal(DataSource dataSource, AuditTables tables,
            String source, String pod, Sealer sealer, Settings settings,
            Clock clock)
    {
        this.dataSource = dataSource;
        this.tables = tables;
        this.source = source;
        this.pod = pod;
        this.sealer = sealer;
        this.settings = settings;
        this.clock = clock;
        this.queue = new ArrayBlockingQueue<>(settings.capacity());
    }

    /**
     * The chain this process writes.
     *
     * @return its ID
     */
    public UUID chainId()
    {
        return chainId;
    }

    /**
     * Creates the tables if they aren't there, records the chain, and
     * starts writing.
     *
     * @return this journal
     * @throws IllegalStateException if the database can't be written
     */
    public JdbcAuditJournal start()
    {
        tables.createIfMissing(dataSource, TIMEOUT_SECONDS);
        Instant now = clock.instant();
        String insert = """
                INSERT INTO %s (chain_id, source, pod, started_at, last_seq,
                                last_hash, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?)""".formatted(tables.chains());
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(insert))
        {
            statement.setQueryTimeout(TIMEOUT_SECONDS);
            statement.setObject(1, chainId);
            statement.setString(2, source);
            statement.setString(3, pod);
            statement.setObject(4, time(now));
            statement.setBytes(5, AuditEntry.GENESIS);
            statement.setObject(6, time(now));
            statement.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not start the audit journal", failure);
        }
        lastSealTry = now;
        writer = new Thread(this::write, "sdh-audit-journal");
        writer.setDaemon(true);
        writer.start();
        LOG.info("audit.journal_started chain_id={}", chainId);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Numbers and chains the event at once; waits while the queue is
     * full. After {@link #close} the event is kept only in the console log.
     */
    @Override
    public void append(Event event)
    {
        synchronized (this)
        {
            if (closing)
            {
                LOG.warn("audit.journal_closed event={}", event.name());
                return;
            }
            AuditEntry entry = AuditEntry.next(chainId, appended + 1,
                    event.at(), event.name(), event.severity(),
                    event.attributes(), lastHash);
            appended = entry.seq();
            lastHash = entry.hash();
            boolean interrupted = false;
            try
            {
                while (true)
                {
                    try
                    {
                        if (queue.offer(entry, WAIT_NOTICE_SECONDS,
                                TimeUnit.SECONDS))
                        {
                            break;
                        }
                        LOG.warn("audit.journal_waiting chain_id={}"
                                + " waiting={}", chainId, queue.size());
                    }
                    catch (InterruptedException stop)
                    {
                        // Kept all the same: the trail mustn't have a hole.
                        interrupted = true;
                    }
                }
            }
            finally
            {
                if (interrupted)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Waits until everything appended so far is written, or
     * {@link Settings#flushWait} has passed.
     */
    @Override
    public void flush()
    {
        long target;
        synchronized (this)
        {
            target = appended;
        }
        awaitWritten(target, settings.flushWait());
    }

    /**
     * Writes what is left, seals the head and closes the chain, waiting up
     * to {@link Settings#closeWait}. If the database can't be written in
     * that time (a second if the last write had failed already), or a
     * write fails once closing has begun, what is left is
     * lost, logged with its count, and the chain stays open, which {@link
     * AuditTrail#verify} reports.
     */
    @Override
    public void close()
    {
        synchronized (this)
        {
            if (closing)
            {
                return;
            }
            closing = true;
            // Wakes a writer waiting to retry, for one last try.
            notifyAll();
        }
        if (writer == null)
        {
            return;
        }
        try
        {
            // With the database already out of reach, a last try gets a
            // moment only: a longer wait just holds up the stop.
            writer.join((failing ? FAILING_CLOSE_WAIT
                    : settings.closeWait()).toMillis());
        }
        catch (InterruptedException stop)
        {
            Thread.currentThread().interrupt();
        }
        if (writer.isAlive())
        {
            writer.interrupt();
            LOG.error("audit.journal_not_closed chain_id={} unwritten={}",
                    chainId, queue.size());
        }
    }

    /**
     * Waits until the entries up to a number are written.
     *
     * @param seq  the number
     * @param wait how long to wait at most
     * @return true if they were written in time
     */
    boolean awaitWritten(long seq, Duration wait)
    {
        long deadline = System.nanoTime() + wait.toNanos();
        synchronized (written)
        {
            while (writtenSeq < seq)
            {
                long left = deadline - System.nanoTime();
                if (left <= 0)
                {
                    return false;
                }
                try
                {
                    TimeUnit.NANOSECONDS.timedWait(written, left);
                }
                catch (InterruptedException stop)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * The writer: takes entries in batches and writes each batch until it
     * is written, sealing the head at intervals; at close, writes what is
     * left, seals and closes the chain.
     */
    private void write()
    {
        List<AuditEntry> batch = new ArrayList<>();
        Duration backoff = Duration.ofSeconds(1);
        try
        {
            while (true)
            {
                if (batch.isEmpty())
                {
                    AuditEntry first = queue.poll(POLL_MILLIS,
                            TimeUnit.MILLISECONDS);
                    if (first == null)
                    {
                        if (finished())
                        {
                            break;
                        }
                        sealIfDue(false);
                        continue;
                    }
                    batch.add(first);
                    queue.drainTo(batch, settings.batchSize() - 1);
                }
                try
                {
                    store(batch);
                    failing = false;
                    batch.clear();
                    backoff = Duration.ofSeconds(1);
                    sealIfDue(false);
                }
                catch (SQLException failure)
                {
                    failing = true;
                    LOG.warn("audit.journal_write_failed chain_id={}"
                            + " reason={} sql_state={} waiting={}", chainId,
                            failure.getClass().getSimpleName(),
                            failure.getSQLState(),
                            batch.size() + queue.size());
                    if (isClosing())
                    {
                        // Stopping with the database out of reach: waiting
                        // only holds up the stop. The chain stays open,
                        // which verify reports.
                        LOG.error("audit.journal_not_closed chain_id={}"
                                + " unwritten={}", chainId,
                                batch.size() + queue.size());
                        return;
                    }
                    pause(backoff);
                    backoff = backoff.multipliedBy(2);
                    if (backoff.compareTo(MAX_BACKOFF) > 0)
                    {
                        backoff = MAX_BACKOFF;
                    }
                }
            }
            sealIfDue(true);
            closeChain();
        }
        catch (InterruptedException stop)
        {
            // close() gave up waiting: the chain stays open.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Waits before retrying a write, or until {@link #close} begins.
     *
     * @param backoff how long
     * @throws InterruptedException if the writer is interrupted
     */
    private synchronized void pause(Duration backoff)
            throws InterruptedException
    {
        if (!closing)
        {
            wait(backoff.toMillis());
        }
    }

    /**
     * Whether the journal is closing.
     *
     * @return true once {@link #close} has begun
     */
    private synchronized boolean isClosing()
    {
        return closing;
    }

    /**
     * Whether the journal is closing and everything appended has been
     * taken: checked holding the lock appends hold, so none can come
     * after.
     *
     * @return true if the writer may stop
     */
    private synchronized boolean finished()
    {
        return closing && queue.isEmpty();
    }

    /**
     * Writes a batch of entries and moves the chain's head to its last, in
     * one transaction.
     *
     * @param batch the entries, in order
     * @throws SQLException if they can't be written
     */
    private void store(List<AuditEntry> batch) throws SQLException
    {
        AuditEntry last = batch.getLast();
        String insert = """
                INSERT INTO %s (chain_id, seq, at, name, severity,
                                attributes, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)""".formatted(tables.events());
        String head = """
                UPDATE %s SET last_seq = ?, last_hash = ?, updated_at = ?
                 WHERE chain_id = ?""".formatted(tables.chains());
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try (PreparedStatement rows = connection.prepareStatement(insert);
                    PreparedStatement chain =
                            connection.prepareStatement(head))
            {
                rows.setQueryTimeout(TIMEOUT_SECONDS);
                for (AuditEntry entry : batch)
                {
                    rows.setObject(1, chainId);
                    rows.setLong(2, entry.seq());
                    rows.setObject(3, time(entry.at()));
                    rows.setString(4, entry.name());
                    rows.setString(5, entry.severity());
                    rows.setString(6, entry.attributesJson());
                    rows.setBytes(7, entry.hash());
                    rows.addBatch();
                }
                rows.executeBatch();
                chain.setQueryTimeout(TIMEOUT_SECONDS);
                chain.setLong(1, last.seq());
                chain.setBytes(2, last.hash());
                chain.setObject(3, time(clock.instant()));
                chain.setObject(4, chainId);
                chain.executeUpdate();
                connection.commit();
            }
            catch (SQLException failure)
            {
                connection.rollback();
                throw failure;
            }
            finally
            {
                connection.setAutoCommit(true);
            }
        }
        synchronized (written)
        {
            writtenSeq = last.seq();
            writtenHash = last.hash();
            written.notifyAll();
        }
    }

    /**
     * Seals the written head if it has grown since the last seal and the
     * interval has passed, or at the end. A seal that fails is logged and
     * left to the next interval.
     *
     * @param last whether this is the end of the chain, when the head is
     *             sealed whatever the interval
     */
    private void sealIfDue(boolean last)
    {
        Instant now = clock.instant();
        long seq;
        byte[] hash;
        synchronized (written)
        {
            seq = writtenSeq;
            hash = writtenHash;
        }
        if (sealer == null || seq == sealedSeq || (!last
                && Duration.between(lastSealTry, now)
                        .compareTo(settings.sealInterval()) < 0))
        {
            return;
        }
        lastSealTry = now;
        String insert = """
                INSERT INTO %s (chain_id, seq, sealed_at, seal)
                VALUES (?, ?, ?, ?)""".formatted(tables.seals());
        try
        {
            String seal = sealer.seal(chainId, seq, hash, now);
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement =
                            connection.prepareStatement(insert))
            {
                statement.setQueryTimeout(TIMEOUT_SECONDS);
                statement.setObject(1, chainId);
                statement.setLong(2, seq);
                statement.setObject(3, time(now));
                statement.setString(4, seal);
                statement.executeUpdate();
            }
            sealedSeq = seq;
        }
        catch (Exception failure)
        {
            LOG.warn("audit.journal_seal_failed chain_id={} seq={} reason={}",
                    chainId, seq, failure.getClass().getSimpleName());
        }
    }

    /** Records that the chain has ended, with everything written. */
    private void closeChain()
    {
        String update = """
                UPDATE %s SET closed_at = ?, updated_at = ?
                 WHERE chain_id = ?""".formatted(tables.chains());
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(update))
        {
            Instant now = clock.instant();
            statement.setQueryTimeout(TIMEOUT_SECONDS);
            statement.setObject(1, time(now));
            statement.setObject(2, time(now));
            statement.setObject(3, chainId);
            statement.executeUpdate();
            LOG.info("audit.journal_closed chain_id={} entries={}", chainId,
                    writtenSeq());
        }
        catch (SQLException failure)
        {
            LOG.warn("audit.journal_close_failed chain_id={} reason={}",
                    chainId, failure.getClass().getSimpleName());
        }
    }

    /**
     * The number of the last entry written.
     *
     * @return it; 0 if none has been
     */
    long writtenSeq()
    {
        synchronized (written)
        {
            return writtenSeq;
        }
    }

    /**
     * A time as the database takes it.
     *
     * @param at the time
     * @return it, in UTC
     */
    static OffsetDateTime time(Instant at)
    {
        return OffsetDateTime.ofInstant(at, ZoneOffset.UTC);
    }
}
