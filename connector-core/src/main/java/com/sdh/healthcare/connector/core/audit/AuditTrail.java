/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import java.security.MessageDigest;
import java.security.interfaces.ECPublicKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Consumer;
import javax.sql.DataSource;

/**
 * Reads the audit journal (HIPAA.md §3.1, audit controls): checks its
 * chains for changes and gaps, exports its events for the operator's SIEM,
 * and prunes chains older than the operator's retention. What it reads
 * holds no PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AuditTrail
{

    /** How many rows are fetched at a time when streaming. */
    private static final int FETCH_SIZE = 1000;

    /** How many chains are deleted in one transaction when pruning. */
    private static final int PRUNE_BATCH = 100;

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The tables. */
    private final AuditTables tables;

    /** How long a statement may run, in seconds. */
    private final int timeout;

    /**
     * Reads a journal.
     *
     * @param dataSource connections to the database
     * @param tables     the journal's tables
     * @param timeout    how long a statement may run
     */
    public AuditTrail(DataSource dataSource, AuditTables tables,
            Duration timeout)
    {
        this.dataSource = dataSource;
        this.tables = tables;
        this.timeout = (int) Math.max(1, timeout.toSeconds());
    }

    /**
     * The stage key that seals the chains.
     *
     * @param kid its ID
     * @param key its public half
     */
    public record SealKey(String kid, ECPublicKey key)
    {
    }

    /** What may be wrong with a chain. */
    public enum Kind
    {
        /** Entries are missing within the chain. */
        GAP(true),
        /** An entry's hash isn't what its fields and the one before give. */
        BROKEN(true),
        /** The chain's head names entries that aren't there. */
        HEAD_MISSING(true),
        /** The chain's head names another hash than its last entry's. */
        HEAD_MISMATCH(true),
        /** A seal isn't the stage key's signature. */
        SEAL_INVALID(true),
        /** A seal is signed, but over another hash than the entry's. */
        SEAL_MISMATCH(true),
        /** A seal names an entry that isn't there. */
        SEAL_ORPHAN(true),
        /**
         * Entries after the last valid seal: as at the end of any chain
         * still being written, or one whose process stopped before it
         * sealed. Their hashes are checked, but nothing signed vouches for
         * them.
         */
        UNSEALED_TAIL(false),
        /**
         * The chain was never closed and hasn't been written for a while:
         * its process stopped abruptly, and events it hadn't yet written
         * are lost.
         */
        UNCLOSED(false);

        /** Whether the chain was changed after it was written. */
        private final boolean tampering;

        /**
         * Creates a kind.
         *
         * @param tampering whether it means the chain was changed
         */
        Kind(boolean tampering)
        {
            this.tampering = tampering;
        }

        /**
         * Whether it means the chain was changed after it was written,
         * rather than that it ended early.
         *
         * @return true if it does
         */
        public boolean tampering()
        {
            return tampering;
        }
    }

    /**
     * Something wrong with a chain.
     *
     * @param kind  what
     * @param seq   the entry where it starts
     * @param count how many entries it covers
     */
    public record Problem(Kind kind, long seq, long count)
    {
    }

    /**
     * What checking one chain found.
     *
     * @param chainId    the chain
     * @param source     what it is of
     * @param pod        the pod that wrote it
     * @param startedAt  when it started
     * @param closedAt   when it closed; null if it didn't
     * @param entries    how many entries it has
     * @param seals      how many seals it has
     * @param sealedUpTo the last entry a valid seal vouches for; 0 for none,
     *                   -1 when no key was given to check them
     * @param problems   what is wrong; empty if nothing
     */
    public record ChainReport(UUID chainId, String source, String pod,
            Instant startedAt, Instant closedAt, long entries, long seals,
            long sealedUpTo, List<Problem> problems)
    {

        /**
         * Whether anything found means the chain was changed.
         *
         * @return true if it does
         */
        public boolean tampered()
        {
            return problems.stream().anyMatch(each -> each.kind().tampering());
        }
    }

    /**
     * What checking the chains found.
     *
     * @param chains each chain's report, oldest first
     */
    public record Verification(List<ChainReport> chains)
    {

        /**
         * Whether no chain was changed.
         *
         * @return true if none was
         */
        public boolean intact()
        {
            return chains.stream().noneMatch(ChainReport::tampered);
        }

        /**
         * How many entries were checked.
         *
         * @return the count
         */
        public long entries()
        {
            return chains.stream().mapToLong(ChainReport::entries).sum();
        }
    }

    /**
     * An event, as exported.
     *
     * @param chainId    its chain
     * @param seq        its number in the chain
     * @param at         when it was emitted
     * @param name       its name
     * @param severity   its severity
     * @param attributes its attributes
     * @param hash       its hash in the chain
     */
    public record Exported(UUID chainId, long seq, Instant at, String name,
            String severity, SortedMap<String, String> attributes,
            byte[] hash)
    {
    }

    /**
     * What pruning removed.
     *
     * @param chains  how many chains
     * @param entries how many entries
     */
    public record Pruned(long chains, long entries)
    {
    }

    /**
     * Checks the chains with events in a span of time, each from its start:
     * every entry's hash and number, the head each chain's row names, and,
     * with a key, every seal.
     *
     * @param from      the start of the span; null for the beginning
     * @param to        its end, exclusive; null for now
     * @param key       the stage key that seals the chains; null to leave
     *                  the seals unchecked
     * @param now       the time now
     * @param staleAfter how long an unclosed chain may go unwritten before
     *                  it counts as ended abruptly
     * @return what was found
     * @throws SQLException if the journal can't be read
     */
    public Verification verify(Instant from, Instant to, SealKey key,
            Instant now, Duration staleAfter) throws SQLException
    {
        List<ChainReport> reports = new ArrayList<>();
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                for (ChainRow chain : chains(connection, from, to))
                {
                    reports.add(verify(connection, chain, key, now,
                            staleAfter));
                }
            }
            finally
            {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        }
        return new Verification(reports);
    }

    /**
     * Streams the events emitted in a span of time, oldest first.
     *
     * @param from   the start of the span; null for the beginning
     * @param to     its end, exclusive; null for now
     * @param action given each event
     * @return how many were exported
     * @throws SQLException if the journal can't be read
     */
    public long export(Instant from, Instant to, Consumer<Exported> action)
            throws SQLException
    {
        String query = """
                SELECT chain_id, seq, at, name, severity, attributes, hash
                  FROM %s
                 WHERE at >= ? AND at < ?
                 ORDER BY at, chain_id, seq""".formatted(tables.events());
        long count = 0;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try (PreparedStatement statement =
                    connection.prepareStatement(query))
            {
                statement.setQueryTimeout(timeout);
                statement.setFetchSize(FETCH_SIZE);
                statement.setObject(1, time(from == null ? Instant.EPOCH
                        : from));
                statement.setObject(2, time(to == null ? Instant.now() : to));
                try (ResultSet rows = statement.executeQuery())
                {
                    while (rows.next())
                    {
                        action.accept(new Exported(
                                rows.getObject(1, UUID.class),
                                rows.getLong(2), instant(rows, 3),
                                rows.getString(4), rows.getString(5),
                                AuditEntry.attributesFromJson(
                                        rows.getString(6)),
                                rows.getBytes(7)));
                        count++;
                    }
                }
            }
            finally
            {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        }
        return count;
    }

    /**
     * Deletes the chains last written before a time, with their entries
     * and seals: for the operator's retention of audit logs, which HIPAA
     * leaves to them (HIPAA.md §3.1). Whole chains only, so what is left
     * still verifies.
     *
     * @param before the time; chains written since are kept
     * @return what was removed
     * @throws SQLException if the journal can't be written
     */
    public Pruned prune(Instant before) throws SQLException
    {
        String find = """
                SELECT chain_id FROM %s WHERE updated_at < ?
                 ORDER BY updated_at LIMIT %d""".formatted(tables.chains(),
                PRUNE_BATCH);
        long chains = 0;
        long entries = 0;
        try (Connection connection = dataSource.getConnection())
        {
            while (true)
            {
                List<UUID> found = new ArrayList<>();
                try (PreparedStatement statement =
                        connection.prepareStatement(find))
                {
                    statement.setQueryTimeout(timeout);
                    statement.setObject(1, time(before));
                    try (ResultSet rows = statement.executeQuery())
                    {
                        while (rows.next())
                        {
                            found.add(rows.getObject(1, UUID.class));
                        }
                    }
                }
                if (found.isEmpty())
                {
                    return new Pruned(chains, entries);
                }
                connection.setAutoCommit(false);
                try
                {
                    for (UUID chain : found)
                    {
                        entries += delete(connection, tables.events(), chain);
                        delete(connection, tables.seals(), chain);
                        delete(connection, tables.chains(), chain);
                    }
                    connection.commit();
                    chains += found.size();
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
        }
    }

    /**
     * A chain's row.
     *
     * @param chainId   the chain
     * @param source    what it is of
     * @param pod       the pod that wrote it
     * @param startedAt when it started
     * @param lastSeq   the last entry written, as the row says
     * @param lastHash  that entry's hash, as the row says
     * @param updatedAt when it was last written
     * @param closedAt  when it closed; null if it didn't
     */
    private record ChainRow(UUID chainId, String source, String pod,
            Instant startedAt, long lastSeq, byte[] lastHash,
            Instant updatedAt, Instant closedAt)
    {
    }

    /**
     * The chains written in a span of time, oldest first.
     *
     * @param connection the connection
     * @param from       the start; null for the beginning
     * @param to         the end, exclusive; null for now
     * @return their rows
     * @throws SQLException if they can't be read
     */
    private List<ChainRow> chains(Connection connection, Instant from,
            Instant to) throws SQLException
    {
        String query = """
                SELECT chain_id, source, pod, started_at, last_seq, last_hash,
                       updated_at, closed_at
                  FROM %s
                 WHERE updated_at >= ? AND started_at < ?
                 ORDER BY started_at, chain_id""".formatted(tables.chains());
        List<ChainRow> found = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(query))
        {
            statement.setQueryTimeout(timeout);
            statement.setObject(1, time(from == null ? Instant.EPOCH : from));
            statement.setObject(2, time(to == null ? Instant.now() : to));
            try (ResultSet rows = statement.executeQuery())
            {
                while (rows.next())
                {
                    found.add(new ChainRow(rows.getObject(1, UUID.class),
                            rows.getString(2), rows.getString(3),
                            instant(rows, 4), rows.getLong(5),
                            rows.getBytes(6), instant(rows, 7),
                            instant(rows, 8)));
                }
            }
        }
        return found;
    }

    /**
     * Checks one chain from its start.
     *
     * @param connection the connection, in a transaction
     * @param chain      the chain's row
     * @param key        the stage key; null to leave seals unchecked
     * @param now        the time now
     * @param staleAfter how long an unclosed chain may go unwritten
     * @return what was found
     * @throws SQLException if it can't be read
     */
    private ChainReport verify(Connection connection, ChainRow chain,
            SealKey key, Instant now, Duration staleAfter)
            throws SQLException
    {
        Map<Long, String> seals = seals(connection, chain.chainId());
        long sealCount = seals.size();
        List<Problem> problems = new ArrayList<>();
        String query = """
                SELECT seq, at, name, severity, attributes, hash FROM %s
                 WHERE chain_id = ? ORDER BY seq""".formatted(tables.events());
        byte[] previous = AuditEntry.GENESIS;
        long expected = 1;
        long entries = 0;
        long lastSeq = 0;
        long sealedUpTo = key == null ? -1 : 0;
        try (PreparedStatement statement = connection.prepareStatement(query))
        {
            statement.setQueryTimeout(timeout);
            statement.setFetchSize(FETCH_SIZE);
            statement.setObject(1, chain.chainId());
            try (ResultSet rows = statement.executeQuery())
            {
                while (rows.next())
                {
                    long seq = rows.getLong(1);
                    if (seq != expected)
                    {
                        problems.add(new Problem(Kind.GAP, expected,
                                seq - expected));
                    }
                    AuditEntry entry = new AuditEntry(chain.chainId(), seq,
                            instant(rows, 2), rows.getString(3),
                            rows.getString(4),
                            AuditEntry.attributesFromJson(rows.getString(5)),
                            rows.getBytes(6));
                    if (!entry.follows(previous))
                    {
                        problems.add(new Problem(Kind.BROKEN, seq, 1));
                    }
                    String seal = seals.remove(seq);
                    if (seal != null && key != null)
                    {
                        switch (AuditSeal.verify(seal, key.kid(), key.key(),
                                chain.chainId(), seq, entry.hash()))
                        {
                            case VALID -> sealedUpTo = seq;
                            case MISMATCH -> problems.add(
                                    new Problem(Kind.SEAL_MISMATCH, seq, 1));
                            case INVALID -> problems.add(
                                    new Problem(Kind.SEAL_INVALID, seq, 1));
                            default -> throw new IllegalStateException();
                        }
                    }
                    previous = entry.hash();
                    expected = seq + 1;
                    lastSeq = seq;
                    entries++;
                }
            }
        }
        for (Long orphan : seals.keySet())
        {
            problems.add(new Problem(Kind.SEAL_ORPHAN, orphan, 1));
        }
        if (chain.lastSeq() > lastSeq)
        {
            problems.add(new Problem(Kind.HEAD_MISSING, lastSeq + 1,
                    chain.lastSeq() - lastSeq));
        }
        else if (entries > 0
                && !MessageDigest.isEqual(chain.lastHash(), previous))
        {
            problems.add(new Problem(Kind.HEAD_MISMATCH, lastSeq, 1));
        }
        if (sealedUpTo >= 0 && sealedUpTo < lastSeq)
        {
            problems.add(new Problem(Kind.UNSEALED_TAIL, sealedUpTo + 1,
                    lastSeq - sealedUpTo));
        }
        if (chain.closedAt() == null
                && chain.updatedAt().plus(staleAfter).isBefore(now))
        {
            problems.add(new Problem(Kind.UNCLOSED, lastSeq + 1, 0));
        }
        return new ChainReport(chain.chainId(), chain.source(), chain.pod(),
                chain.startedAt(), chain.closedAt(), entries, sealCount,
                sealedUpTo, List.copyOf(problems));
    }

    /**
     * A chain's seals.
     *
     * @param connection the connection
     * @param chainId    the chain
     * @return each seal, by the entry it seals
     * @throws SQLException if they can't be read
     */
    private Map<Long, String> seals(Connection connection, UUID chainId)
            throws SQLException
    {
        String query = "SELECT seq, seal FROM %s WHERE chain_id = ?"
                .formatted(tables.seals());
        Map<Long, String> found = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(query))
        {
            statement.setQueryTimeout(timeout);
            statement.setObject(1, chainId);
            try (ResultSet rows = statement.executeQuery())
            {
                while (rows.next())
                {
                    found.put(rows.getLong(1), rows.getString(2));
                }
            }
        }
        return found;
    }

    /**
     * Deletes a chain's rows from a table.
     *
     * @param connection the connection, in a transaction
     * @param table      the table
     * @param chainId    the chain
     * @return how many rows were deleted
     * @throws SQLException if they can't be
     */
    private int delete(Connection connection, String table, UUID chainId)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM " + table + " WHERE chain_id = ?"))
        {
            statement.setQueryTimeout(timeout);
            statement.setObject(1, chainId);
            return statement.executeUpdate();
        }
    }

    /**
     * Reads a time.
     *
     * @param rows   the result
     * @param column the column
     * @return the time; null if there is none
     * @throws SQLException if it can't be read
     */
    private static Instant instant(ResultSet rows, int column)
            throws SQLException
    {
        OffsetDateTime time = rows.getObject(column, OffsetDateTime.class);
        return time == null ? null : time.toInstant();
    }

    /**
     * A time as the database takes it.
     *
     * @param at the time
     * @return it, in UTC
     */
    private static OffsetDateTime time(Instant at)
    {
        return JdbcAuditJournal.time(at);
    }
}
