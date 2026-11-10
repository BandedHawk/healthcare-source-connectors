/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.failure.FailureReason;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The releases a stage has let in (STAGES.md §14.4a): each set of versions
 * its pods stamp, with when and how it was let in, the newest being the
 * stage's current one; and the releases an operator accepted while the
 * stage wasn't drained. Kept in the stage's own schema, beside its
 * registry or checkpoints. It holds no content.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageReleases
{

    /** A schema name: a plain identifier, as it is put in the SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /**
     * A pod's release: the versions it stamps, sorted, named by a digest of
     * them.
     *
     * @param versions the versions, sorted, each once
     * @param digest   the first 16 hex digits of the SHA-256 of the
     *                 versions, one per line
     */
    public record Release(List<String> versions, String digest)
    {

        /**
         * Copies the versions.
         */
        public Release
        {
            versions = List.copyOf(versions);
        }

        /**
         * The release of a set of versions.
         *
         * @param versions the versions, in any order
         * @return the release
         * @throws IllegalArgumentException if there are none
         */
        public static Release of(Collection<String> versions)
        {
            List<String> sorted = List.copyOf(new TreeSet<>(versions));
            if (sorted.isEmpty())
            {
                throw new IllegalArgumentException("A release has versions");
            }
            try
            {
                byte[] hash = MessageDigest.getInstance("SHA-256").digest(
                        String.join("\n", sorted)
                                .getBytes(StandardCharsets.UTF_8));
                return new Release(sorted,
                        HexFormat.of().formatHex(hash).substring(0, 16));
            }
            catch (NoSuchAlgorithmException missing)
            {
                throw new IllegalStateException(missing);
            }
        }
    }

    /**
     * A release let in.
     *
     * @param release the release
     * @param how     how it was let in, as {@link DrainGate.How} names it
     * @param at      when
     */
    public record LetIn(Release release, String how, Instant at)
    {
    }

    /**
     * An operator's acceptance of a release while the stage wasn't drained.
     *
     * @param acceptId the acceptance's ID; its reason is bound to it
     * @param release  the release
     * @param operator who accepted it
     * @param at       when
     */
    public record Accepted(UUID acceptId, Release release, String operator,
            Instant at)
    {
    }

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The stage whose releases these are. */
    private final String stage;

    /** The releases let in, with the schema. */
    private final String releases;

    /** The releases accepted, with the schema. */
    private final String accepts;

    /** How long a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /**
     * Creates the store.
     *
     * @param dataSource   connections to the database
     * @param schema       the schema its tables are in
     * @param stage        the stage whose releases these are
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public StageReleases(DataSource dataSource, String schema, String stage,
            Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid release schema name");
        }
        this.dataSource = dataSource;
        this.stage = stage;
        this.releases = schema + ".stage_release";
        this.accepts = schema + ".stage_release_accept";
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Creates the tables if they aren't there.
     *
     * @throws IllegalStateException if they can't be created
     */
    public void createTablesIfMissing()
    {
        String schema = releases.substring(0, releases.indexOf('.'));
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      seq       bigserial   PRIMARY KEY,
                      stage     text        NOT NULL,
                      digest    text        NOT NULL,
                      versions  text[]      NOT NULL,
                      let_in_by text        NOT NULL,
                      at        timestamptz NOT NULL
                    )""".formatted(releases));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      accept_id  uuid        PRIMARY KEY,
                      stage      text        NOT NULL,
                      digest     text        NOT NULL,
                      versions   text[]      NOT NULL,
                      operator   text        NOT NULL,
                      reason_enc bytea       NOT NULL,
                      reason_key bytea       NOT NULL,
                      reason_kek text        NOT NULL,
                      at         timestamptz NOT NULL
                    )""".formatted(accepts));
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the release tables", failure);
        }
    }

    /**
     * The releases let in, newest first.
     *
     * @param limit the most returned
     * @return them
     * @throws IllegalStateException if the database can't be reached
     */
    public List<LetIn> history(int limit)
    {
        String sql = "SELECT versions, digest, let_in_by, at FROM "
                + releases + " WHERE stage = ? ORDER BY seq DESC LIMIT ?";
        return query(sql, select ->
        {
            select.setString(1, stage);
            select.setInt(2, limit);
        }, rows -> new LetIn(release(rows, 1, 2), rows.getString(3),
                rows.getTimestamp(4).toInstant()));
    }

    /**
     * The stage's current release and the one current before it: the
     * newest let in, and the newest let in before it under another digest.
     *
     * @return the two, the first null if none was let in yet, the second
     *         if there was none before
     * @throws IllegalStateException if the database can't be reached
     */
    public LetIn[] currentAndPrevious()
    {
        String sql = """
                WITH latest AS (
                  SELECT seq, digest FROM %1$s WHERE stage = ?
                   ORDER BY seq DESC LIMIT 1)
                SELECT versions, r.digest, let_in_by, at FROM %1$s r, latest
                 WHERE r.stage = ? AND r.seq = latest.seq
                UNION ALL
                (SELECT versions, r.digest, let_in_by, at FROM %1$s r, latest
                  WHERE r.stage = ? AND r.digest <> latest.digest
                  ORDER BY r.seq DESC LIMIT 1)""".formatted(releases);
        List<LetIn> found = query(sql, select ->
        {
            select.setString(1, stage);
            select.setString(2, stage);
            select.setString(3, stage);
        }, rows -> new LetIn(release(rows, 1, 2), rows.getString(3),
                rows.getTimestamp(4).toInstant()));
        return new LetIn[] {found.isEmpty() ? null : found.get(0),
                found.size() < 2 ? null : found.get(1)};
    }

    /**
     * Records a release let in, as the stage's current one.
     *
     * @param release the release
     * @param how     how it was let in
     * @throws IllegalStateException if the database can't be reached
     */
    public void letIn(Release release, String how)
    {
        String sql = "INSERT INTO " + releases
                + " (stage, digest, versions, let_in_by, at)"
                + " VALUES (?, ?, ?, ?, clock_timestamp())";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setString(1, stage);
            insert.setString(2, release.digest());
            insert.setArray(3, connection.createArrayOf("text",
                    release.versions().toArray()));
            insert.setString(4, how);
            insert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * Records an operator's acceptance of a release, with the reason,
     * encrypted, bound to the acceptance (as a failure's reason is, its
     * acceptance ID taking the item's place).
     *
     * @param release  the release
     * @param operator who accepts it
     * @param reason   encrypts the reason, given the acceptance's ID
     * @return the acceptance
     * @throws IllegalStateException if the database can't be reached
     */
    public Accepted accept(Release release, String operator,
            Function<UUID, FailureReason> reason)
    {
        UUID acceptId = UUID.randomUUID();
        FailureReason sealed = reason.apply(acceptId);
        String sql = "INSERT INTO " + accepts
                + " (accept_id, stage, digest, versions, operator,"
                + " reason_enc, reason_key, reason_kek, at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, clock_timestamp())"
                + " RETURNING at";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setObject(1, acceptId);
            insert.setString(2, stage);
            insert.setString(3, release.digest());
            insert.setArray(4, connection.createArrayOf("text",
                    release.versions().toArray()));
            insert.setString(5, operator);
            insert.setBytes(6, sealed.sealed());
            insert.setBytes(7, sealed.key());
            insert.setString(8, sealed.keyRef());
            try (ResultSet rows = insert.executeQuery())
            {
                rows.next();
                return new Accepted(acceptId, release, operator,
                        rows.getTimestamp(1).toInstant());
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * The newest acceptance of a release.
     *
     * @param release the release
     * @return the acceptance; empty if none was made
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Accepted> accepted(Release release)
    {
        String sql = "SELECT accept_id, versions, digest, operator, at FROM "
                + accepts + " WHERE stage = ? AND digest = ?"
                + " ORDER BY at DESC LIMIT 1";
        return query(sql, select ->
        {
            select.setString(1, stage);
            select.setString(2, release.digest());
        }, rows -> new Accepted(rows.getObject(1, UUID.class),
                release(rows, 2, 3), rows.getString(4),
                rows.getTimestamp(5).toInstant())).stream().findFirst();
    }

    /** Sets a statement's parameters. */
    @FunctionalInterface
    private interface Binder
    {

        /**
         * Sets them.
         *
         * @param statement the statement
         * @throws SQLException if one can't be set
         */
        void bind(PreparedStatement statement) throws SQLException;
    }

    /**
     * Reads a row.
     *
     * @param <T> what it is read as
     */
    @FunctionalInterface
    private interface Reader<T>
    {

        /**
         * Reads the row the rows are on.
         *
         * @param rows the rows
         * @return what it holds
         * @throws SQLException if it can't be read
         */
        T read(ResultSet rows) throws SQLException;
    }

    /**
     * Runs a query.
     *
     * @param <T>    what each row is read as
     * @param sql    the query
     * @param binder sets its parameters
     * @param reader reads each row
     * @return the rows, read
     * @throws IllegalStateException if the database can't be reached
     */
    private <T> List<T> query(String sql, Binder binder, Reader<T> reader)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            binder.bind(select);
            List<T> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(reader.read(rows));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * Reads a release from a row.
     *
     * @param rows     the rows, on one
     * @param versions the column of its versions
     * @param digest   the column of its digest
     * @return the release
     * @throws SQLException if it can't be read
     */
    private static Release release(ResultSet rows, int versions, int digest)
            throws SQLException
    {
        Array array = rows.getArray(versions);
        List<String> list = new ArrayList<>();
        for (Object each : (Object[]) array.getArray())
        {
            list.add((String) each);
        }
        return new Release(list, rows.getString(digest));
    }
}
