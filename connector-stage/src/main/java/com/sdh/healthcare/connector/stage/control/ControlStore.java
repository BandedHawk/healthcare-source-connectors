/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The control API's own tables (CONTROL-API-DESIGN.md §5, §8.1), beside the
 * stage's audit journal in the pipeline's database:
 * <ul>
 *   <li>{@code sdh_control_requests}: each action's idempotency key, by
 *       principal, with the request's hash and its first answer, for
 *       {@value #KEY_HOURS} hours;</li>
 *   <li>{@code sdh_control_operations}: the long operations, their state,
 *       progress and result, and the lease of the control pod running one,
 *       so two replicas never both run it.</li>
 * </ul>
 * Neither holds PHI: parameters, progress and answers are identifiers and
 * counts. Times that decide anything are the database's.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlStore
{

    /** How long an idempotency key is kept. */
    static final int KEY_HOURS = 24;

    /** How often expired keys are deleted, at most. */
    static final Duration PURGE_EVERY = Duration.ofMinutes(10);

    /** A plain SQL identifier, for the schema. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]*");

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT = 30;

    /** Connections that may write. */
    private final DataSource dataSource;

    /** The idempotency keys' table, with its schema. */
    private final String requests;

    /** The operations' table, with its schema. */
    private final String operations;

    /** When expired keys were last deleted, by this process. */
    private volatile Instant purged = Instant.EPOCH;

    /** The states an operation may be in. */
    public enum State
    {

        /** Waiting for a second person. */
        AWAITING_APPROVAL,

        /** Approved, not yet started. */
        APPROVED,

        /** Being carried out. */
        RUNNING,

        /** Finished. */
        DONE,

        /** Finished, refused or failed. */
        FAILED,

        /** Cancelled before it finished. */
        CANCELLED,

        /** Refused by the second person. */
        REJECTED,

        /** Not approved in time. */
        EXPIRED;

        /**
         * Its name in the table and the API, such as
         * {@code awaiting-approval}.
         *
         * @return the name
         */
        public String wire()
        {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }

        /**
         * The state a name gives.
         *
         * @param wire the name
         * @return the state
         */
        static State of(String wire)
        {
            return valueOf(wire.toUpperCase(Locale.ROOT).replace('-', '_'));
        }

        /**
         * Whether an operation in this state is finished.
         *
         * @return true once nothing more happens to it
         */
        public boolean finished()
        {
            return this == DONE || this == FAILED || this == CANCELLED
                    || this == REJECTED || this == EXPIRED;
        }
    }

    /**
     * A long operation.
     *
     * @param id          its ID
     * @param kind        the operation, such as {@code reprocess}
     * @param scope       {@code stage:<name>}, or {@code pipeline}
     * @param parameters  what it acts on: identifiers and counts
     * @param requestedBy the principal that asked
     * @param reason      why, as checked
     * @param requestedAt when
     * @param state       where it stands
     * @param progress    how far it has got: counts
     * @param updatedAt   when it last changed
     * @param holder      the control pod running it; null for none
     * @param ticket      the change or incident it is for; null if none
     *                    was given
     * @param decidedBy   who approved or rejected it; null until then, and
     *                    for an operation that needs no approval
     * @param decision    why they did, as checked; null until then
     * @param decidedAt   when; null until then
     * @param result      what it did: counts; empty until it has finished
     */
    public record Operation(UUID id, String kind, String scope,
            Map<String, Object> parameters, String requestedBy, String reason,
            Instant requestedAt, State state, Map<String, Object> progress,
            Instant updatedAt, String holder, String ticket,
            String decidedBy, String decision, Instant decidedAt,
            Map<String, Object> result)
    {
    }

    /** What became of claiming an idempotency key. */
    public sealed interface Claim permits Fresh, Replay, Mismatch, Pending
    {
    }

    /** The key is new: the request is to be carried out. */
    public record Fresh() implements Claim
    {
    }

    /**
     * The key was used for the same request, which was answered.
     *
     * @param status      the answer's HTTP status
     * @param contentType its media type
     * @param body        its body
     * @param location    its {@code Location}; null for none
     */
    public record Replay(int status, String contentType, String body,
            String location) implements Claim
    {
    }

    /** The key was used for another request. */
    public record Mismatch() implements Claim
    {
    }

    /** The key's request is still being carried out. */
    public record Pending() implements Claim
    {
    }

    /**
     * Opens the store on a database, creating its tables if they aren't
     * there.
     *
     * @param dataSource connections that may write
     * @param schema     the schema, as the audit journal's; null for none
     */
    public ControlStore(DataSource dataSource, String schema)
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        String prefix = schema == null ? "" : schema + ".";
        this.dataSource = dataSource;
        this.requests = prefix + "sdh_control_requests";
        this.operations = prefix + "sdh_control_operations";
        createIfMissing(schema);
    }

    /**
     * Creates the tables if they aren't there.
     *
     * @param schema the schema; null for none
     */
    private void createIfMissing(String schema)
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(TIMEOUT);
            if (schema != null)
            {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            }
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      principal    text        NOT NULL,
                      idem_key     text        NOT NULL,
                      operation    text        NOT NULL,
                      request_hash bytea       NOT NULL,
                      created_at   timestamptz NOT NULL,
                      status       integer,
                      content_type text,
                      answer       text,
                      location     text,
                      PRIMARY KEY (principal, idem_key)
                    )""".formatted(requests));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      id              uuid        NOT NULL PRIMARY KEY,
                      kind            text        NOT NULL,
                      scope           text        NOT NULL,
                      parameters      text        NOT NULL,
                      requested_by    text        NOT NULL,
                      reason          text        NOT NULL,
                      ticket          text,
                      requested_at    timestamptz NOT NULL,
                      approved_by     text,
                      approval_reason text,
                      approved_at     timestamptz,
                      state           text        NOT NULL,
                      parts           text,
                      progress        text,
                      result          text,
                      holder          text,
                      holder_until    timestamptz,
                      updated_at      timestamptz NOT NULL
                    )""".formatted(operations));
            statement.execute("CREATE INDEX IF NOT EXISTS"
                    + " sdh_control_operations_scope ON " + operations
                    + " (scope, requested_at DESC, id DESC)");
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the control API's tables", failure);
        }
    }

    /**
     * Claims an idempotency key for a request. A key older than
     * {@value #KEY_HOURS} hours counts as unused.
     *
     * @param principal the caller
     * @param key       the key
     * @param operation the operation, for the record
     * @param hash      the request's hash: its method, path and body
     * @return whether to carry it out, or what to answer instead
     */
    public Claim claim(String principal, String key, String operation,
            byte[] hash)
    {
        purgeNowAndThen();
        try (Connection connection = dataSource.getConnection())
        {
            try (PreparedStatement expired = connection.prepareStatement(
                    "DELETE FROM " + requests + " WHERE principal = ? AND"
                            + " idem_key = ? AND created_at < now() - "
                            + interval()))
            {
                expired.setQueryTimeout(TIMEOUT);
                expired.setString(1, principal);
                expired.setString(2, key);
                expired.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + requests + " (principal, idem_key,"
                            + " operation, request_hash, created_at)"
                            + " VALUES (?, ?, ?, ?, now())"
                            + " ON CONFLICT DO NOTHING"))
            {
                insert.setQueryTimeout(TIMEOUT);
                insert.setString(1, principal);
                insert.setString(2, key);
                insert.setString(3, operation);
                insert.setBytes(4, hash);
                if (insert.executeUpdate() == 1)
                {
                    return new Fresh();
                }
            }
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT request_hash, status, content_type, answer,"
                            + " location FROM " + requests
                            + " WHERE principal = ? AND idem_key = ?"))
            {
                select.setQueryTimeout(TIMEOUT);
                select.setString(1, principal);
                select.setString(2, key);
                try (ResultSet row = select.executeQuery())
                {
                    if (!row.next())
                    {
                        // Deleted as it expired, between the two: use it.
                        return claim(principal, key, operation, hash);
                    }
                    if (!Arrays.equals(row.getBytes(1), hash))
                    {
                        return new Mismatch();
                    }
                    int status = row.getInt(2);
                    if (row.wasNull())
                    {
                        return new Pending();
                    }
                    return new Replay(status, row.getString(3),
                            row.getString(4), row.getString(5));
                }
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not claim the idempotency key", failure);
        }
    }

    /**
     * Keeps a request's first answer, for retries with its key.
     *
     * @param principal   the caller
     * @param key         the key
     * @param status      the answer's HTTP status
     * @param contentType its media type
     * @param body        its body, which holds no PHI
     * @param location    its {@code Location}; null for none
     */
    public void answered(String principal, String key, int status,
            String contentType, String body, String location)
    {
        update("UPDATE " + requests + " SET status = ?, content_type = ?,"
                + " answer = ?, location = ? WHERE principal = ? AND"
                + " idem_key = ?", statement ->
                {
                    statement.setInt(1, status);
                    statement.setString(2, contentType);
                    statement.setString(3, body);
                    statement.setString(4, location);
                    statement.setString(5, principal);
                    statement.setString(6, key);
                });
    }

    /**
     * Records a long operation.
     *
     * @param kind        the operation
     * @param scope       {@code stage:<name>} or {@code pipeline}
     * @param parameters  what it acts on
     * @param requestedBy who asked
     * @param reason      why
     * @param state       its first state
     * @param progress    how far it has got
     * @return it, as recorded
     */
    public Operation record(String kind, String scope,
            Map<String, Object> parameters, String requestedBy, String reason,
            State state, Map<String, Object> progress)
    {
        return record(kind, scope, parameters, requestedBy, reason, null,
                state, progress);
    }

    /**
     * Records a long operation, with the ticket it is for.
     *
     * @param kind        the operation
     * @param scope       {@code stage:<name>} or {@code pipeline}
     * @param parameters  what it acts on
     * @param requestedBy who asked
     * @param reason      why
     * @param ticket      the change or incident it is for; null for none
     * @param state       its first state
     * @param progress    how far it has got
     * @return it, as recorded
     */
    public Operation record(String kind, String scope,
            Map<String, Object> parameters, String requestedBy, String reason,
            String ticket, State state, Map<String, Object> progress)
    {
        UUID id = UUID.randomUUID();
        update("INSERT INTO " + operations + " (id, kind, scope, parameters,"
                + " requested_by, reason, ticket, requested_at, state,"
                + " progress, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?,"
                + " now(), ?, ?, now())", statement ->
                {
                    statement.setObject(1, id);
                    statement.setString(2, kind);
                    statement.setString(3, scope);
                    statement.setString(4, json(parameters));
                    statement.setString(5, requestedBy);
                    statement.setString(6, reason);
                    statement.setString(7, ticket);
                    statement.setString(8, state.wire());
                    statement.setString(9, json(progress));
                });
        return find(id, scope).orElseThrow();
    }

    /**
     * An operation, if it is of a scope.
     *
     * @param id    its ID
     * @param scope the scope
     * @return it; empty if there is none of that scope
     */
    public Optional<Operation> find(UUID id, String scope)
    {
        List<Operation> found = select("WHERE id = ? AND scope = ?",
                statement ->
                {
                    statement.setObject(1, id);
                    statement.setString(2, scope);
                });
        return found.stream().findFirst();
    }

    /**
     * A scope's operations, newest first, a page at a time.
     *
     * @param scope the scope
     * @param limit the most to list
     * @param after the last one of the page before; null for the first
     *              page
     * @return them
     */
    public List<Operation> list(String scope, int limit, Operation after)
    {
        if (after == null)
        {
            return select("WHERE scope = ? ORDER BY requested_at DESC,"
                    + " id DESC LIMIT ?", statement ->
                    {
                        statement.setString(1, scope);
                        statement.setInt(2, limit);
                    });
        }
        return select("WHERE scope = ? AND (requested_at, id) < (?, ?)"
                + " ORDER BY requested_at DESC, id DESC LIMIT ?",
                statement ->
                {
                    statement.setString(1, scope);
                    statement.setTimestamp(2,
                            Timestamp.from(after.requestedAt()));
                    statement.setObject(3, after.id());
                    statement.setInt(4, limit);
                });
    }

    /**
     * Records where an operation stands, unless it has finished already.
     *
     * @param id       its ID
     * @param state    its state now
     * @param progress how far it has got
     * @return true if it was changed
     */
    public boolean progress(UUID id, State state,
            Map<String, Object> progress)
    {
        return update("UPDATE " + operations + " SET state = ?, progress = ?,"
                + " updated_at = now() WHERE id = ? AND state NOT IN"
                + " ('done', 'failed', 'cancelled', 'rejected', 'expired')",
                statement ->
                {
                    statement.setString(1, state.wire());
                    statement.setString(2, json(progress));
                    statement.setObject(3, id);
                }) == 1;
    }

    /**
     * Decides an operation waiting for a second person: approves or
     * rejects it, unless it is no longer waiting.
     *
     * @param id       its ID
     * @param approved whether it is approved; false to reject it
     * @param by       who decides
     * @param reason   why, as checked
     * @return true if it was decided now
     */
    public boolean decide(UUID id, boolean approved, String by,
            String reason)
    {
        return update("UPDATE " + operations + " SET state = ?,"
                + " approved_by = ?, approval_reason = ?, approved_at ="
                + " now(), updated_at = now() WHERE id = ? AND state ="
                + " 'awaiting-approval'", statement ->
                {
                    statement.setString(1, (approved ? State.APPROVED
                            : State.REJECTED).wire());
                    statement.setString(2, by);
                    statement.setString(3, reason);
                    statement.setObject(4, id);
                }) == 1;
    }

    /**
     * Cancels an operation that hasn't started: one waiting for approval,
     * or approved and not yet taken by a control pod.
     *
     * @param id its ID
     * @return true if it was cancelled now
     */
    public boolean cancel(UUID id)
    {
        return update("UPDATE " + operations + " SET state = 'cancelled',"
                + " updated_at = now() WHERE id = ? AND (state ="
                + " 'awaiting-approval' OR state = 'approved' AND holder"
                + " IS NULL)", statement -> statement.setObject(1, id)) == 1;
    }

    /**
     * Expires an operation that waited too long for approval.
     *
     * @param id its ID
     * @return true if it was expired now
     */
    public boolean expire(UUID id)
    {
        return update("UPDATE " + operations + " SET state = 'expired',"
                + " updated_at = now() WHERE id = ? AND state ="
                + " 'awaiting-approval'",
                statement -> statement.setObject(1, id)) == 1;
    }

    /**
     * A scope's approved operations that a control pod is to run: those
     * approved and not yet taken, and those running whose holder's lease
     * has run out.
     *
     * @param scope the scope
     * @param limit the most to list
     * @return them, oldest first
     */
    public List<Operation> runnable(String scope, int limit)
    {
        return select("WHERE scope = ? AND approved_by IS NOT NULL AND"
                + " state IN ('approved', 'running') AND (holder IS NULL OR"
                + " holder_until < now()) ORDER BY requested_at, id LIMIT ?",
                statement ->
                {
                    statement.setString(1, scope);
                    statement.setInt(2, limit);
                });
    }

    /**
     * Finishes an operation this pod holds, with its result, and gives up
     * its lease.
     *
     * @param id       its ID
     * @param holder   the control pod
     * @param state    how it finished: done or failed
     * @param progress how far it got
     * @param result   what it did: counts, never PHI
     * @return true if it still held it; false if another took it over
     */
    public boolean finish(UUID id, String holder, State state,
            Map<String, Object> progress, Map<String, Object> result)
    {
        return update("UPDATE " + operations + " SET state = ?, progress = ?,"
                + " result = ?, holder = NULL, holder_until = NULL,"
                + " updated_at = now() WHERE id = ? AND holder = ? AND state"
                + " = 'running'", statement ->
                {
                    statement.setString(1, state.wire());
                    statement.setString(2, json(progress));
                    statement.setString(3, json(result));
                    statement.setObject(4, id);
                    statement.setString(5, holder);
                }) == 1;
    }

    /**
     * Takes an operation's lease, to run it: only if it is to be run,
     * and no other control pod holds a lease on it that hasn't run out.
     * The holder may take it again, which renews it.
     *
     * @param id     its ID
     * @param holder the control pod
     * @param lease  how long the lease lasts, unless renewed
     * @return true if this pod now holds it
     */
    public boolean take(UUID id, String holder, Duration lease)
    {
        return update("UPDATE " + operations + " SET holder = ?,"
                + " holder_until = now() + ? * interval '1 millisecond',"
                + " updated_at = now() WHERE id = ? AND state IN"
                + " ('approved', 'running') AND (holder IS NULL OR"
                + " holder = ? OR holder_until < now())", statement ->
                {
                    statement.setString(1, holder);
                    statement.setLong(2, lease.toMillis());
                    statement.setObject(3, id);
                    statement.setString(4, holder);
                }) == 1;
    }

    /**
     * Renews a lease this pod holds.
     *
     * @param id     the operation's ID
     * @param holder the control pod
     * @param lease  how long the lease lasts from now
     * @return true if it still held it; false if another took it over
     */
    public boolean renew(UUID id, String holder, Duration lease)
    {
        return update("UPDATE " + operations + " SET holder_until = now() +"
                + " ? * interval '1 millisecond' WHERE id = ? AND holder = ?"
                + " AND holder_until >= now()", statement ->
                {
                    statement.setLong(1, lease.toMillis());
                    statement.setObject(2, id);
                    statement.setString(3, holder);
                }) == 1;
    }

    /**
     * Gives up a lease this pod holds, as it stops.
     *
     * @param id     the operation's ID
     * @param holder the control pod
     */
    public void release(UUID id, String holder)
    {
        update("UPDATE " + operations + " SET holder = NULL, holder_until ="
                + " NULL WHERE id = ? AND holder = ?", statement ->
                {
                    statement.setObject(1, id);
                    statement.setString(2, holder);
                });
    }

    /**
     * Deletes idempotency keys past their time, at most every
     * {@link #PURGE_EVERY}.
     */
    private void purgeNowAndThen()
    {
        Instant now = Instant.now();
        if (now.isBefore(purged.plus(PURGE_EVERY)))
        {
            return;
        }
        purged = now;
        update("DELETE FROM " + requests + " WHERE created_at < now() - "
                + interval(), statement ->
                {
                });
    }

    /**
     * How long a key is kept, as SQL.
     *
     * @return the interval
     */
    private static String interval()
    {
        return "interval '" + KEY_HOURS + " hours'";
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
     * Runs a statement that changes rows.
     *
     * @param sql    the statement
     * @param binder sets its parameters
     * @return how many rows it changed
     */
    private int update(String sql, Binder binder)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql))
        {
            statement.setQueryTimeout(TIMEOUT);
            binder.bind(statement);
            return statement.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not write the control API's tables", failure);
        }
    }

    /**
     * Reads operations.
     *
     * @param where  the statement's {@code WHERE} and what follows
     * @param binder sets its parameters
     * @return them
     */
    private List<Operation> select(String where, Binder binder)
    {
        String sql = "SELECT id, kind, scope, parameters, requested_by,"
                + " reason, requested_at, state, progress, updated_at, holder,"
                + " ticket, approved_by, approval_reason, approved_at, result"
                + " FROM " + operations + " " + where;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql))
        {
            statement.setQueryTimeout(TIMEOUT);
            binder.bind(statement);
            List<Operation> found = new ArrayList<>();
            try (ResultSet row = statement.executeQuery())
            {
                while (row.next())
                {
                    found.add(new Operation(row.getObject(1, UUID.class),
                            row.getString(2), row.getString(3),
                            parse(row.getString(4)), row.getString(5),
                            row.getString(6),
                            row.getTimestamp(7).toInstant(),
                            State.of(row.getString(8)),
                            parse(row.getString(9)),
                            row.getTimestamp(10).toInstant(),
                            row.getString(11), row.getString(12),
                            row.getString(13), row.getString(14),
                            instant(row.getTimestamp(15)),
                            parse(row.getString(16))));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not read the control API's tables", failure);
        }
    }

    /**
     * A time, as read.
     *
     * @param time the column's value
     * @return the instant; null for none
     */
    private static Instant instant(Timestamp time)
    {
        return time == null ? null : time.toInstant();
    }

    /**
     * Fields as JSON.
     *
     * @param fields the fields; null for none
     * @return the JSON
     */
    private static String json(Map<String, Object> fields)
    {
        return JSONObjectUtils.toJSONString(fields == null ? Map.of()
                : fields);
    }

    /**
     * JSON as fields.
     *
     * @param json the JSON; null for none
     * @return the fields
     */
    private static Map<String, Object> parse(String json)
    {
        if (json == null)
        {
            return Map.of();
        }
        try
        {
            return JSONObjectUtils.parse(json);
        }
        catch (ParseException unreadable)
        {
            throw new IllegalStateException("An operation's fields can't be"
                    + " read", unreadable);
        }
    }
}
