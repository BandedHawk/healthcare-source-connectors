package com.sdh.healthcare.connector.core.checkpoint;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * PostgreSQL lease store. One row per lease name. All expiry checks use the database clock
 * ({@code clock_timestamp()}), so instance clock skew cannot cause two holders.
 *
 * <p>Renewals update one row and write nothing to Iceberg. The epoch only ever increases: release
 * expires the row instead of deleting it.
 */
public final class JdbcLeaseStore implements LeaseStore {
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?");

    private final DataSource dataSource;
    private final String table;
    private final int queryTimeoutSeconds;

    public JdbcLeaseStore(DataSource dataSource, String table, Duration queryTimeout) {
        if (!IDENTIFIER.matcher(table).matches()) {
            throw new IllegalArgumentException("Invalid lease table name");
        }
        this.dataSource = dataSource;
        this.table = table;
        this.queryTimeoutSeconds = (int) Math.max(1, queryTimeout.toSeconds());
    }

    public void createTableIfMissing() {
        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                  name        text PRIMARY KEY,
                  owner       text        NOT NULL,
                  epoch       bigint      NOT NULL,
                  expires_at  timestamptz NOT NULL,
                  acquired_at timestamptz NOT NULL,
                  renewed_at  timestamptz NOT NULL
                )""".formatted(table);
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.setQueryTimeout(queryTimeoutSeconds);
            s.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create lease table", e);
        }
    }

    @Override
    public LeaseGrant acquire(String leaseName, String owner, Duration ttl) throws LeaseHeldException {
        // Insert a new lease, or take over an expired one with epoch + 1. A live lease is left untouched
        // and no row is returned. ON CONFLICT re-checks the WHERE clause under a row lock, so two
        // concurrent callers cannot both win.
        String sql = """
                INSERT INTO %1$s AS l (name, owner, epoch, expires_at, acquired_at, renewed_at)
                VALUES (?, ?, 1, clock_timestamp() + ? * interval '1 millisecond', clock_timestamp(), clock_timestamp())
                ON CONFLICT (name) DO UPDATE
                   SET owner = EXCLUDED.owner,
                       epoch = l.epoch + 1,
                       expires_at = EXCLUDED.expires_at,
                       acquired_at = EXCLUDED.acquired_at,
                       renewed_at = EXCLUDED.renewed_at
                 WHERE l.expires_at <= clock_timestamp()
                RETURNING epoch, expires_at""".formatted(table);
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setString(1, leaseName);
                ps.setString(2, owner);
                ps.setLong(3, ttl.toMillis());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return new LeaseGrant(leaseName, owner, rs.getLong(1), instant(rs, 2));
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT owner, expires_at FROM " + table + " WHERE name = ?")) {
                ps.setQueryTimeout(queryTimeoutSeconds);
                ps.setString(1, leaseName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new LeaseHeldException(rs.getString(1), instant(rs, 2));
                    }
                }
            }
            throw new LeaseHeldException("unknown", Instant.EPOCH);
        } catch (SQLException e) {
            throw new IllegalStateException("Lease store unavailable", e);
        }
    }

    @Override
    public boolean renew(LeaseGrant grant, Duration ttl) {
        String sql = """
                UPDATE %s SET expires_at = clock_timestamp() + ? * interval '1 millisecond', renewed_at = clock_timestamp()
                 WHERE name = ? AND owner = ? AND epoch = ?""".formatted(table);
        return update(sql, ttl.toMillis(), grant) == 1;
    }

    @Override
    public void release(LeaseGrant grant) {
        String sql = """
                UPDATE %s SET expires_at = clock_timestamp() - ? * interval '1 millisecond'
                 WHERE name = ? AND owner = ? AND epoch = ?""".formatted(table);
        update(sql, 1L, grant);
    }

    private int update(String sql, long millis, LeaseGrant grant) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(true);
            ps.setQueryTimeout(queryTimeoutSeconds);
            ps.setLong(1, millis);
            ps.setString(2, grant.leaseName());
            ps.setString(3, grant.owner());
            ps.setLong(4, grant.epoch());
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Lease store unavailable", e);
        }
    }

    private static Instant instant(ResultSet rs, int col) throws SQLException {
        return rs.getObject(col, OffsetDateTime.class).toInstant();
    }
}
