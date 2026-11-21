/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.nimbusds.jose.jwk.Curve;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.ExportSealer;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The principals' sealing keys (CONTROL-API-DESIGN.md §7): the public keys
 * a PHI result may be sealed to, each registered in advance for one
 * principal by another, so a stolen token can't have PHI sealed to the
 * thief's own key. They are kept in {@code sdh_operator_keys} in the
 * pipeline's database, which all its stages share.
 *
 * <p>A key is EC P-256 or P-384, or RSA of at least 3072 bits, and
 * expires within a year. A fingerprint names one key, and so one
 * principal. A revoked key stays, for the audit.
 *
 * <p>It is the operator-key operations' context; closing it does nothing,
 * since its connections are the control API's own pool.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class OperatorKeys implements AutoCloseable
{

    /** The longest a key may be registered for. */
    public static final Duration MAX_LIFETIME = Duration.ofDays(366);

    /** The fewest bits an RSA key may have. */
    public static final int MIN_RSA_BITS = 3072;

    /** A fingerprint: the SHA-256 of the key, lower-case hex. */
    public static final Pattern FINGERPRINT =
            Pattern.compile("[0-9a-f]{64}");

    /** A schema name that can be put in SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** The longest a statement may take, in seconds. */
    private static final int TIMEOUT = 30;

    /** The columns a key is read with. */
    private static final String COLUMNS = "fingerprint, principal,"
            + " registered_by, registered_at, expires_at, revoked_by,"
            + " revoked_at";

    /** Connections that may write. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /**
     * A registered key, without the key itself.
     *
     * @param fingerprint  its fingerprint
     * @param principal    whose it is
     * @param registeredBy who registered it
     * @param registeredAt when
     * @param expiresAt    when it expires
     * @param revokedBy    who revoked it; null if not revoked
     * @param revokedAt    when; null if not revoked
     */
    public record Key(String fingerprint, String principal,
            String registeredBy, Instant registeredAt, Instant expiresAt,
            String revokedBy, Instant revokedAt)
    {
    }

    /**
     * Opens the keys on a database, creating the table if it isn't there.
     *
     * @param dataSource connections that may write
     * @param schema     the schema, as the audit journal's; null for none
     */
    public OperatorKeys(DataSource dataSource, String schema)
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        this.dataSource = dataSource;
        this.table = (schema == null ? "" : schema + ".")
                + "sdh_operator_keys";
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
                      fingerprint   text        NOT NULL PRIMARY KEY,
                      principal     text        NOT NULL,
                      public_key    text        NOT NULL,
                      registered_by text        NOT NULL,
                      registered_at timestamptz NOT NULL,
                      expires_at    timestamptz NOT NULL,
                      revoked_by    text,
                      revoked_at    timestamptz
                    )""".formatted(table));
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the operator keys' table", failure);
        }
    }

    /**
     * Checks a public key is one a result may be sealed to.
     *
     * @param pem the key, PEM
     * @return its sealer
     * @throws AdminArgs.UsageException if it isn't EC P-256 or P-384, or
     *         RSA of at least {@value #MIN_RSA_BITS} bits
     */
    public static ExportSealer check(byte[] pem)
    {
        ExportSealer sealer = new ExportSealer(pem);
        PublicKey key = sealer.recipient();
        boolean good = switch (key)
        {
            case ECPublicKey ec ->
            {
                Curve curve = Curve.forECParameterSpec(ec.getParams());
                yield Curve.P_256.equals(curve) || Curve.P_384.equals(curve);
            }
            case RSAPublicKey rsa ->
                    rsa.getModulus().bitLength() >= MIN_RSA_BITS;
            default -> false;
        };
        if (!good)
        {
            throw new AdminArgs.UsageException("A sealing key must be EC"
                    + " P-256 or P-384, or RSA of at least " + MIN_RSA_BITS
                    + " bits");
        }
        return sealer;
    }

    /**
     * Registers a key for a principal.
     *
     * @param principal    whose it is
     * @param pem          the key, PEM, as checked
     * @param expiresAt    when it expires: within {@link #MAX_LIFETIME}
     * @param registeredBy who registers it
     * @return it, as registered
     * @throws AdminArgs.UsageException if the key or its expiry isn't
     *         allowed
     * @throws IllegalStateException if the key is registered already
     */
    public Key register(String principal, byte[] pem, Instant expiresAt,
            String registeredBy)
    {
        ExportSealer sealer = check(pem);
        Instant now = Instant.now();
        if (!expiresAt.isAfter(now)
                || expiresAt.isAfter(now.plus(MAX_LIFETIME)))
        {
            throw new AdminArgs.UsageException("A key must expire in the"
                    + " future, within a year");
        }
        String fingerprint = sealer.fingerprint();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + table + " (fingerprint, principal,"
                                + " public_key, registered_by,"
                                + " registered_at, expires_at) VALUES"
                                + " (?, ?, ?, ?, now(), ?) ON CONFLICT DO"
                                + " NOTHING"))
        {
            insert.setQueryTimeout(TIMEOUT);
            insert.setString(1, fingerprint);
            insert.setString(2, principal);
            insert.setString(3, new String(pem, US_ASCII).strip());
            insert.setString(4, registeredBy);
            insert.setTimestamp(5, Timestamp.from(expiresAt));
            if (insert.executeUpdate() != 1)
            {
                throw new IllegalStateException("The key is registered"
                        + " already");
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not register the key",
                    failure);
        }
        return find(fingerprint).orElseThrow();
    }

    /**
     * A key, by its fingerprint.
     *
     * @param fingerprint its fingerprint
     * @return it; empty if none is registered
     */
    public Optional<Key> find(String fingerprint)
    {
        return select("WHERE fingerprint = ?", fingerprint).stream()
                .findFirst();
    }

    /**
     * The keys registered, newest first.
     *
     * @param principal whose; null for everyone's
     * @return them
     */
    public List<Key> list(String principal)
    {
        return principal == null
                ? select("ORDER BY registered_at DESC, fingerprint", null)
                : select("WHERE principal = ? ORDER BY registered_at DESC,"
                        + " fingerprint", principal);
    }

    /**
     * Revokes a key, unless it is revoked already.
     *
     * @param fingerprint its fingerprint
     * @param revokedBy   who revokes it
     * @return true if it was revoked now
     */
    public boolean revoke(String fingerprint, String revokedBy)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + table + " SET revoked_by = ?,"
                                + " revoked_at = now() WHERE fingerprint"
                                + " = ? AND revoked_at IS NULL"))
        {
            update.setQueryTimeout(TIMEOUT);
            update.setString(1, revokedBy);
            update.setString(2, fingerprint);
            return update.executeUpdate() == 1;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not revoke the key",
                    failure);
        }
    }

    /**
     * A key a principal may have a result sealed to now: its own, not
     * expired and not revoked.
     *
     * @param principal   the caller
     * @param fingerprint the key it names
     * @return the key, PEM; empty if it may not
     */
    public Optional<byte[]> usable(String principal, String fingerprint)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT public_key FROM " + table + " WHERE"
                                + " fingerprint = ? AND principal = ? AND"
                                + " revoked_at IS NULL AND expires_at >"
                                + " now()"))
        {
            select.setQueryTimeout(TIMEOUT);
            select.setString(1, fingerprint);
            select.setString(2, principal);
            try (ResultSet row = select.executeQuery())
            {
                return row.next()
                        ? Optional.of(row.getString(1).getBytes(US_ASCII))
                        : Optional.empty();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not read the operator"
                    + " keys", failure);
        }
    }

    /** Does nothing: the connections are the control API's pool. */
    @Override
    public void close()
    {
    }

    /**
     * Reads keys.
     *
     * @param where     the clause after the table
     * @param parameter its one parameter; null for none
     * @return the keys
     */
    private List<Key> select(String where, String parameter)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT " + COLUMNS + " FROM " + table + " "
                                + where))
        {
            select.setQueryTimeout(TIMEOUT);
            if (parameter != null)
            {
                select.setString(1, parameter);
            }
            List<Key> keys = new ArrayList<>();
            try (ResultSet row = select.executeQuery())
            {
                while (row.next())
                {
                    keys.add(new Key(row.getString(1), row.getString(2),
                            row.getString(3), instant(row.getTimestamp(4)),
                            instant(row.getTimestamp(5)), row.getString(6),
                            instant(row.getTimestamp(7))));
                }
            }
            return keys;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not read the operator"
                    + " keys", failure);
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
}
