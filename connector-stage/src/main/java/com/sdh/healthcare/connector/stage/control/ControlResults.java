/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;

/**
 * The plaintext results of approved break-glass operations
 * (CONTROL-API-DESIGN.md §6), held for their requester to fetch once,
 * within the break-glass TTL, and then deleted. They are never in the
 * clear at rest: each result has a data key of its own, wrapped with the
 * stage's KEK, and each part is AES-256-GCM, bound to its result and its
 * place in it.
 *
 * <p>Two tables, beside the audit journal's in the pipeline's database:
 * {@code sdh_control_results}, a row per result with its wrapped key,
 * and {@code sdh_control_result_parts}, its parts in order. A result
 * fetched, or past its time, is deleted, as is one whose run never
 * finished.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlResults
{

    /** How long a result whose run never finished is kept. */
    static final Duration ABANDONED = Duration.ofDays(1);

    /** A schema name that can be put in SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]*");

    /** How long a statement may take, in seconds. */
    private static final int TIMEOUT = 30;

    /** A data key's length, in bytes. */
    private static final int KEY_BYTES = 32;

    /** A part's nonce's length, in bytes. */
    private static final int NONCE_BYTES = 12;

    /** A part's tag's length, in bits. */
    private static final int TAG_BITS = 128;

    /** Makes data keys and nonces. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Connections that may write. */
    private final DataSource dataSource;

    /** The results' table, with its schema. */
    private final String results;

    /** The parts' table, with its schema. */
    private final String parts;

    /** Wraps and unwraps the data keys; null if the stage has no keys. */
    private final KeyProvider wrap;

    /** The KEK the data keys are wrapped with; null with no keys. */
    private final String kek;

    /**
     * A result being written, by the run of its operation.
     */
    public final class Holding
    {

        /** The operation. */
        private final UUID id;

        /** The result's data key, cleared once it is finished. */
        private final byte[] key;

        /** Parts written. */
        private int count;

        /** Plaintext bytes written. */
        private long bytes;

        /**
         * Starts a result.
         *
         * @param id  the operation
         * @param key its data key
         */
        private Holding(UUID id, byte[] key)
        {
            this.id = id;
            this.key = key;
        }

        /**
         * Writes a part, encrypted.
         *
         * @param plaintext its bytes; cleared once written
         */
        public void part(byte[] plaintext)
        {
            byte[] sealed = seal(key, id, count, plaintext);
            Arrays.fill(plaintext, (byte) 0);
            int index = count;
            update("INSERT INTO " + parts + " (operation_id, part, body)"
                    + " VALUES (?, ?, ?)", statement ->
                    {
                        statement.setObject(1, id);
                        statement.setInt(2, index);
                        statement.setBytes(3, sealed);
                    });
            count++;
            bytes += plaintext.length;
        }

        /**
         * Finishes the result, to be fetched within a time.
         *
         * @param ttl how long it may be fetched for
         * @return when it may no longer be
         */
        public Instant finish(Duration ttl)
        {
            Arrays.fill(key, (byte) 0);
            update("UPDATE " + results + " SET parts = ?, bytes = ?,"
                    + " expires_at = now() + ? * interval '1 millisecond'"
                    + " WHERE operation_id = ?", statement ->
                    {
                        statement.setInt(1, count);
                        statement.setLong(2, bytes);
                        statement.setLong(3, ttl.toMillis());
                        statement.setObject(4, id);
                    });
            return expiry(id).orElseThrow();
        }

        /** Deletes what was written of it, as its run failed. */
        public void discard()
        {
            Arrays.fill(key, (byte) 0);
            delete(id);
        }

        /**
         * How many parts were written.
         *
         * @return the count
         */
        public int parts()
        {
            return count;
        }

        /**
         * How many plaintext bytes were written.
         *
         * @return the count
         */
        public long bytes()
        {
            return bytes;
        }
    }

    /**
     * A result claimed for its one fetch.
     *
     * @param id      the operation
     * @param wrapped its data key, wrapped
     * @param parts   how many parts it has
     * @param bytes   how many plaintext bytes
     */
    public record Claimed(UUID id, WrappedKey wrapped, int parts, long bytes)
    {
    }

    /**
     * Opens the results on a database, creating the tables if they aren't
     * there.
     *
     * @param dataSource connections that may write
     * @param schema     the schema, as the audit journal's; null for none
     * @param wrap       wraps the data keys; null if the stage has no
     *                   keys, when no result can be held
     * @param kek        the KEK to wrap them with; null with no keys
     */
    public ControlResults(DataSource dataSource, String schema,
            KeyProvider wrap, String kek)
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        String prefix = schema == null ? "" : schema + ".";
        this.dataSource = dataSource;
        this.results = prefix + "sdh_control_results";
        this.parts = prefix + "sdh_control_result_parts";
        this.wrap = wrap;
        this.kek = kek;
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
                      operation_id uuid        NOT NULL PRIMARY KEY,
                      kek_ref      text        NOT NULL,
                      wrapped_key  bytea       NOT NULL,
                      parts        integer,
                      bytes        bigint,
                      created_at   timestamptz NOT NULL,
                      expires_at   timestamptz,
                      fetched_at   timestamptz
                    )""".formatted(results));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      operation_id uuid    NOT NULL REFERENCES %s
                                           ON DELETE CASCADE,
                      part         integer NOT NULL,
                      body         bytea   NOT NULL,
                      PRIMARY KEY (operation_id, part)
                    )""".formatted(parts, results));
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the control API's result tables",
                    failure);
        }
    }

    /**
     * Starts holding an operation's result, replacing what a run of it
     * that didn't finish left.
     *
     * @param id the operation
     * @return the result, to write
     * @throws IllegalStateException if the stage has no keys to hold it
     *                               with
     */
    public Holding hold(UUID id)
    {
        if (wrap == null || kek == null)
        {
            throw new IllegalStateException("The stage has no KEK to hold"
                    + " a plaintext result with");
        }
        delete(id);
        byte[] key = new byte[KEY_BYTES];
        RANDOM.nextBytes(key);
        WrappedKey wrapped = wrap.wrap(kek, key);
        update("INSERT INTO " + results + " (operation_id, kek_ref,"
                + " wrapped_key, created_at) VALUES (?, ?, ?, now())",
                statement ->
                {
                    statement.setObject(1, id);
                    statement.setString(2, wrapped.keyRef());
                    statement.setBytes(3, wrapped.wrapped());
                });
        return new Holding(id, key);
    }

    /**
     * Claims a finished result for its one fetch: it can't be claimed
     * again, whether or not it is then sent whole.
     *
     * @param id the operation
     * @return the result; empty if there is none, or it was fetched or is
     *         past its time
     */
    public Optional<Claimed> claim(UUID id)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement claim = connection.prepareStatement(
                        "UPDATE " + results + " SET fetched_at = now()"
                                + " WHERE operation_id = ? AND fetched_at IS"
                                + " NULL AND parts IS NOT NULL AND"
                                + " expires_at > now() RETURNING kek_ref,"
                                + " wrapped_key, parts, bytes"))
        {
            claim.setQueryTimeout(TIMEOUT);
            claim.setObject(1, id);
            try (ResultSet row = claim.executeQuery())
            {
                return row.next() ? Optional.of(new Claimed(id,
                        new WrappedKey(row.getString(1), row.getBytes(2)),
                        row.getInt(3), row.getLong(4))) : Optional.empty();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not claim the result",
                    failure);
        }
    }

    /**
     * Writes a claimed result, decrypted a part at a time, then deletes
     * it, whether or not it was written whole.
     *
     * @param claimed the result
     * @param out     where it goes
     * @return the bytes written
     * @throws IOException if it can't be written
     */
    public long write(Claimed claimed, OutputStream out) throws IOException
    {
        if (wrap == null)
        {
            throw new IllegalStateException("The stage has no KEK to open"
                    + " a plaintext result with");
        }
        byte[] key = wrap.unwrap(claimed.wrapped());
        long written = 0;
        try
        {
            for (int part = 0; part < claimed.parts(); part++)
            {
                byte[] plaintext = open(key, claimed.id(), part,
                        body(claimed.id(), part));
                try
                {
                    out.write(plaintext);
                    written += plaintext.length;
                }
                finally
                {
                    Arrays.fill(plaintext, (byte) 0);
                }
            }
            return written;
        }
        finally
        {
            Arrays.fill(key, (byte) 0);
            delete(claimed.id());
        }
    }

    /**
     * When a finished result may no longer be fetched.
     *
     * @param id the operation
     * @return the time; empty if there is no finished result
     */
    public Optional<Instant> expiry(UUID id)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT expires_at FROM " + results + " WHERE"
                                + " operation_id = ? AND expires_at IS NOT"
                                + " NULL"))
        {
            select.setQueryTimeout(TIMEOUT);
            select.setObject(1, id);
            try (ResultSet row = select.executeQuery())
            {
                return row.next() ? Optional.of(row.getTimestamp(1)
                        .toInstant()) : Optional.empty();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not read the results",
                    failure);
        }
    }

    /**
     * Deletes the results fetched, past their time, or whose run never
     * finished.
     *
     * @return how many were deleted
     */
    public int sweep()
    {
        return update("DELETE FROM " + results + " WHERE fetched_at IS NOT"
                + " NULL OR expires_at < now() OR expires_at IS NULL AND"
                + " created_at < now() - ? * interval '1 millisecond'",
                statement -> statement.setLong(1, ABANDONED.toMillis()));
    }

    /**
     * Deletes a result and its parts.
     *
     * @param id the operation
     */
    private void delete(UUID id)
    {
        update("DELETE FROM " + results + " WHERE operation_id = ?",
                statement -> statement.setObject(1, id));
    }

    /**
     * Reads a part, as stored.
     *
     * @param id   the operation
     * @param part the part
     * @return nonce, ciphertext and tag
     */
    private byte[] body(UUID id, int part)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT body FROM " + parts + " WHERE operation_id ="
                                + " ? AND part = ?"))
        {
            select.setQueryTimeout(TIMEOUT);
            select.setObject(1, id);
            select.setInt(2, part);
            try (ResultSet row = select.executeQuery())
            {
                if (!row.next())
                {
                    throw new IllegalStateException("A part of the result"
                            + " is missing");
                }
                return row.getBytes(1);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not read the result",
                    failure);
        }
    }

    /**
     * Encrypts a part.
     *
     * @param key       the result's data key
     * @param id        the operation
     * @param part      the part's place
     * @param plaintext its bytes
     * @return nonce, ciphertext and tag
     */
    private static byte[] seal(byte[] key, UUID id, int part,
            byte[] plaintext)
    {
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nonce);
        try
        {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(aad(id, part));
            byte[] out = new byte[NONCE_BYTES
                    + cipher.getOutputSize(plaintext.length)];
            System.arraycopy(nonce, 0, out, 0, NONCE_BYTES);
            cipher.doFinal(plaintext, 0, plaintext.length, out, NONCE_BYTES);
            return out;
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("A part couldn't be encrypted",
                    failure);
        }
    }

    /**
     * Decrypts a part.
     *
     * @param key    the result's data key
     * @param id     the operation
     * @param part   the part's place
     * @param stored nonce, ciphertext and tag
     * @return its bytes
     * @throws IllegalStateException if it was altered or moved
     */
    private static byte[] open(byte[] key, UUID id, int part, byte[] stored)
    {
        try
        {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, stored, 0, NONCE_BYTES));
            cipher.updateAAD(aad(id, part));
            return cipher.doFinal(stored, NONCE_BYTES,
                    stored.length - NONCE_BYTES);
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("A part couldn't be opened",
                    failure);
        }
    }

    /**
     * What a part is bound to: its result and its place.
     *
     * @param id   the operation
     * @param part the part's place
     * @return the additional authenticated data
     */
    private static byte[] aad(UUID id, int part)
    {
        return ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits()).putInt(part).array();
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
                    "Could not write the control API's results", failure);
        }
    }
}
