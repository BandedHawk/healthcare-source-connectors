/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.audit.AuditTrail;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.ControlState;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

/**
 * A stage's audit journal (HIPAA.md §3.1, audit controls), as its operator
 * commands find it: where its tables are, what its chains are of, and the
 * stage key that seals them. Each command's run is journaled there too, and
 * the {@code audit-*} commands read it. Opens nothing until asked.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AuditTarget implements AutoCloseable
{

    /** How long a statement may run when reading the journal. */
    private static final Duration TIMEOUT = Duration.ofMinutes(10);

    /** The lease database's settings. */
    private final CommonSettings.Lease lease;

    /** The key settings, for the stage key that seals the chains. */
    private final CommonSettings.Crypto crypto;

    /** The journal's tables. */
    private final AuditTables tables;

    /** What the stage's chains are of, such as {@code convert/<id>}. */
    private final String source;

    /** The lease database, read-only, once opened. */
    private final AdminDatabase reading;

    /** The lease database, writing, once opened: for pruning. */
    private final AdminDatabase writing;

    /**
     * Describes a stage's journal.
     *
     * @param lease  the lease database's settings
     * @param crypto the key settings; null for none, when seals can't be
     *               checked
     * @param tables the journal's tables
     * @param source what the stage's chains are of
     */
    public AuditTarget(CommonSettings.Lease lease,
            CommonSettings.Crypto crypto, AuditTables tables, String source)
    {
        this.lease = lease;
        this.crypto = crypto;
        this.tables = tables;
        this.source = source;
        this.reading = new AdminDatabase(lease);
        this.writing = new AdminDatabase(lease, true);
    }

    /**
     * The lease database's settings.
     *
     * @return them
     */
    public CommonSettings.Lease lease()
    {
        return lease;
    }

    /**
     * The journal's tables.
     *
     * @return them
     */
    public AuditTables tables()
    {
        return tables;
    }

    /**
     * What the stage's chains are of.
     *
     * @return it, such as {@code convert/<connector ID>}
     */
    public String source()
    {
        return source;
    }

    /**
     * The journal, to read.
     *
     * @return its reader, on a read-only connection
     */
    public AuditTrail trail()
    {
        return new AuditTrail(reading.dataSource(), tables, TIMEOUT);
    }

    /**
     * The journal, to prune.
     *
     * @return its reader, on a connection that may write
     */
    public AuditTrail trailToChange()
    {
        return new AuditTrail(writing.dataSource(), tables, TIMEOUT);
    }

    /**
     * Reaches the lease database, where the journal is: one round trip,
     * reading nothing (CONTROL-API-DESIGN.md §5, {@code health}).
     *
     * @throws SQLException if it can't be reached
     */
    public void ping() throws SQLException
    {
        try (Connection connection = reading.dataSource().getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(10);
            statement.execute("SELECT 1");
        }
    }

    /**
     * Whether the stage is paused (CONTROL-API-DESIGN.md §9), for its
     * scope: what its chains are of.
     *
     * @param change whether it is to be paused or resumed, on a connection
     *               that may write
     * @return the state
     */
    public ControlState controlState(boolean change)
    {
        return new ControlState((change ? writing : reading).dataSource(),
                tables.schema(), source);
    }

    /**
     * The stage key that seals the chains, read without the KEKs or the
     * private key.
     *
     * @return it; empty if a local key's public half isn't configured, or
     *         no key settings were given
     * @throws Exception if it can't be read
     */
    public Optional<AuditTrail.SealKey> sealKey() throws Exception
    {
        if (crypto == null)
        {
            return Optional.empty();
        }
        return KeyMaterial.signingPublicKey(crypto,
                KeyMaterial.Providers.DEFAULT)
                .map(key -> new AuditTrail.SealKey(
                        crypto.keys().signing().kid(), key));
    }

    /**
     * The KEK the control API wraps a break-glass result's data key with
     * (CONTROL-API-DESIGN.md §6): the stage's records KEK, or, on a
     * landing node, which has none, its {@code landed_items} KEK.
     *
     * @return its reference; empty if no key settings were given
     */
    public Optional<String> recordsKek()
    {
        if (crypto == null)
        {
            return Optional.empty();
        }
        return Optional.ofNullable(landing() ? crypto.keys().landedItemsKek()
                : crypto.keys().recordsKek());
    }

    /**
     * Whether the key settings are a landing node's: a {@code landed_items}
     * KEK and no records KEK (design §5.3).
     *
     * @return true for a landing node
     */
    private boolean landing()
    {
        return crypto.keys().recordsKek() == null
                && crypto.keys().landedItemsKek() != null;
    }

    /**
     * The stage's keys, whose message signer seals a chain, for the
     * control API's journal (CONTROL-API-DESIGN.md §10), which runs with
     * the stage's own identity.
     *
     * @return the keys, to close after the journal; null if no key
     *         settings were given, when the chain is left unsealed
     * @throws Exception if they can't be loaded
     */
    public KeyMaterial keys() throws Exception
    {
        if (crypto == null)
        {
            return null;
        }
        return landing()
                ? KeyMaterial.forLanding(crypto, KeyMaterial.Providers.DEFAULT)
                : KeyMaterial.forStage(crypto, KeyMaterial.Providers.DEFAULT);
    }

    @Override
    public void close()
    {
        reading.close();
        writing.close();
    }
}
