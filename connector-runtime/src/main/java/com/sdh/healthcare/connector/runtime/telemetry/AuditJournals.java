/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import com.sdh.healthcare.connector.core.audit.AuditSeal;
import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.audit.JdbcAuditJournal;
import com.sdh.healthcare.connector.core.telemetry.AuditJournal;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Opens a process's durable audit journal (HIPAA.md §3.1, audit controls)
 * and installs it, so every audit event from then on, and those emitted
 * while the process started, is kept. The journal writes over a pool of its
 * own on the lease database, of one connection.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AuditJournals
{

    /** How long {@code flush} waits at most, as before a fatal exit. */
    private static final Duration FLUSH_WAIT = Duration.ofSeconds(5);

    /**
     * How long a write waits for a connection: a write that fails is tried
     * again, so a short wait loses nothing, and lets a stop with the
     * database out of reach finish soon.
     */
    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(3);

    /**
     * How long closing waits at most for what is left to be written: when
     * the database is reachable that takes milliseconds; when it isn't,
     * waiting longer only holds up the stop, and what is left is logged.
     */
    private static final Duration CLOSE_WAIT = Duration.ofSeconds(10);

    /** Not instantiated: this class only has static methods. */
    private AuditJournals()
    {
    }

    /**
     * A started journal, or none, which closing uninstalls.
     */
    public interface Opened extends AutoCloseable
    {

        /**
         * The chain this process writes.
         *
         * @return its ID; empty if no journal is kept
         */
        Optional<UUID> chainId();

        /** Writes what is left, seals and closes the chain. */
        @Override
        void close();
    }

    /**
     * Starts a stage's journal and installs it.
     *
     * @param lease     the lease database's settings
     * @param telemetry the telemetry settings, with the journal's
     * @param tables    the journal's tables: in the stage's schema
     * @param source    what the chain is of, such as
     *                  {@code convert/<connector ID>}
     * @param keys      the stage's keys, whose message signer seals the
     *                  chain; null to leave it unsealed
     * @return the journal, to close last at shutdown
     * @throws IllegalStateException if it can't be started
     */
    public static Opened start(CommonSettings.Lease lease,
            CommonSettings.Telemetry telemetry, AuditTables tables,
            String source, KeyMaterial keys)
    {
        CommonSettings.Journal settings = telemetry.journal();
        if (!settings.enabled())
        {
            return none();
        }
        HikariDataSource pool = LeaseDatabase.dataSource(lease.jdbc(),
                "sdh-audit", 1);
        pool.setConnectionTimeout(CONNECTION_TIMEOUT.toMillis());
        try
        {
            JdbcAuditJournal journal = new JdbcAuditJournal(pool, tables,
                    source, pod(),
                    keys == null ? null
                            : new AuditSeal(keys.messages(), keys.signerKid()),
                    new JdbcAuditJournal.Settings(settings.capacity(),
                            settings.batchSize(), settings.sealInterval(),
                            FLUSH_WAIT, CLOSE_WAIT),
                    Clock.systemUTC()).start();
            Telemetry.journal(journal);
            return new Opened()
            {
                @Override
                public Optional<UUID> chainId()
                {
                    return Optional.of(journal.chainId());
                }

                @Override
                public void close()
                {
                    journal.close();
                    Telemetry.journal(AuditJournal.NONE);
                    pool.close();
                }
            };
        }
        catch (RuntimeException failure)
        {
            pool.close();
            throw failure;
        }
    }

    /**
     * Keeps no journal: installs {@link AuditJournal#NONE}, so the events
     * emitted while the process started aren't kept for one.
     *
     * @return one whose close does nothing
     */
    public static Opened none()
    {
        Telemetry.journal(AuditJournal.NONE);
        return new Opened()
        {
            @Override
            public Optional<UUID> chainId()
            {
                return Optional.empty();
            }

            @Override
            public void close()
            {
                // nothing to close
            }
        };
    }

    /**
     * The pod, or host, this process runs on.
     *
     * @return its name; {@code unknown} if it has none
     */
    private static String pod()
    {
        return Optional.ofNullable(System.getenv("HOSTNAME"))
                .filter(name -> !name.isBlank()).orElse("unknown");
    }
}
