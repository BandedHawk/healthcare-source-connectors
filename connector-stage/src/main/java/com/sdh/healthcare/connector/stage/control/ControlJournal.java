/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.telemetry.AuditJournals;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminTelemetry;
import com.sdh.healthcare.connector.stage.admin.AuditTarget;

/**
 * The control API's audit (CONTROL-API-DESIGN.md §10): export of its audit
 * events, as the stage exports its own, and one chain in the stage's
 * durable journal for the server's life, of {@code <stage source>/control},
 * sealed with the stage's key. The API doesn't start if the journal can't
 * be opened: unlike a CLI query, a request is never served unjournaled.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlJournal implements AutoCloseable
{

    /** The audit export. */
    private final AdminTelemetry telemetry;

    /** Where the stage's journal is. */
    private final AuditTarget target;

    /** The stage's keys, which seal the chain; null to leave it unsealed. */
    private final KeyMaterial keys;

    /** The chain. */
    private final AuditJournals.Opened journal;

    /**
     * Keeps what was opened.
     *
     * @param telemetry the audit export
     * @param target    where the stage's journal is
     * @param keys      the stage's keys, or null
     * @param journal   the chain
     */
    private ControlJournal(AdminTelemetry telemetry, AuditTarget target,
            KeyMaterial keys, AuditJournals.Opened journal)
    {
        this.telemetry = telemetry;
        this.target = target;
        this.keys = keys;
        this.journal = journal;
    }

    /**
     * The stage's KEK provider, which wraps a break-glass result's data
     * key (CONTROL-API-DESIGN.md §6).
     *
     * @return it; null if the stage has no key settings
     */
    KeyProvider wrap()
    {
        return keys == null ? null : keys.wrap();
    }

    /**
     * The KEK a break-glass result's data key is wrapped with: the stage's
     * records KEK, which its keys hold.
     *
     * @return its reference; null if the stage has no key settings
     */
    String resultKek()
    {
        return keys == null ? null : target.recordsKek().orElse(null);
    }

    /**
     * Opens the audit export and the chain.
     *
     * @param cli      the stage's operator commands
     * @param settings the stage's settings
     * @return the journal, to close when the server stops
     * @throws Exception if either can't be opened
     */
    static ControlJournal open(AdminCli<Object, ?> cli, Object settings)
            throws Exception
    {
        CommonSettings.Telemetry telemetrySettings =
                cli.stage().telemetry().apply(settings);
        AdminTelemetry telemetry = AdminTelemetry.install(telemetrySettings,
                cli.stage().service());
        AuditTarget target = null;
        KeyMaterial keys = null;
        try
        {
            target = cli.stage().audit().apply(settings);
            keys = target.keys();
            AuditJournals.Opened journal = AuditJournals.start(
                    target.lease(), telemetrySettings, target.tables(),
                    target.source() + "/control", keys);
            return new ControlJournal(telemetry, target, keys, journal);
        }
        catch (Exception | Error failure)
        {
            close(keys, target, telemetry);
            throw failure;
        }
    }

    /** Seals and closes the chain, then the keys and the export. */
    @Override
    public void close()
    {
        try
        {
            journal.close();
        }
        finally
        {
            close(keys, target, telemetry);
        }
    }

    /**
     * Closes what was opened, last first.
     *
     * @param keys      the keys, or null
     * @param target    the journal's location, or null
     * @param telemetry the export
     */
    private static void close(KeyMaterial keys, AuditTarget target,
            AdminTelemetry telemetry)
    {
        try
        {
            if (keys != null)
            {
                keys.close();
            }
        }
        finally
        {
            try
            {
                if (target != null)
                {
                    target.close();
                }
            }
            finally
            {
                telemetry.close();
            }
        }
    }
}
