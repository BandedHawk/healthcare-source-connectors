/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * What the support commands, {@code health}, {@code settings},
 * {@code compliance} and {@code support-bundle} (CONTROL-API-DESIGN.md §5,
 * HIPAA.md P5), read: the settings in force, as the process read them; the
 * stage's audit journal, for its database and control state; the stage's
 * own context, for its store and keys; and, in the control API, its recent
 * operations. None of it holds PHI. Opens nothing until a command reads.
 *
 * @param service    the stage's service name
 * @param build      the stage's build, as {@code StageBuild} gives it
 * @param settings   the settings in force, by canonical name, with their
 *                   values: secrets still in, for the commands to remove
 * @param audit      the stage's audit journal, closed with this
 * @param context    the stage's own context, closed with this
 * @param status     the stage's {@code status} answer; null if the stage
 *                   has none
 * @param operations the control API's recent operations, without their
 *                   parameters or reasons; null outside the control API
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SupportTarget(String service, String build,
        Map<String, String> settings, AuditTarget audit, Object context,
        Callable<Map<String, Object>> status,
        Callable<List<Map<String, Object>>> operations)
        implements AutoCloseable
{

    /**
     * Copies the settings.
     *
     * @param service    the service
     * @param build      the build
     * @param settings   the settings
     * @param audit      the journal
     * @param context    the context
     * @param status     the status; null for none
     * @param operations the operations; null for none
     */
    public SupportTarget
    {
        settings = Map.copyOf(settings);
        Objects.requireNonNull(audit, "audit");
    }

    /** Closes the journal's connections and the stage's context. */
    @Override
    public void close() throws Exception
    {
        try
        {
            audit.close();
        }
        finally
        {
            if (context instanceof AutoCloseable closeable)
            {
                closeable.close();
            }
        }
    }
}
