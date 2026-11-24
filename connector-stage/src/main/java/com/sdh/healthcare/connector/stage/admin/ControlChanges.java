/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.stage.ControlLog;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps a control change in the store's control log
 * ({@link ControlLog}, DESIGN.md §18.6), once the database has it, so a
 * restore of the database to an earlier point can make it again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ControlChanges
{

    /** Not created: holds the step. */
    private ControlChanges()
    {
    }

    /**
     * Keeps a change, if the stage keeps tables in a store and has one
     * yet. A stage with no table yet has nothing a restore could bring
     * back, so nothing is kept.
     *
     * @param context the command's context
     * @param kind    what kind of change
     * @param id      its operation's ID
     * @param by      who made it
     * @param fields  its own fields
     * @throws Exception if the stage's tables can't be read, or the change
     *                   can't be written: the command then fails, though
     *                   the database has it, and asking again keeps it
     */
    static void keep(Object context, ControlLog.Kind kind, String id,
            String by, Map<String, String> fields) throws Exception
    {
        if (!(context instanceof DataAdmin data))
        {
            return;
        }
        Optional<ControlLog> log = data.controlLog();
        if (log.isPresent())
        {
            log.get().keep(kind, id, by, fields);
        }
    }
}
