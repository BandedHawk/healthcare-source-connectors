/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import java.util.ArrayList;
import java.util.List;

/**
 * The database tables a stage keeps whose rows hold a data key of their
 * own, wrapped with its KEK (CONTROL-API-DESIGN.md §11): what its re-key
 * re-wraps and its {@code keys} query counts. They are the tables stages
 * share in the registry's schema, so each is read by the stage's column.
 *
 * <p>The control API's held results ({@code sdh_control_results}) are left
 * out: each lives an hour at most, and their table is shared by every
 * stage writing to the schema, whatever its KEK.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageKeys
{

    /** The column naming a row's stage. */
    private static final String STAGE = "stage";

    /** Not created: holds the tables. */
    private StageKeys()
    {
    }

    /**
     * The tables every stage keeps: failures and their events,
     * corrections, reprocess runs and release acceptances.
     *
     * @param schema the registry's schema
     * @return them
     */
    public static List<WrappedRows> rows(String schema)
    {
        return List.of(
                new WrappedRows(FailureStore.table(schema), List.of(STAGE,
                        "item_id", "seq", "generation", "line"),
                        "reason_key", "reason_kek", STAGE),
                new WrappedRows(FailureStore.eventsTable(schema),
                        List.of(STAGE, "item_id", "seq", "event"),
                        "reason_key", "reason_kek", STAGE),
                new WrappedRows(CorrectionStore.table(schema), List.of(STAGE,
                        "item_id", "seq", "correction"), "content_key",
                        "content_kek", STAGE),
                new WrappedRows(ReprocessStore.runs(schema),
                        List.of("run_id"), "reason_key", "reason_kek",
                        STAGE),
                new WrappedRows(schema + ".stage_release_accept",
                        List.of("accept_id"), "reason_key", "reason_kek",
                        STAGE));
    }

    /**
     * The split stage's tables: every stage's, and its held items'
     * releases.
     *
     * @param schema the registry's schema
     * @return them
     */
    public static List<WrappedRows> splitRows(String schema)
    {
        List<WrappedRows> rows = new ArrayList<>(rows(schema));
        rows.add(new WrappedRows(schema + ".split_release",
                List.of("item_id", "request"), "reason_key", "reason_kek",
                null));
        return List.copyOf(rows);
    }
}
