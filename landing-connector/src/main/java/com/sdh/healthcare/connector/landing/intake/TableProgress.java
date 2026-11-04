/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.text.ParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;

/**
 * How far an Iceberg table intake has got (design §5.3.18): the snapshot
 * it has landed everything up to, kept in the table property
 * {@code sdh.landing.table.<intake>} of the ledger, MAC'd like every
 * checkpoint, and committed through the lease's fence. It holds snapshot
 * IDs only, no PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class TableProgress
{

    /** What the property's name starts with. */
    public static final String PROPERTY_PREFIX = "sdh.landing.table.";

    /**
     * The mark of a table that had no snapshot when the intake first
     * started: before its first one. Iceberg's snapshot IDs are never
     * negative.
     */
    public static final long NONE = -1L;

    /** The MAC's label. */
    private static final String LABEL = "sdh-landing-table";

    /**
     * A point in a table's history.
     *
     * @param snapshotId the snapshot
     * @param backfill   true while the rows the snapshot holds are being
     *                   landed (the first start, with {@code earliest});
     *                   false once everything up to and including it is
     */
    public record Mark(long snapshotId, boolean backfill)
    {
    }

    /** The ledger table the property is on; null for a test's subclass. */
    private final Table ledger;

    /** Commits it. */
    private final FencedCommitter committer;

    /** Writes and reads it with its MAC. */
    private final MacedJson<Mark> codec;

    /** The property's name. */
    private final String key;

    /**
     * Keeps an intake's progress on the ledger table.
     *
     * @param ledger    the ledger table
     * @param committer commits it, under the lease
     * @param ids       makes the MAC
     * @param intake    the intake
     */
    public TableProgress(Table ledger, FencedCommitter committer,
            SectionIds ids, String intake)
    {
        this.ledger = ledger;
        this.committer = committer;
        this.codec = ids == null ? null
                : new MacedJson<>(ids, LABEL, TableProgress::canonical,
                        TableProgress::parse);
        this.key = PROPERTY_PREFIX + intake;
    }

    /**
     * The progress last saved.
     *
     * @return the mark; empty before the intake's first start
     * @throws MacedJson.Tampered if it has been changed
     */
    public Optional<Mark> load()
    {
        ledger.refresh();
        String json = ledger.properties().get(key);
        return json == null ? Optional.empty()
                : Optional.of(codec.decode(json));
    }

    /**
     * Saves progress.
     *
     * @param mark the new mark
     */
    public void save(Mark mark)
    {
        String json = codec.encode(mark);
        committer.commit(ledger,
                tx -> tx.updateProperties().set(key, json).commit());
    }

    /**
     * A mark's fields, in a fixed order.
     *
     * @param mark the mark
     * @return its fields
     */
    private static Map<String, Object> canonical(Mark mark)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("snapshotId", mark.snapshotId());
        fields.put("backfill", mark.backfill());
        return fields;
    }

    /**
     * Rebuilds a mark from its parsed fields.
     *
     * @param fields the fields
     * @return the mark
     */
    private static Mark parse(Map<String, Object> fields)
    {
        try
        {
            return new Mark(JSONObjectUtils.getLong(fields, "snapshotId"),
                    JSONObjectUtils.getBoolean(fields, "backfill"));
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException("Malformed table progress",
                    malformed);
        }
    }
}
