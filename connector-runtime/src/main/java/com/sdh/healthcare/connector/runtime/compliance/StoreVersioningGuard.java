/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

/**
 * The {@link ComplianceCheck#STORE_VERSIONING} check, applied to each
 * table as it is opened (design §18.8): the store under it must keep
 * versions, so that what a restore finds missing can be brought back. A
 * table whose store doesn't, or won't say, is refused unless the check is
 * waived, which is audited once per table. Each bucket is asked once.
 *
 * <p>A refusal names the table, never its location or bucket.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class StoreVersioningGuard implements Consumer<Table>
{

    /** The profile in force. */
    private final ComplianceProfile profile;

    /** The waiver's reason; null if the check isn't waived. */
    private final String waiver;

    /** Asks a store whether it keeps versions. */
    private final BiFunction<FileIO, String,
            StoreVersioning.Verdict> probe;

    /** Each bucket's verdict, by scheme and bucket. */
    private final Map<String, StoreVersioning.Verdict> verdicts =
            new ConcurrentHashMap<>();

    /** The tables whose waiver has been audited. */
    private final Set<String> audited = ConcurrentHashMap.newKeySet();

    /**
     * Creates the guard.
     *
     * @param profile the profile in force
     * @param waiver  the check's waiver reason; null if not waived
     * @param probe   asks a store, given a table's IO and location
     */
    StoreVersioningGuard(ComplianceProfile profile, String waiver,
            BiFunction<FileIO, String, StoreVersioning.Verdict> probe)
    {
        this.profile = profile;
        this.waiver = waiver;
        this.probe = probe;
    }

    /**
     * Checks a table's store.
     *
     * @param table the table, just opened
     * @throws IllegalStateException if its store doesn't keep versions, or
     *                               won't say, and the check isn't waived
     */
    @Override
    public void accept(Table table)
    {
        String location = table.location();
        String key = StoreVersioning.bucket(location)
                .map(bucket -> scheme(location) + "://" + bucket)
                .orElse(location);
        StoreVersioning.Verdict verdict = verdicts.computeIfAbsent(key,
                ignored -> probe.apply(table.io(), location));
        if (verdict == StoreVersioning.Verdict.ON)
        {
            return;
        }
        ComplianceRules.Finding finding = new ComplianceRules.Finding(
                ComplianceCheck.STORE_VERSIONING, "table " + table.name());
        Fields fields = Fields.of(Attr.PROFILE, profile.id())
                .with(Attr.CHECK, finding.check().id())
                .with(Attr.SETTING, finding.setting());
        if (waiver != null)
        {
            if (audited.add(table.name()))
            {
                Telemetry.get().auditWarn("compliance.waived",
                        fields.with(Attr.WAIVER_REASON, waiver));
            }
            return;
        }
        Telemetry.get().auditError("compliance.refused", fields);
        throw new IllegalStateException(ComplianceRules.PROFILE + "="
                + profile.id() + " refuses " + finding.describe() + ": "
                + (verdict == StoreVersioning.Verdict.OFF
                        ? "the store says versioning is off"
                        : "the store couldn't be asked (on S3 the stage"
                                + " needs s3:GetBucketVersioning; Azure"
                                + " and other stores can't be asked)")
                + " (design §18.8). Turn versioning on, or accept it by"
                + " setting " + ComplianceRules.WAIVE
                + finding.check().id() + " to the reason.");
    }

    /**
     * A location's scheme.
     *
     * @param location the location
     * @return its scheme; empty if it has none
     */
    private static String scheme(String location)
    {
        int colon = location.indexOf(':');
        return colon < 0 ? "" : location.substring(0, colon);
    }
}
