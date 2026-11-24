/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StoreVersioningGuard}, with the store's answer given.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StoreVersioningGuardTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    StoreVersioningGuardTest()
    {
    }

    /** A store that keeps versions passes. */
    @Test
    void aVersionedStorePasses()
    {
        StoreVersioningGuard guard = guard(null, StoreVersioning.Verdict.ON,
                new AtomicInteger());

        assertThatCode(() -> guard.accept(table("landed_items", "bronze")))
                .doesNotThrowAnyException();
    }

    /**
     * A store without versions, or that won't say, is refused, naming the
     * table and the waiver, never the location or bucket.
     */
    @Test
    void anUnversionedOrUnknownStoreIsRefused()
    {
        assertThatThrownBy(() -> guard(null, StoreVersioning.Verdict.OFF,
                new AtomicInteger()).accept(table("landed_items", "bronze")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("store-versioning: table"
                        + " sdh.sdh_ingest.landed_items")
                .hasMessageContaining("versioning is off")
                .hasMessageContaining("sdh.compliance.waive.store-versioning")
                .hasMessageNotContaining("bronze");
        assertThatThrownBy(() -> guard(null, StoreVersioning.Verdict.UNKNOWN,
                new AtomicInteger()).accept(table("records", "silver")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("s3:GetBucketVersioning")
                .hasMessageNotContaining("silver");
    }

    /** A waiver lets the table open. */
    @Test
    void aWaiverLetsTheTableOpen()
    {
        StoreVersioningGuard guard = guard("Azure: versioning set on the"
                + " account by the platform team",
                StoreVersioning.Verdict.UNKNOWN, new AtomicInteger());

        assertThatCode(() -> guard.accept(table("landed_items", "bronze")))
                .doesNotThrowAnyException();
    }

    /** Each bucket is asked once, whatever its tables. */
    @Test
    void eachBucketIsAskedOnce()
    {
        AtomicInteger asked = new AtomicInteger();
        StoreVersioningGuard guard = guard(null, StoreVersioning.Verdict.ON,
                asked);

        guard.accept(table("landed_items", "bronze"));
        guard.accept(table("landing_ledger", "bronze"));
        guard.accept(table("landed_items", "bronze"));
        guard.accept(table("records", "silver"));

        assertThat(asked).hasValue(2);
    }

    /**
     * With the profile off nothing is checked; with it on, the waiver is
     * read from the settings.
     */
    @Test
    void theProfileDecidesWhetherStoresAreChecked()
    {
        Table table = table("landed_items", "bronze");
        assertThatCode(() -> ComplianceConfiguration.ComplianceGuard
                .storeGuard(ComplianceProfile.NONE, Map.of()).accept(table))
                .doesNotThrowAnyException();
        assertThat(ComplianceConfiguration.ComplianceGuard.storeGuard(
                ComplianceProfile.parse("hipaa"), Map.of()))
                .isInstanceOf(StoreVersioningGuard.class);
    }

    /**
     * A guard whose store answers with a verdict.
     *
     * @param waiver  the waiver's reason; null for none
     * @param verdict the store's answer
     * @param asked   counts the questions
     * @return the guard
     */
    private static StoreVersioningGuard guard(String waiver,
            StoreVersioning.Verdict verdict, AtomicInteger asked)
    {
        return new StoreVersioningGuard(ComplianceProfile.parse("hipaa"),
                waiver, (io, location) ->
                {
                    asked.incrementAndGet();
                    return verdict;
                });
    }

    /**
     * A table in a bucket.
     *
     * @param name   the table's name in {@code sdh.sdh_ingest}
     * @param bucket its bucket
     * @return the table
     */
    private static Table table(String name, String bucket)
    {
        Table table = mock(Table.class);
        when(table.name()).thenReturn("sdh.sdh_ingest." + name);
        when(table.location()).thenReturn("s3://" + bucket
                + "/warehouse/sdh_ingest/" + name);
        when(table.io()).thenReturn(mock(FileIO.class));
        return table;
    }
}
