/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.LedgerIndex;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.landing.TelemetryCapture;
import io.opentelemetry.api.common.AttributeKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for the ledger's share of the heap at startup
 * ({@link LandingRuntime#loadLedger}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LedgerHeapTest
{

    /** The intake under test. */
    static final String INTAKE = "claims-sftp";

    /** How many entries the ledger has. */
    static final int ENTRIES = 3_000;

    /** Captures the audit events. */
    @RegisterExtension
    final TelemetryCapture telemetry = new TelemetryCapture();

    /** The ledger, with {@link #ENTRIES} entries. */
    LandingLedger ledger;

    /**
     * Creates the ledger table and records its entries.
     *
     * @throws Exception if they can't be recorded
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        Table table = LandingTables.ledger(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(table);
        ledger = new LandingLedger(table, committer,
                new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1)));
        List<LandingLedger.Entry> entries = new ArrayList<>(ENTRIES);
        for (int index = 0; index < ENTRIES; index++)
        {
            entries.add(new LandingLedger.Entry(INTAKE, new SourceRef(
                    UUID.randomUUID().toString(), 1, "1"), UUID.randomUUID(),
                    "abcdefghijkl" + index, Instant.now(), 1, null));
        }
        ledger.record(entries);
    }

    /**
     * A ledger whose index fits in half the heap is loaded, sized for its
     * entries.
     *
     * @throws Exception if the test fails
     */
    @Test
    void loadsALedgerThatFits() throws Exception
    {
        long needed = LedgerIndex.bytesFor(ENTRIES);
        LedgerIndex index = LandingRuntime.loadLedger(ledger, INTAKE,
                2 * needed);
        assertThat(index.size()).isEqualTo(ENTRIES);
        assertThat(index.bytes()).isEqualTo(needed);
        assertThat(telemetry.events()).contains("ledger.memory_low")
                .doesNotContain("ledger.too_large");
    }

    /**
     * A ledger whose index would take more than half the heap stops
     * startup before it is loaded, with the heap it needs.
     */
    @Test
    void refusesALedgerTooLargeForTheHeap()
    {
        long needed = LedgerIndex.bytesFor(ENTRIES);
        assertThatThrownBy(() -> LandingRuntime.loadLedger(ledger, INTAKE,
                2 * needed - 1))
                .isInstanceOfSatisfying(LedgerTooLargeException.class,
                        failure ->
                        {
                            assertThat(failure.code())
                                    .isEqualTo(ErrorCode.LEDGER_TOO_LARGE);
                            assertThat(failure.getMessage()).isEqualTo(
                                    "The ledger's 3000 entries need a heap of"
                                            + " at least 1 MiB; raise the"
                                            + " memory limit");
                        });
        assertThat(telemetry.attributesOf("ledger.too_large"))
                .singleElement().satisfies(attributes ->
                {
                    assertThat(attributes.get(
                            AttributeKey.longKey("sdh.count")))
                            .isEqualTo(ENTRIES);
                    assertThat(attributes.get(
                            AttributeKey.longKey("sdh.bytes")))
                            .isEqualTo(2 * needed);
                    assertThat(attributes.get(
                            AttributeKey.stringKey("sdh.error_code")))
                            .isEqualTo("LEDGER_TOO_LARGE");
                });
    }
}
