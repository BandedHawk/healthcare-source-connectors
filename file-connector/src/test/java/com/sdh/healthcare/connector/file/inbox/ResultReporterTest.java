/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.landing.RecordIndex;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor.Outcome;
import com.sdh.healthcare.connector.core.storage.CommittedSections.Failed;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link ResultReporter} builds results.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ResultReporterTest
{

    /** The item. */
    static final UUID ITEM = UUID.randomUUID();

    /** When it finished. */
    static final Instant AT = Instant.parse("2026-11-04T12:00:00Z");

    /** A batch of four messages; the third is empty. */
    static final RecordIndex BATCH = new RecordIndex(
            List.of(0L, 100L, 250L, 250L), Arrays.asList("k0", "k1", null,
                    "k3"));

    /** Finds the message that holds an offset. */
    @Test
    void placesAnOffsetInItsMessage()
    {
        assertThat(ResultReporter.message(null, 50)).isEqualTo(-1);
        assertThat(ResultReporter.message(BATCH, 0)).isZero();
        assertThat(ResultReporter.message(BATCH, 99)).isZero();
        assertThat(ResultReporter.message(BATCH, 100)).isEqualTo(1);
        assertThat(ResultReporter.message(BATCH, 249)).isEqualTo(1);
        assertThat(ResultReporter.message(BATCH, 250)).isEqualTo(3);
        assertThat(ResultReporter.message(BATCH, 9_999)).isEqualTo(3);
    }

    /**
     * Problems are split into parts, each carrying the totals, and placed
     * in their messages with the broker IDs.
     */
    @Test
    void splitsProblemsIntoPartsWithTheTotals()
    {
        List<Failed> failed = List.of(
                new Failed(0, 10, "CONVERT_A", new SourcePosition(1, 5)),
                new Failed(2, 120, "CONVERT_B", null),
                new Failed(5, 300, "CONVERT_C", new SourcePosition(9, 1)));
        ResultReporter.Landed landed = new ResultReporter.Landed("ref-1",
                null, BATCH, Arrays.asList("m0", "m1", "m2", null));
        List<ItemResult> parts = ResultReporter.parts(
                new Outcome(ITEM, ItemProcessor.Result.COMPLETED, 6, null),
                "claims", failed, landed, AT, 2, Integer.MAX_VALUE);

        assertThat(parts).hasSize(2);
        assertThat(parts).extracting(ItemResult::part).containsExactly(1, 2);
        assertThat(parts).allSatisfy(part ->
        {
            assertThat(part.status()).isEqualTo(ItemResult.COMPLETED);
            assertThat(part.partCount()).isEqualTo(2);
            assertThat(part.problemCount()).isEqualTo(3);
            assertThat(part.failedSectionCount()).isEqualTo(3);
            assertThat(part.sectionCount()).isEqualTo(6);
            assertThat(part.failure()).isNull();
            assertThat(part.producerRef()).isEqualTo("ref-1");
            assertThat(part.finishedAt()).isEqualTo(AT);
        });
        assertThat(parts.get(0).problems()).containsExactly(
                new Problem("CONVERT_A", "convert", 0, 10,
                        new SourcePosition(1, 5), 0, "m0"),
                new Problem("CONVERT_B", "convert", 2, 120, null, 1, "m1"));
        assertThat(parts.get(1).problems()).containsExactly(
                new Problem("CONVERT_C", "convert", 5, 300,
                        new SourcePosition(9, 1), 3, null));
        assertThat(parts.get(1).messageId())
                .isEqualTo("rs-" + ITEM + "-2");
    }

    /** A failed item has one part, with the failure and no problems. */
    @Test
    void reportsAFailedItemInOnePart()
    {
        UUID earlier = UUID.randomUUID();
        Failure failure = new Failure("split",
                ErrorCode.values()[0], 42, AT);
        List<ItemResult> parts = ResultReporter.parts(
                new Outcome(ITEM, ItemProcessor.Result.FAILED, 0, failure),
                "claims", List.of(),
                new ResultReporter.Landed(null, earlier, null, null), AT,
                ResultReporter.PART_SIZE, ResultReporter.PART_BYTES);

        assertThat(parts).singleElement().satisfies(part ->
        {
            assertThat(part.status()).isEqualTo(ItemResult.FAILED);
            assertThat(part.partCount()).isEqualTo(1);
            assertThat(part.problems()).isEmpty();
            assertThat(part.problemCount()).isZero();
            assertThat(part.replaces()).isEqualTo(earlier);
            assertThat(part.failure()).isEqualTo(new Problem(
                    ErrorCode.values()[0].name(), "split", -1, 42, null, -1,
                    null));
        });
    }

    /**
     * Even with the longest broker IDs, each sealed part fits the smallest
     * broker limit, 256 KB; parts are cut by bytes as well as by count.
     *
     * @throws Exception if a key can't be made
     */
    @Test
    void keepsEachSealedPartUnderTheSmallestBrokerLimit() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(256);
        KeyPair signing = generator.generateKeyPair();
        KeyPair producer = generator.generateKeyPair();
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signing.getPrivate()),
                "connector-2026", producer.getPublic(), "producer-2026");

        int count = 5_000;
        List<Long> offsets = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        List<String> refs = new ArrayList<>();
        List<Failed> failed = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            offsets.add(i * 1_000_000_000L);
            keys.add(null);
            refs.add("\"".repeat(255));
            failed.add(new Failed(i, i * 1_000_000_000L + 7,
                    "CONVERT_CSV_FIELD_COUNT",
                    new SourcePosition(Long.MAX_VALUE, Integer.MAX_VALUE)));
        }
        ResultReporter.Landed landed = new ResultReporter.Landed("x".repeat(
                1_000), UUID.randomUUID(), new RecordIndex(offsets, keys),
                refs);
        List<ItemResult> parts = ResultReporter.parts(
                new Outcome(ITEM, ItemProcessor.Result.COMPLETED, count, null),
                "claims", failed, landed, AT, ResultReporter.PART_SIZE,
                ResultReporter.PART_BYTES);

        assertThat(parts.size())
                .isGreaterThan(count / ResultReporter.PART_SIZE);
        assertThat(parts.stream().mapToInt(part -> part.problems().size())
                .sum()).isEqualTo(count);
        assertThat(parts).allSatisfy(part -> assertThat(
                sealer.seal(part).length).isLessThan(256 * 1000));

        // Short problems are cut by count.
        List<ItemResult> small = ResultReporter.parts(
                new Outcome(ITEM, ItemProcessor.Result.COMPLETED, 2_500, null),
                "claims", failed.subList(0, 2_500).stream()
                        .map(f -> new Failed(f.seq(), 0, "C", null))
                        .toList(),
                ResultReporter.Landed.NONE, AT, ResultReporter.PART_SIZE,
                ResultReporter.PART_BYTES);
        assertThat(small).extracting(part -> part.problems().size())
                .containsExactly(1_000, 1_000, 500);
    }
}
