/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.storage.CommittedRecords.Failed;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResultReporter}'s parts.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ResultReporterTest
{

    /** The item. */
    static final UUID ITEM = UUID.randomUUID();

    /** When it finished. */
    static final Instant AT = Instant.parse("2026-11-06T12:00:00Z");

    /** Creates the tests. */
    ResultReporterTest()
    {
    }

    /**
     * The item's {@code item.split}.
     *
     * @param records     how many records it has
     * @param failed      how many of them failed
     * @param producerRef the producer's reference; null if none
     * @param replaces    the item it replaces; null if none
     * @param failure     the item's own failure; null if it was split
     * @return the message
     */
    static ItemSplit total(long records, long failed, String producerRef,
            UUID replaces, Problem failure)
    {
        return new ItemSplit(ITEM, "claims", records, failed, 100, "digest",
                producerRef, replaces, failure, AT, AT);
    }

    /**
     * Problems are split into parts, each carrying the totals, put down to
     * the stage that found them, and placed in their messages with the
     * broker IDs and the messages they replace, as kept on their rows.
     */
    @Test
    void splitsProblemsIntoPartsWithTheTotals()
    {
        List<Failed> failed = List.of(
                new Failed(0, 10, "CONVERT_A", new SourcePosition(1, 5), 0,
                        "m0", null),
                new Failed(2, 120, "PARSE_B", null, 1, "m1", "old/m9"),
                new Failed(5, 300, "CONVERT_C", new SourcePosition(9, 1), 3,
                        null, null));
        List<ItemResult> parts = ResultReporter.parts(
                total(6, 1, "ref-1", null, null), failed, AT, 2,
                Integer.MAX_VALUE);

        assertThat(parts).hasSize(2);
        assertThat(parts).extracting(ItemResult::part).containsExactly(1, 2);
        assertThat(parts).allSatisfy(part ->
        {
            assertThat(part.status()).isEqualTo(ItemResult.COMPLETED);
            assertThat(part.intake()).isEqualTo("claims");
            assertThat(part.partCount()).isEqualTo(2);
            assertThat(part.problemCount()).isEqualTo(3);
            assertThat(part.failedRecordCount()).isEqualTo(3);
            assertThat(part.recordCount()).isEqualTo(6);
            assertThat(part.failure()).isNull();
            assertThat(part.producerRef()).isEqualTo("ref-1");
            assertThat(part.finishedAt()).isEqualTo(AT);
        });
        assertThat(parts.get(0).problems()).containsExactly(
                new Problem("CONVERT_A", "convert", 0, 10,
                        new SourcePosition(1, 5), 0, "m0", null),
                new Problem("PARSE_B", "split", 2, 120, null, 1, "m1",
                        "old/m9"));
        assertThat(parts.get(1).problems()).containsExactly(
                new Problem("CONVERT_C", "convert", 5, 300,
                        new SourcePosition(9, 1), 3, null, null));
        assertThat(parts.get(1).messageId())
                .isEqualTo("rs-" + ITEM + "-2");
    }

    /**
     * An item that failed as a whole has one part, with its failure as the
     * split stage placed it, and the item it replaces.
     */
    @Test
    void reportsAFailedItemInOnePart()
    {
        UUID earlier = UUID.randomUUID();
        Problem failure = new Problem("LIMIT_ITEM_SIZE", "split", -1, 42,
                null, 0, "m0", null);
        List<ItemResult> parts = ResultReporter.parts(
                total(0, 0, null, earlier, failure), List.of(), AT,
                ResultReporter.PART_SIZE, ResultReporter.PART_BYTES);

        assertThat(parts).singleElement().satisfies(part ->
        {
            assertThat(part.status()).isEqualTo(ItemResult.FAILED);
            assertThat(part.partCount()).isEqualTo(1);
            assertThat(part.problems()).isEmpty();
            assertThat(part.problemCount()).isZero();
            assertThat(part.replaces()).isEqualTo(earlier);
            assertThat(part.failure()).isEqualTo(failure);
        });
    }

    /**
     * While a failure is open, the result is incomplete, from the split
     * stage's item.progress, with the problems of both stages put down to
     * each; one counts made differently is a different message, and an
     * item that couldn't be split further is still failed (STAGES.md
     * §15.6).
     */
    @Test
    void reportsAnItemWithFailuresOpenAsIncomplete()
    {
        ConnectorMessage.ItemProgressed progressed =
                new ConnectorMessage.ItemProgressed(ITEM, "claims", "split",
                        3, 2, 1, 0, null, "claims.csv", null, AT, null);
        ResultReporter.Facts facts = ResultReporter.Facts.of(progressed);
        List<Failed> failed = List.of(
                new Failed(0, 0, "PARSE_UNTERMINATED_QUOTE", null),
                new Failed(2, 40, "CONVERT_CSV_FIELD_COUNT", null));
        List<ItemResult> parts = ResultReporter.parts(facts,
                ItemResult.INCOMPLETE, 3, failed, AT,
                ResultReporter.PART_SIZE, ResultReporter.PART_BYTES);
        assertThat(parts).singleElement().satisfies(part ->
        {
            assertThat(part.status()).isEqualTo(ItemResult.INCOMPLETE);
            assertThat(part.recordCount()).isEqualTo(3);
            assertThat(part.failedRecordCount()).isEqualTo(2);
            assertThat(part.producerRef()).isEqualTo("claims.csv");
            assertThat(part.problems()).extracting(Problem::stage)
                    .containsExactly("split", "convert");
        });
        assertThat(ResultReporter.parts(facts, ItemResult.INCOMPLETE, 3,
                failed.subList(0, 1), AT, ResultReporter.PART_SIZE,
                ResultReporter.PART_BYTES).getFirst().messageId())
                .isNotEqualTo(parts.getFirst().messageId());

        Problem failure = new Problem("PARSE_XML_MALFORMED", "split", -1, 9,
                null, -1, null, null);
        assertThat(ResultReporter.parts(ResultReporter.Facts.of(
                new ConnectorMessage.ItemProgressed(ITEM, "claims", "split",
                        0, 0, 1, 0, failure, null, null, AT, null)),
                ItemResult.INCOMPLETE, 0, List.of(), AT,
                ResultReporter.PART_SIZE, ResultReporter.PART_BYTES))
                .singleElement().satisfies(part -> assertThat(part.status())
                        .isEqualTo(ItemResult.FAILED));
        assertThat(ResultReporter.Facts.of(null)).isNull();
    }

    /**
     * Even with the longest broker IDs and links, each sealed part fits
     * the smallest broker limit, 256 KB; parts are cut by bytes as well as
     * by count.
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
        List<Failed> failed = new ArrayList<>();
        for (int index = 0; index < count; index++)
        {
            failed.add(new Failed(index, index * 1_000_000_000L + 7,
                    "CONVERT_CSV_FIELD_COUNT",
                    new SourcePosition(Long.MAX_VALUE, Integer.MAX_VALUE),
                    index, "\"".repeat(255),
                    UUID.randomUUID() + "/" + "\"".repeat(255)));
        }
        List<ItemResult> parts = ResultReporter.parts(
                total(count, count, "x".repeat(1_000), UUID.randomUUID(),
                        null), failed, AT, ResultReporter.PART_SIZE,
                ResultReporter.PART_BYTES);

        assertThat(parts.size())
                .isGreaterThan(count / ResultReporter.PART_SIZE);
        assertThat(parts.stream().mapToInt(part -> part.problems().size())
                .sum()).isEqualTo(count);
        assertThat(parts).allSatisfy(part -> assertThat(
                sealer.seal(part).length).isLessThan(256 * 1000));

        // Short problems are cut by count.
        List<ItemResult> small = ResultReporter.parts(
                total(2_500, 2_500, null, null, null),
                failed.subList(0, 2_500).stream()
                        .map(f -> new Failed(f.seq(), 0, "C", null))
                        .toList(),
                AT, ResultReporter.PART_SIZE, ResultReporter.PART_BYTES);
        assertThat(small).extracting(part -> part.problems().size())
                .containsExactly(1_000, 1_000, 500);
    }
}
