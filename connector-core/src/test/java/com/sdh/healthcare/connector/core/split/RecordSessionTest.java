/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.split.builtin.CsvSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for splitting a message batch message by message
 * ({@link SplitterRegistry#openRecords}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RecordSessionTest
{

    /** Splits CSV. */
    static final CsvSplitter CSV = new CsvSplitter();

    /**
     * CSV settings with a header row, repeated in each section.
     *
     * @return the settings
     */
    static SplitConfig config()
    {
        return SplitTesting.config(CSV.format())
                .withOption("csv.header", "true");
    }

    /**
     * A batch: its bytes and where each message starts.
     *
     * @param bytes the batch
     * @param offsets where each message starts
     */
    record Batch(byte[] bytes, List<Long> offsets)
    {
    }

    /**
     * Joins messages into a batch.
     *
     * @param messages the messages
     * @return the batch
     */
    static Batch batch(String... messages)
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        List<Long> offsets = new ArrayList<>();
        for (String message : messages)
        {
            offsets.add((long) bytes.size());
            bytes.writeBytes(message.getBytes(UTF_8));
        }
        return new Batch(bytes.toByteArray(), offsets);
    }

    /**
     * Splits a batch from a resume point.
     *
     * @param batch  the batch
     * @param config the split settings
     * @param from   where to start
     * @return the sections
     * @throws Exception if splitting fails
     */
    static List<Section> split(Batch batch, SplitConfig config,
            ResumePoint from) throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(batch.bytes());
        List<Section> sections = new ArrayList<>();
        try (SplitSession session = SplitterRegistry.of(CSV).openRecords(
                CSV.format(), source, batch.offsets(), from, 0, config))
        {
            Optional<Section> next;
            while ((next = session.next()).isPresent())
            {
                sections.add(next.get());
            }
        }
        return sections;
    }

    /**
     * The sections' text.
     *
     * @param batch    the batch
     * @param sections the sections
     * @return each section's text
     * @throws Exception if one can't be read
     */
    static List<String> texts(Batch batch, List<Section> sections)
            throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(batch.bytes());
        List<String> texts = new ArrayList<>();
        for (Section section : sections)
        {
            texts.add(new String(section.readAll(source), UTF_8));
        }
        return texts;
    }

    /**
     * Each message is split on its own, with its own header, and the
     * sections' offsets are where they sit in the batch; resuming after any
     * section gives the rest, including across a message boundary.
     *
     * @throws Exception if the test fails
     */
    @Test
    void splitsEachMessageOnItsOwnAndResumesAnywhere() throws Exception
    {
        Batch batch = batch("id,name\n1,a\n2,b\n", "", "code\nX\n");
        SplitConfig config = config();
        List<Section> all = split(batch, config, ResumePoint.START);
        assertThat(texts(batch, all)).containsExactly("id,name\n1,a\n",
                "id,name\n2,b\n", "code\nX\n");
        assertThat(all).extracting(Section::failure).containsOnlyNulls();
        assertThat(all.get(2).sourceOffset())
                .isEqualTo(batch.offsets().get(2) + "code\n".length());
        for (int i = 0; i < all.size(); i++)
        {
            ResumePoint from = ResumePoint.parse(all.get(i).resume().encode());
            assertThat(texts(batch, split(batch, config, from)))
                    .as("after section %d", i)
                    .isEqualTo(texts(batch, all).subList(i + 1, all.size()));
        }
    }

    /**
     * A message that can't be split fails alone: the rest of it, from its
     * last good section, is one section carrying the error, and the next
     * message is split as usual.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsOnlyTheMessageThatCantBeSplit() throws Exception
    {
        Batch batch = batch("id\n1\n", "id\n7\n\"open\n", "id\n9\n");
        List<Section> all = split(batch, config(),
                ResumePoint.START);
        assertThat(texts(batch, all)).containsExactly("id\n1\n", "id\n7\n",
                "\"open\n", "id\n9\n");
        Section failed = all.get(2);
        assertThat(failed.failure().code())
                .isEqualTo(ErrorCode.PARSE_UNTERMINATED_QUOTE);
        assertThat(failed.sourceOffset())
                .isEqualTo(batch.offsets().get(1) + "id\n7\n".length());
        assertThat(failed.failure().offset())
                .isBetween(failed.sourceOffset(), batch.offsets().get(2));
        assertThat(failed.resume().offset()).isEqualTo(batch.offsets().get(2));
        assertThat(texts(batch, split(batch, config(),
                failed.resume()))).containsExactly("id\n9\n");
    }

    /**
     * A section over the size limit fails its message, which is kept whole
     * from that section on, without failing the batch.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsAMessageWithASectionOverTheLimit() throws Exception
    {
        Batch batch = batch("id\n" + "x".repeat(40) + "\n", "id\n2\n");
        SplitConfig config = config()
                .withLimits(new SplitLimits(1 << 20, 20, Long.MAX_VALUE));
        List<Section> all = split(batch, config, ResumePoint.START);
        assertThat(all).hasSize(2);
        assertThat(all.get(0).failure().code())
                .isEqualTo(ErrorCode.LIMIT_SECTION_SIZE);
        assertThat(texts(batch, all).get(0))
                .isEqualTo("id\n" + "x".repeat(40) + "\n");
        assertThat(texts(batch, all).get(1)).isEqualTo("id\n2\n");
    }
}
