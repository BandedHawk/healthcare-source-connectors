/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Helpers shared by the splitter tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitTesting
{

    /** Not instantiable. */
    private SplitTesting()
    {
    }

    /**
     * Default settings with a tiny read buffer, so tokens straddle buffer
     * refills.
     *
     * @param format the format's name
     * @return the settings
     */
    public static SplitConfig config(String format)
    {
        return SplitConfig.defaults(format)
                .withReadBuffer(ByteScanner.MIN_BUFFER);
    }

    /**
     * Splits data through a registry, so the limits apply.
     *
     * @param splitter the splitter
     * @param data     the file
     * @param config   the split settings
     * @param from     where to start
     * @return the records
     * @throws Exception if splitting fails
     */
    public static List<SplitRecord> records(RecordSplitter splitter,
            byte[] data, SplitConfig config, ResumePoint from)
            throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(data);
        List<SplitRecord> out = new ArrayList<>();
        try (SplitSession session = SplitterRegistry.of(splitter)
                .open(splitter.format(), source, from, 0, config))
        {
            Optional<SplitRecord> next;
            while ((next = session.next()).isPresent())
            {
                out.add(next.get());
            }
        }
        return out;
    }

    /**
     * Reads records' bytes as text, checking each one's length.
     *
     * @param data    the file
     * @param records the records
     * @return their text
     * @throws Exception if a record can't be read
     */
    public static List<String> texts(byte[] data, List<SplitRecord> records)
            throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(data);
        List<String> out = new ArrayList<>();
        for (SplitRecord record : records)
        {
            byte[] bytes = record.readAll(source);
            assertThat((long) bytes.length)
                    .isEqualTo(record.plaintextLength());
            out.add(new String(bytes, UTF_8));
        }
        return out;
    }

    /**
     * Splits the whole input, then checks that resuming after each record
     * yields exactly the rest.
     *
     * @param splitter the splitter
     * @param input    the file, as text
     * @param config   the split settings
     * @return the records' text
     * @throws Exception if splitting fails
     */
    public static List<String> splitAndCheckResume(RecordSplitter splitter,
            String input, SplitConfig config) throws Exception
    {
        byte[] data = input.getBytes(UTF_8);
        List<SplitRecord> all = records(splitter, data, config,
                ResumePoint.START);
        List<String> texts = texts(data, all);
        for (int index = 0; index < all.size(); index++)
        {
            ResumePoint resume = ResumePoint.parse(
                    all.get(index).resume().encode());
            List<SplitRecord> rest = records(splitter, data, config, resume);
            assertThat(texts(data, rest))
                    .as("resume after record %d", index)
                    .isEqualTo(texts.subList(index + 1, texts.size()));
            assertThat(rest).extracting(SplitRecord::sourceOffset)
                    .isEqualTo(all.subList(index + 1, all.size()).stream()
                            .map(SplitRecord::sourceOffset).toList());
        }
        // Same answer with a normal-sized buffer.
        assertThat(texts(data, records(splitter, data,
                config.withReadBuffer(SplitConfig.DEFAULT_READ_BUFFER),
                ResumePoint.START))).isEqualTo(texts);
        return texts;
    }

    /**
     * The error splitting fails with.
     *
     * @param splitter the splitter
     * @param input    the file, as text
     * @param config   the split settings
     * @return the error
     * @throws AssertionError if splitting succeeds or fails otherwise
     */
    public static SplitException splitError(RecordSplitter splitter,
            String input, SplitConfig config)
    {
        try
        {
            records(splitter, input.getBytes(UTF_8), config,
                    ResumePoint.START);
        }
        catch (SplitException expected)
        {
            return expected;
        }
        catch (Exception other)
        {
            throw new AssertionError(other);
        }
        throw new AssertionError("Expected a SplitException");
    }
}
