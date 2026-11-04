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
     * @return the sections
     * @throws Exception if splitting fails
     */
    public static List<Section> sections(SectionSplitter splitter,
            byte[] data, SplitConfig config, ResumePoint from)
            throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(data);
        List<Section> out = new ArrayList<>();
        try (SplitSession session = SplitterRegistry.of(splitter)
                .open(splitter.format(), source, from, 0, config))
        {
            Optional<Section> next;
            while ((next = session.next()).isPresent())
            {
                out.add(next.get());
            }
        }
        return out;
    }

    /**
     * Reads sections' bytes as text, checking each one's length.
     *
     * @param data     the file
     * @param sections the sections
     * @return their text
     * @throws Exception if a section can't be read
     */
    public static List<String> texts(byte[] data, List<Section> sections)
            throws Exception
    {
        SeekableByteSource source = SeekableByteSource.of(data);
        List<String> out = new ArrayList<>();
        for (Section section : sections)
        {
            byte[] bytes = section.readAll(source);
            assertThat((long) bytes.length)
                    .isEqualTo(section.plaintextLength());
            out.add(new String(bytes, UTF_8));
        }
        return out;
    }

    /**
     * Splits the whole input, then checks that resuming after each section
     * yields exactly the rest.
     *
     * @param splitter the splitter
     * @param input    the file, as text
     * @param config   the split settings
     * @return the sections' text
     * @throws Exception if splitting fails
     */
    public static List<String> splitAndCheckResume(SectionSplitter splitter,
            String input, SplitConfig config) throws Exception
    {
        byte[] data = input.getBytes(UTF_8);
        List<Section> all = sections(splitter, data, config,
                ResumePoint.START);
        List<String> texts = texts(data, all);
        for (int index = 0; index < all.size(); index++)
        {
            ResumePoint resume = ResumePoint.parse(
                    all.get(index).resume().encode());
            List<Section> rest = sections(splitter, data, config, resume);
            assertThat(texts(data, rest))
                    .as("resume after section %d", index)
                    .isEqualTo(texts.subList(index + 1, texts.size()));
            assertThat(rest).extracting(Section::sourceOffset)
                    .isEqualTo(all.subList(index + 1, all.size()).stream()
                            .map(Section::sourceOffset).toList());
        }
        // Same answer with a normal-sized buffer.
        assertThat(texts(data, sections(splitter, data,
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
    public static SplitException splitError(SectionSplitter splitter,
            String input, SplitConfig config)
    {
        try
        {
            sections(splitter, input.getBytes(UTF_8), config,
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
