/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SectionOrigins}: the positions it gives sections, whether the
 * background count is ahead of the split thread or behind it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SectionOriginsTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    SectionOriginsTest()
    {
    }

    /**
     * Every section of a file of several MiB, large and small, gets the
     * position a plain count from the file's start gives its first byte.
     * The file mixes line ends and UTF-8 characters, so a mark often falls
     * inside a CRLF or a character.
     *
     * @throws Exception if the file can't be read
     */
    @Test
    void sectionsGetTheirPositionsInTheFile() throws Exception
    {
        byte[] alphabet = {'a', 'b', ' ', '\n', '\r', (byte) 0xC3,
                (byte) 0xA9};
        Random random = new Random(7);
        byte[] file = new byte[6 * 1024 * 1024];
        for (int index = 0; index < file.length; index++)
        {
            file[index] = alphabet[random.nextInt(alphabet.length)];
        }
        try (SectionOrigins origins =
                new SectionOrigins(SeekableByteSource.of(file)))
        {
            LineCounter expected = new LineCounter();
            int offset = 0;
            while (offset < file.length)
            {
                // Mostly small sections, now and then one of up to 400 KiB.
                int length = random.nextInt(10) == 0
                        ? 1 + random.nextInt(400 * 1024)
                        : 1 + random.nextInt(2048);
                length = Math.min(length, file.length - offset);
                Section section = origins.withOrigin(
                        Section.ofRange(offset, length, ResumePoint.START));
                assertThat(section.origin().inFile()).as("offset %d", offset)
                        .isEqualTo(expected.position());
                expected.add(file, offset, length);
                offset += length;
            }
        }
    }
}
