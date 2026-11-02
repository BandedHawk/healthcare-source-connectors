/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.position;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SourcePosition}, {@link SectionOrigin} and
 * {@link LineCounter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PositionTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    PositionTest()
    {
    }

    /**
     * LF, CRLF and CR each end one line; a character of several bytes is
     * one column.
     */
    @Test
    void linesEndAtLfCrlfOrCrAndColumnsCountCharacters()
    {
        byte[] text = "ab\ncd\r\nef\rgéh".getBytes(StandardCharsets.UTF_8);
        assertThat(LineCounter.positionOf(text, 0)).isEqualTo(position(1, 1));
        assertThat(LineCounter.positionOf(text, 3)).isEqualTo(position(2, 1));
        // Between the CR and the LF of a CRLF, the line has already ended.
        assertThat(LineCounter.positionOf(text, 6)).isEqualTo(position(3, 1));
        assertThat(LineCounter.positionOf(text, 7)).isEqualTo(position(3, 1));
        assertThat(LineCounter.positionOf(text, 10)).isEqualTo(position(4, 1));
        // "g", then "é" (two bytes), then "h" in column 3.
        assertThat(LineCounter.positionOf(text, 13)).isEqualTo(position(4, 3));
        assertThat(LineCounter.positionOf(text, text.length))
                .isEqualTo(position(4, 4));
    }

    /** A UTF-8 byte order mark at the start isn't a column. */
    @Test
    void aByteOrderMarkIsNotCounted()
    {
        byte[] text = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, 'a', 'b'};
        assertThat(LineCounter.positionOf(text, 4)).isEqualTo(position(1, 2));
    }

    /**
     * offsetOf is the reverse of positionOf, for every byte that starts a
     * character.
     */
    @Test
    void offsetOfIsTheReverseOfPositionOf()
    {
        byte[] text = "ab\ncd\r\nef\rgéh".getBytes(StandardCharsets.UTF_8);
        for (int offset : new int[] {0, 1, 2, 3, 5, 7, 9, 10, 11, 13, 14})
        {
            assertThat(LineCounter.offsetOf(text,
                    LineCounter.positionOf(text, offset)))
                    .as("offset %d", offset)
                    .isEqualTo(offset);
        }
        // A column past the end of a line is at the line's end.
        assertThat(LineCounter.offsetOf(text, position(1, 9))).isEqualTo(2);
        // A line past the end of the text is at its end.
        assertThat(LineCounter.offsetOf(text, position(9, 1)))
                .isEqualTo(text.length);
    }

    /** Bytes added in pieces give the same position as all at once. */
    @Test
    void piecesCountTheSameAsTheWhole()
    {
        byte[] text = "a\r\nb\r".getBytes(StandardCharsets.UTF_8);
        LineCounter counter = new LineCounter();
        counter.add(text, 0, 2);
        counter.add(text, 2, 3);
        assertThat(counter.position())
                .isEqualTo(LineCounter.positionOf(text, text.length));
        assertThat(counter.count()).isEqualTo(text.length);
    }

    /**
     * Positions after the origin move by its offset; before it, they are at
     * the origin.
     */
    @Test
    void anOriginTurnsSectionPositionsIntoFilePositions()
    {
        SectionOrigin origin =
                new SectionOrigin(position(2, 1), position(40, 9));
        assertThat(origin.toFile(position(2, 1))).isEqualTo(position(40, 9));
        assertThat(origin.toFile(position(2, 5))).isEqualTo(position(40, 13));
        assertThat(origin.toFile(position(7, 3))).isEqualTo(position(45, 3));
        // In the XML declaration the splitter added: not in the file.
        assertThat(origin.toFile(position(1, 10))).isEqualTo(position(40, 9));
        assertThat(SectionOrigin.WHOLE_FILE.toFile(position(12, 7)))
                .isEqualTo(position(12, 7));
    }

    /** Lines and columns start at 1. */
    @Test
    void positionsStartAtOne()
    {
        assertThatThrownBy(() -> position(0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> position(1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(position(12, 7)).hasToString("line 12, column 7");
    }

    /**
     * A shorter way to write a position.
     *
     * @param line   the line, from 1
     * @param column the column, from 1
     * @return the position
     */
    private static SourcePosition position(long line, int column)
    {
        return new SourcePosition(line, column);
    }
}
