/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.position;

/**
 * Works out line and column positions from bytes, reading them in order: give
 * it the bytes of a text from its start ({@link #add}), in as many pieces as
 * convenient, and it knows the position of the next byte
 * ({@link #position()}).
 * <p>
 * It counts the way an XML parser does, so its positions match those the
 * C-CDA converter writes:
 * <ul>
 * <li>A line ends at a line feed (LF), a carriage return and line feed (CRLF,
 * one line end) or a carriage return alone (CR).</li>
 * <li>A column is a character. The bytes are taken as UTF-8, so a character
 * of several bytes is one column. A UTF-8 byte order mark at the very start
 * isn't counted.</li>
 * </ul>
 * In a text in a single-byte character set other than ASCII (such as ISO
 * 8859-1), a byte from 0x80 to 0xBF is taken for part of a UTF-8 character
 * and not counted, so a column after one can be early; lines are always
 * right.
 * <p>
 * A counter isn't thread-safe; use one per text.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LineCounter
{

    /** The line of the next byte, from 1. */
    private long line = 1;

    /** The column of the next byte, from 1, in characters. */
    private int column = 1;

    /** How many bytes have been added. */
    private long count;

    /**
     * Whether the last byte added was a CR, so that an LF right after it
     * belongs to the same line end.
     */
    private boolean afterCarriageReturn;

    /**
     * How many bytes of a UTF-8 byte order mark have been seen at the start
     * of the text; -1 once a byte that isn't part of one has been seen.
     */
    private int byteOrderMark;

    /** Creates a counter at the start of a text: line 1, column 1. */
    public LineCounter()
    {
    }

    /**
     * Counts some bytes of the text, the ones that follow those added
     * before.
     *
     * @param bytes  the bytes
     * @param from   where they start in the array
     * @param length how many there are
     */
    public void add(byte[] bytes, int from, int length)
    {
        int end = from + length;
        int index = from;
        // The byte order mark is only looked for at the start of the text.
        while (index < end && byteOrderMark >= 0)
        {
            add(bytes[index++]);
        }
        count += end - index;
        if (index == end)
        {
            return;
        }
        // The same counting as add(byte), done in bulk: a file is counted on
        // the split thread, which limits large records. Every CR ends a
        // line, and every LF that doesn't follow a CR.
        int breaks = 0;
        int crlfs = afterCarriageReturn && bytes[index] == '\n' ? 1 : 0;
        for (int at = index; at < end; at++)
        {
            byte value = bytes[at];
            breaks += (value == '\n' ? 1 : 0) + (value == '\r' ? 1 : 0);
        }
        for (int at = index + 1; at < end; at++)
        {
            crlfs += bytes[at - 1] == '\r' && bytes[at] == '\n' ? 1 : 0;
        }
        line += breaks - crlfs;
        // The column counts the characters after the last line end.
        int lastBreak = end - 1;
        while (lastBreak >= index && bytes[lastBreak] != '\n'
                && bytes[lastBreak] != '\r')
        {
            lastBreak--;
        }
        if (lastBreak >= index)
        {
            column = 1;
        }
        column += characters(bytes, lastBreak + 1, end);
        afterCarriageReturn = bytes[end - 1] == '\r';
    }

    /**
     * Counts the characters in some UTF-8 bytes: every byte but the
     * continuation bytes, 10xxxxxx.
     *
     * @param bytes the bytes
     * @param from  where they start in the array
     * @param end   where they end, not included
     * @return how many characters start there
     */
    private static int characters(byte[] bytes, int from, int end)
    {
        int characters = 0;
        for (int at = from; at < end; at++)
        {
            characters += (bytes[at] & 0xC0) != 0x80 ? 1 : 0;
        }
        return characters;
    }

    /**
     * Counts one byte of the text, the one after those added before.
     *
     * @param value the byte
     */
    public void add(byte value)
    {
        count++;
        if (skipsByteOrderMark(value))
        {
            return;
        }
        if (value == '\n')
        {
            if (!afterCarriageReturn)
            {
                newLine();
            }
            afterCarriageReturn = false;
            return;
        }
        afterCarriageReturn = value == '\r';
        if (afterCarriageReturn)
        {
            newLine();
        }
        else if ((value & 0xC0) != 0x80)
        {
            // Every byte starts a character except UTF-8's continuation
            // bytes, 10xxxxxx.
            column++;
        }
    }

    /**
     * Makes a counter that has counted the same bytes as this one, to count
     * on from here separately.
     *
     * @return a new counter in the same state
     */
    public LineCounter copy()
    {
        LineCounter copy = new LineCounter();
        copy.line = line;
        copy.column = column;
        copy.count = count;
        copy.afterCarriageReturn = afterCarriageReturn;
        copy.byteOrderMark = byteOrderMark;
        return copy;
    }

    /**
     * The position of the next byte: of the character right after those
     * counted so far.
     *
     * @return the line and column
     */
    public SourcePosition position()
    {
        return new SourcePosition(line, column);
    }

    /**
     * How many bytes have been counted.
     *
     * @return the number of bytes added
     */
    public long count()
    {
        return count;
    }

    /**
     * The position of a byte of a text.
     *
     * @param bytes  the text, from its start
     * @param offset the byte's index, from 0; the text's length for the
     *               position just after it
     * @return the byte's line and column
     * @throws IndexOutOfBoundsException if the offset is negative or past the
     *                                   end of the text
     */
    public static SourcePosition positionOf(byte[] bytes, int offset)
    {
        if (offset < 0 || offset > bytes.length)
        {
            throw new IndexOutOfBoundsException(offset);
        }
        LineCounter counter = new LineCounter();
        counter.add(bytes, 0, offset);
        return counter.position();
    }

    /**
     * The index of the byte at a line and column of a text: the reverse of
     * {@link #positionOf}.
     *
     * @param bytes    the text, from its start
     * @param position a line and column
     * @return the index of the first byte of the character there; the text's
     *         length if the text ends first, and the index of the line's end
     *         if the line is shorter than the column
     */
    public static int offsetOf(byte[] bytes, SourcePosition position)
    {
        LineCounter counter = new LineCounter();
        for (int index = 0; index < bytes.length; index++)
        {
            SourcePosition here = counter.position();
            // Only the first byte of a character has its position: not a
            // UTF-8 continuation byte, nor the LF of a CRLF, which belongs to
            // the line it ends though the next line has already begun.
            boolean startsCharacter = (bytes[index] & 0xC0) != 0x80
                    && !(bytes[index] == '\n' && counter.afterCarriageReturn);
            if (!here.isBefore(position) && startsCharacter)
            {
                return index;
            }
            long lineBefore = here.line();
            counter.add(bytes[index]);
            if (counter.line > lineBefore && lineBefore == position.line())
            {
                // The line ended before the column: this byte ended it.
                return index;
            }
        }
        return bytes.length;
    }

    /** Moves to the start of the next line. */
    private void newLine()
    {
        line++;
        column = 1;
    }

    /**
     * Tells whether a byte is part of a UTF-8 byte order mark ({@code EF BB
     * BF}) at the very start of the text, which isn't counted.
     *
     * @param value the byte just added
     * @return true to skip it
     */
    private boolean skipsByteOrderMark(byte value)
    {
        if (byteOrderMark < 0 || byteOrderMark >= 3)
        {
            byteOrderMark = -1;
            return false;
        }
        int expected = byteOrderMark == 0 ? 0xEF
                : byteOrderMark == 1 ? 0xBB : 0xBF;
        if ((value & 0xFF) == expected)
        {
            byteOrderMark++;
            return true;
        }
        if (byteOrderMark > 0)
        {
            // A partial mark: count the bytes skipped as one character, as
            // they began one.
            column++;
        }
        byteOrderMark = -1;
        return false;
    }
}
