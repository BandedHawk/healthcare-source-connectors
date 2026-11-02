/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.position;

/**
 * A place in a text: a line and a column, both counted from 1, the column in
 * characters, as a text editor shows them. It says where something is in a
 * section or in the file it came from, so an operator can find it there: an
 * element of a C-CDA document, the start of a section, or the place a
 * conversion failed.
 * <p>
 * Lines end at a line feed (LF), a carriage return and line feed (CRLF) or a
 * carriage return alone (CR), as XML counts them. {@link LineCounter} works
 * positions out from bytes.
 *
 * @param line   the line, from 1
 * @param column the column on that line, in characters, from 1
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see SectionOrigin
 */
public record SourcePosition(long line, int column)
{

    /** The start of a text: line 1, column 1. */
    public static final SourcePosition START = new SourcePosition(1, 1);

    /**
     * Checks the position.
     *
     * @throws IllegalArgumentException if the line or the column is less than
     *                                  1
     */
    public SourcePosition
    {
        if (line < 1 || column < 1)
        {
            throw new IllegalArgumentException(
                    "A line and a column are counted from 1");
        }
    }

    /**
     * Tells whether this position comes before another one in the same text.
     *
     * @param other the other position
     * @return true if this one is on an earlier line, or earlier on the same
     *         line
     */
    public boolean isBefore(SourcePosition other)
    {
        return line < other.line
                || line == other.line && column < other.column;
    }

    /**
     * The position as an operator reads it.
     *
     * @return for example {@code line 120, column 7}
     */
    @Override
    public String toString()
    {
        return "line " + line + ", column " + column;
    }
}
