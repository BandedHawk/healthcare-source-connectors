/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.position;

import java.util.Objects;

/**
 * Where a record's text came from in its file, so that a position in the
 * record can be turned into a position in the file
 * ({@link #toFile(SourcePosition)}).
 * <p>
 * A record is mostly a copy of part of the file, but a splitter can add text
 * of its own before that part: the XML splitter starts each record with the
 * file's XML declaration on a line of its own, and the X12 and EDIFACT
 * splitters add envelope segments. So the origin pairs two positions of the
 * same character, the first one copied from the file: where it is in the
 * record, and where it is in the file. For a record that is the file's text
 * from its start, both are line 1, column 1.
 * <p>
 * Positions in the record are what a converter reports (such as the
 * {@code #line} and {@code #col} keys of C-CDA JSON) and what a mapping
 * reports in its issues; the file positions are what an operator needs to
 * find the problem in the file as it was received.
 *
 * @param inRecord where the copied text starts in the record
 * @param inItem   where the same text starts in the file
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record RecordOrigin(SourcePosition inRecord, SourcePosition inItem)
{

    /**
     * The origin of a record that is a file's text from its start, with
     * nothing added: every position is the same in both.
     */
    public static final RecordOrigin WHOLE_FILE = new RecordOrigin(
            SourcePosition.START, SourcePosition.START);

    /**
     * Checks that both positions are given.
     *
     * @throws NullPointerException if either is null
     */
    public RecordOrigin
    {
        Objects.requireNonNull(inRecord, "inRecord");
        Objects.requireNonNull(inItem, "inItem");
    }

    /**
     * Turns a position in the record into the position of the same character
     * in the file.
     * <p>
     * Lines after the origin's line are moved by the same number of lines.
     * On the origin's own line, columns are moved too, as the copied text can
     * start partway through a line of the file. A position before the origin
     * is in text the splitter added, which isn't in the file as it is; it is
     * given as the origin's file position, the nearest place the file has.
     * When the XML splitter adds namespace declarations to a document's root
     * start tag, the whole tag is such text, so the root element is given at
     * the end of its start tag in the file; the elements inside it are
     * exact.
     *
     * @param inRecord a position in the record
     * @return the position in the file
     */
    public SourcePosition toFile(SourcePosition inRecord)
    {
        if (inRecord.isBefore(this.inRecord))
        {
            return inItem;
        }
        long line = inItem.line() + inRecord.line() - this.inRecord.line();
        int column = inRecord.line() == this.inRecord.line()
                ? inItem.column() + inRecord.column() - this.inRecord.column()
                : inRecord.column();
        return new SourcePosition(line, column);
    }
}
