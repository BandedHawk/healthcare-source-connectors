/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.position;

import java.util.Objects;

/**
 * Where a section's text came from in its file, so that a position in the
 * section can be turned into a position in the file
 * ({@link #toFile(SourcePosition)}).
 * <p>
 * A section is mostly a copy of part of the file, but a splitter can add text
 * of its own before that part: the XML splitter starts each section with the
 * file's XML declaration on a line of its own, and the X12 and EDIFACT
 * splitters add envelope segments. So the origin pairs two positions of the
 * same character, the first one copied from the file: where it is in the
 * section, and where it is in the file. For a section that is the file's text
 * from its start, both are line 1, column 1.
 * <p>
 * Positions in the section are what a converter reports (such as the
 * {@code #line} and {@code #col} keys of C-CDA JSON) and what a mapping
 * reports in its issues; the file positions are what an operator needs to
 * find the problem in the file as it was received.
 *
 * @param inSection where the copied text starts in the section
 * @param inFile    where the same text starts in the file
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SectionOrigin(SourcePosition inSection, SourcePosition inFile)
{

    /**
     * The origin of a section that is a file's text from its start, with
     * nothing added: every position is the same in both.
     */
    public static final SectionOrigin WHOLE_FILE = new SectionOrigin(
            SourcePosition.START, SourcePosition.START);

    /**
     * Checks that both positions are given.
     *
     * @throws NullPointerException if either is null
     */
    public SectionOrigin
    {
        Objects.requireNonNull(inSection, "inSection");
        Objects.requireNonNull(inFile, "inFile");
    }

    /**
     * Turns a position in the section into the position of the same character
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
     * @param inSection a position in the section
     * @return the position in the file
     */
    public SourcePosition toFile(SourcePosition inSection)
    {
        if (inSection.isBefore(this.inSection))
        {
            return inFile;
        }
        long line = inFile.line() + inSection.line() - this.inSection.line();
        int column = inSection.line() == this.inSection.line()
                ? inFile.column() + inSection.column() - this.inSection.column()
                : inSection.column();
        return new SourcePosition(line, column);
    }
}
