/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Works out where each section of a file came from, as lines and columns
 * ({@link SectionOrigin}), as the sections come from the splitter. Splitters
 * deal in bytes and don't count lines, so the pipeline does it here, once for
 * every splitter.
 * <p>
 * A section's origin is the start of its first range copied from the file:
 * its position in the section, after any text the splitter put before it
 * (such as an XML declaration), and its position in the file. The file is
 * read once more for this, from its start up to the last section's range, a
 * block at a time; nothing is kept but the count. Sections come in file
 * order, so each is counted on from where the last one was. A section before
 * the count, which no built-in splitter makes, is counted from the file's
 * start again.
 * <p>
 * Lines and columns are counted as {@link LineCounter} counts them: lines end
 * at LF, CRLF or CR, and columns are characters of UTF-8.
 * <p>
 * One instance serves one file, on one thread.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SectionOrigins
{

    /** How many bytes of the file are read at a time. */
    private static final int BLOCK = 64 * 1024;

    /** The file the sections come from. */
    private final SeekableByteSource source;

    /** The count of the file's lines and columns, up to {@link #counted}. */
    private LineCounter counter = new LineCounter();

    /** How many bytes of the file have been counted, from its start. */
    private long counted;

    /** The block the file is read into, made on first use. */
    private byte[] block;

    /**
     * Starts counting a file from its start.
     *
     * @param source the file the sections come from
     */
    SectionOrigins(SeekableByteSource source)
    {
        this.source = source;
    }

    /**
     * Gives a section its origin.
     *
     * @param section a section of the file, as the splitter made it
     * @return the same section with its origin
     * @throws IOException if the file can't be read
     */
    Section withOrigin(Section section) throws IOException
    {
        LineCounter added = new LineCounter();
        long fileOffset = section.sourceOffset();
        for (Section.Part part : section.parts())
        {
            if (part instanceof Section.SourceRange range)
            {
                fileOffset = range.offset();
                break;
            }
            // Text the splitter added, before anything from the file.
            byte[] bytes = ((Section.Literal) part).bytes();
            added.add(bytes, 0, bytes.length);
        }
        return section.withOrigin(new SectionOrigin(added.position(),
                positionInFile(fileOffset)));
    }

    /**
     * The line and column of a byte of the file, counting on from the last
     * one asked for.
     *
     * @param offset the byte's index in the file
     * @return its position
     * @throws IOException if the file can't be read, or ends before the
     *                     offset
     */
    private SourcePosition positionInFile(long offset) throws IOException
    {
        if (offset < counted)
        {
            counter = new LineCounter();
            counted = 0;
        }
        if (block == null)
        {
            block = new byte[BLOCK];
        }
        while (counted < offset)
        {
            int length = (int) Math.min(BLOCK, offset - counted);
            ByteBuffer buffer = ByteBuffer.wrap(block, 0, length);
            int read = source.read(counted, buffer);
            if (read <= 0)
            {
                throw new IOException("The file ended before a section");
            }
            counter.add(block, 0, read);
            counted += read;
        }
        return counter.position();
    }
}
