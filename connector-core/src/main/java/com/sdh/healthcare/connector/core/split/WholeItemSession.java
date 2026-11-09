/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * The split stage's rules for an item that gives no records (STAGES.md
 * §16), around a splitter's session, the same for every format:
 * <ul>
 * <li>an item of 0 bytes, or blank (only spaces, tabs, CR and LF, after a
 * UTF-8 byte-order mark), fails with {@code SPLIT_EMPTY} before the
 * splitter sees it;</li>
 * <li>an item the splitter read to its end without a record, and reports
 * as an empty container of its format
 * ({@link SplitSession#emptyContainer()}), fails with
 * {@code SPLIT_NO_RECORDS};</li>
 * <li>any other item without a record is taken as a single document: the
 * whole item becomes one record, for the stages after to judge.</li>
 * </ul>
 * The rules apply only to a session started at the item's start, since a
 * resume point always comes after a record. For a message batch, each
 * message is an item here (design §5.3.16).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class WholeItemSession implements SplitSession
{

    /** How many bytes the blank check reads at a time. */
    private static final int CHUNK = 64 * 1024;

    /** The splitter's session. */
    private final SplitSession inner;

    /** The item. */
    private final SeekableByteSource source;

    /** Whether the session started at the item's start. */
    private final boolean fromStart;

    /** Whether the splitter has given a record. */
    private boolean any;

    /** Whether the item is finished. */
    private boolean done;

    /**
     * Wraps a session.
     *
     * @param inner     the splitter's session
     * @param source    the item
     * @param fromStart whether it started at the item's start
     */
    private WholeItemSession(SplitSession inner, SeekableByteSource source,
            boolean fromStart)
    {
        this.inner = inner;
        this.source = source;
        this.fromStart = fromStart;
    }

    /**
     * Opens a splitter's session under these rules.
     *
     * @param splitter the splitter
     * @param source   the item
     * @param from     where to start
     * @param config   the split settings
     * @return the session
     * @throws IOException    if the item can't be read
     * @throws SplitException {@code SPLIT_EMPTY} if the item is empty or
     *                        blank, or the splitter's own error
     */
    static SplitSession open(RecordSplitter splitter,
            SeekableByteSource source, ResumePoint from, SplitConfig config)
            throws IOException, SplitException
    {
        boolean fromStart = ResumePoint.START.equals(from);
        if (fromStart && blank(source))
        {
            throw new SplitException(ErrorCode.SPLIT_EMPTY, 0);
        }
        return new WholeItemSession(splitter.open(source, from, config),
                source, fromStart);
    }

    /**
     * {@inheritDoc}
     *
     * @throws SplitException also {@code SPLIT_NO_RECORDS} for an empty
     *                        container
     */
    @Override
    public Optional<SplitRecord> next() throws IOException, SplitException
    {
        if (done)
        {
            return Optional.empty();
        }
        Optional<SplitRecord> next = inner.next();
        if (next.isPresent())
        {
            any = true;
            return next;
        }
        done = true;
        if (any || !fromStart)
        {
            return next;
        }
        if (inner.emptyContainer())
        {
            throw new SplitException(ErrorCode.SPLIT_NO_RECORDS, 0);
        }
        long size = source.size();
        return Optional.of(SplitRecord.ofRange(0, size,
                ResumePoint.at(size)));
    }

    /**
     * Closes the splitter's session.
     */
    @Override
    public void close()
    {
        inner.close();
    }

    /**
     * Tells whether an item is empty or blank: only spaces, tabs, CR and
     * LF, after a UTF-8 byte-order mark. Reads only as far as the first
     * other byte.
     *
     * @param source the item
     * @return true if it is
     * @throws IOException if it can't be read
     */
    static boolean blank(SeekableByteSource source) throws IOException
    {
        long size = source.size();
        long position = 0;
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(CHUNK,
                Math.max(size, 1)));
        boolean first = true;
        while (position < size)
        {
            buffer.clear();
            int read = source.read(position, buffer);
            if (read <= 0)
            {
                throw new IOException("The item ended before its size");
            }
            int index = 0;
            if (first && read >= 3 && (buffer.get(0) & 0xFF) == 0xEF
                    && (buffer.get(1) & 0xFF) == 0xBB
                    && (buffer.get(2) & 0xFF) == 0xBF)
            {
                index = 3;
            }
            first = false;
            for (; index < read; index++)
            {
                byte value = buffer.get(index);
                if (value != ' ' && value != '\t' && value != '\r'
                        && value != '\n')
                {
                    return false;
                }
            }
            position += read;
        }
        return true;
    }
}
