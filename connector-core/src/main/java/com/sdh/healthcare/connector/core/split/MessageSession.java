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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Splits a message batch message by message (design §5.3.16). Each message
 * is split on its own, by the intake's splitter, as if it were a file; the
 * records' offsets and resume points are then moved to where the message
 * sits in the item, so resuming, origins and storage work as for a file.
 * <p>
 * A message that can't be split fails alone: the rest of it, from where
 * its last good record ended, becomes one record marked with the error
 * ({@link SplitRecord.Failure}), and the next message carries on. A record
 * larger than the limit fails its message the same way.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class MessageSession implements SplitSession
{

    /** Splits each message. */
    private final RecordSplitter splitter;

    /** The whole item. */
    private final SeekableByteSource source;

    /** Where each message starts in the item. */
    private final List<Long> offsets;

    /** The item's size. */
    private final long size;

    /** The split settings. */
    private final SplitConfig config;

    /** The message being split; equal to the count once all are done. */
    private int message;

    /** Where splitting the current message has got to, in the item. */
    private long position;

    /** The current message's session; null between messages. */
    private SplitSession inner;

    /** Where the current message starts, for its session's offsets. */
    private long base;

    /** Where to resume within the first message; null to start it anew. */
    private ResumePoint resumeWithin;

    /**
     * Opens a session at a resume point in item coordinates.
     *
     * @param splitter splits each message
     * @param source   the whole item
     * @param offsets  where each message starts
     * @param from     where to resume, in the item
     * @param config   the split settings
     * @throws IOException if the item's size can't be read
     */
    MessageSession(RecordSplitter splitter, SeekableByteSource source,
            List<Long> offsets, ResumePoint from, SplitConfig config)
            throws IOException
    {
        this.splitter = splitter;
        this.source = source;
        this.offsets = List.copyOf(offsets);
        this.size = source.size();
        this.config = config;
        this.position = from.offset();
        this.message = messageAt(from.offset());
        if (message < this.offsets.size()
                && from.offset() > this.offsets.get(message))
        {
            long start = this.offsets.get(message);
            List<Long> context = new ArrayList<>();
            for (long offset : from.context())
            {
                context.add(offset - start);
            }
            resumeWithin = new ResumePoint(from.offset() - start, context);
        }
    }

    @Override
    public Optional<SplitRecord> next() throws IOException
    {
        while (message < offsets.size())
        {
            long end = end(message);
            try
            {
                if (inner == null)
                {
                    base = offsets.get(message);
                    if (end == base)
                    {
                        message++;
                        continue;
                    }
                    ResumePoint from = resumeWithin == null
                            ? ResumePoint.START : resumeWithin;
                    resumeWithin = null;
                    position = base + from.offset();
                    open(from);
                }
                Optional<SplitRecord> next = inner.next();
                if (next.isEmpty())
                {
                    finishMessage();
                    continue;
                }
                SplitRecord record = moved(next.get());
                if (record.plaintextLength()
                        > config.limits().maxRecordBytes())
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
                            record.sourceOffset() - base);
                }
                position = record.resume().offset();
                return Optional.of(record);
            }
            catch (SplitException failure)
            {
                long at = position;
                SplitRecord rest = new SplitRecord(at, end - at,
                        end > at ? List.of(new SplitRecord.SourceRange(at,
                                end - at)) : List.of(),
                        ResumePoint.at(end), null,
                        new SplitRecord.Failure(failure.code(),
                                Math.min(end, base + failure.offset())));
                finishMessage();
                position = end;
                return Optional.of(rest);
            }
        }
        return Optional.empty();
    }

    @Override
    public void close()
    {
        if (inner != null)
        {
            inner.close();
            inner = null;
        }
    }

    /**
     * Opens the current message's session.
     *
     * @param from where to start, in the message
     * @throws IOException    if the item can't be read
     * @throws SplitException if the message can't be split from there
     */
    private void open(ResumePoint from) throws IOException, SplitException
    {
        inner = splitter.open(new Slice(source, base, end(message) - base),
                from, config);
    }

    /** Closes the current message's session and moves to the next. */
    private void finishMessage()
    {
        close();
        message++;
    }

    /**
     * Where a message ends.
     *
     * @param index the message
     * @return the offset just after it
     */
    private long end(int index)
    {
        return index + 1 < offsets.size() ? offsets.get(index + 1) : size;
    }

    /**
     * The message an offset falls in. An offset at a message's start is
     * that message, skipping empty ones.
     *
     * @param offset the offset
     * @return the message; the count if the offset is at the end
     */
    private int messageAt(long offset)
    {
        for (int index = 0; index < offsets.size(); index++)
        {
            if (offset < end(index))
            {
                return index;
            }
        }
        return offsets.size();
    }

    /**
     * A record of the current message, moved to where it is in the item.
     *
     * @param record the record, in message coordinates
     * @return the record, in item coordinates
     */
    private SplitRecord moved(SplitRecord record)
    {
        List<SplitRecord.Part> parts = new ArrayList<>();
        for (SplitRecord.Part part : record.parts())
        {
            parts.add(part instanceof SplitRecord.SourceRange range
                    ? new SplitRecord.SourceRange(range.offset() + base,
                            range.length())
                    : part);
        }
        List<Long> context = new ArrayList<>();
        for (long offset : record.resume().context())
        {
            context.add(offset + base);
        }
        return new SplitRecord(record.sourceOffset() + base,
                record.sourceLength(), parts,
                new ResumePoint(record.resume().offset() + base, context));
    }

    /** A message's bytes: a window onto the item. */
    private static final class Slice implements SeekableByteSource
    {

        /** The item. */
        private final SeekableByteSource item;

        /** Where the message starts. */
        private final long start;

        /** The message's length. */
        private final long length;

        /**
         * Creates a window.
         *
         * @param item   the item
         * @param start  where the message starts
         * @param length its length
         */
        Slice(SeekableByteSource item, long start, long length)
        {
            this.item = item;
            this.start = start;
            this.length = length;
        }

        @Override
        public long size()
        {
            return length;
        }

        @Override
        public int read(long position, ByteBuffer destination)
                throws IOException
        {
            if (position >= length)
            {
                return -1;
            }
            int room = (int) Math.min(destination.remaining(),
                    length - position);
            ByteBuffer window = destination.duplicate();
            window.limit(window.position() + room);
            int read = item.read(start + position, window);
            if (read > 0)
            {
                destination.position(destination.position() + read);
            }
            return read;
        }

        /** The item stays open: it belongs to the caller. */
        @Override
        public void close()
        {
        }
    }
}
