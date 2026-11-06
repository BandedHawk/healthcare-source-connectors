/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Works out where each record of a file came from, as lines and columns
 * ({@link RecordOrigin}), as the records come from the splitter. Splitters
 * deal in bytes and don't count lines, so the pipeline does it here, once for
 * every splitter.
 * <p>
 * A record's origin is the start of its first range copied from the file:
 * its position in the record, after any text the splitter put before it
 * (such as an XML declaration), and its position in the file. The file is
 * read once more for this, from its start up to the last record's range, a
 * block at a time; nothing is kept but counts. Records come in file
 * order, so each is counted on from where the last one was. A record before
 * the count, which no built-in splitter makes, is counted from the file's
 * start again.
 * <p>
 * Counting the whole file on the split thread would slow the splitting of
 * large records, so a background thread counts the file ahead of it and
 * leaves a mark at the end of each block: the count so far. The split thread
 * jumps to the last mark before a record and counts only from there. The
 * marks are kept for at most {@link #MARKS_AHEAD} blocks ahead. When the
 * background thread is behind, or has stopped, the split thread counts by
 * itself, so the positions are the same either way.
 * <p>
 * Lines and columns are counted as {@link LineCounter} counts them: lines end
 * at LF, CRLF or CR, and columns are characters of UTF-8.
 * <p>
 * A splitter whose records aren't text of the file, such as the rows of
 * a Parquet file, gives each record its origin itself; that origin is
 * kept, and the file is counted only once a record without one comes.
 * <p>
 * One instance serves one file, on one thread; close it when the file is
 * split.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RecordOrigins implements AutoCloseable
{

    /** How many bytes of the file are read at a time. */
    private static final int BLOCK = 64 * 1024;

    /** How many marks the background count may make ahead: 64 MiB. */
    private static final int MARKS_AHEAD = 1024;

    /** How long the background count waits for room for a mark. */
    private static final long WAIT_MILLIS = 50;

    /** The file the records come from. */
    private final SeekableByteSource source;

    /**
     * The background count's marks, in file order: each a counter at the
     * end of a block.
     */
    private final BlockingQueue<LineCounter> marks =
            new ArrayBlockingQueue<>(MARKS_AHEAD);

    /** Whether the file is split, so the background count should stop. */
    private volatile boolean closed;

    /** The count of the file's lines and columns, up to where it is. */
    private LineCounter counter = new LineCounter();

    /** A mark taken from the queue that is past the last record asked for. */
    private LineCounter nextMark;

    /** Whether the background count has been started. */
    private boolean counting;

    /** The block the file is read into, made on first use. */
    private byte[] block;

    /**
     * Prepares to count a file from its start. The background count starts
     * with the first record that needs it.
     *
     * @param source the file the records come from; read from two threads
     *               at once
     */
    RecordOrigins(SeekableByteSource source)
    {
        this.source = source;
    }

    /**
     * Gives a record its origin.
     *
     * @param record a record of the file, as the splitter made it
     * @return the same record with its origin; the splitter's own, if it
     *         gave one
     * @throws IOException if the file can't be read
     */
    SplitRecord withOrigin(SplitRecord record) throws IOException
    {
        if (record.origin() != null)
        {
            return record;
        }
        if (!counting)
        {
            counting = true;
            Thread.ofPlatform().name("sdh-lines").daemon(true)
                    .start(this::countAhead);
        }
        LineCounter added = new LineCounter();
        long fileOffset = record.sourceOffset();
        for (SplitRecord.Part part : record.parts())
        {
            if (part instanceof SplitRecord.SourceRange range)
            {
                fileOffset = range.offset();
                break;
            }
            // Text the splitter added, before anything from the file.
            byte[] bytes = ((SplitRecord.Literal) part).bytes();
            added.add(bytes, 0, bytes.length);
        }
        return record.withOrigin(new RecordOrigin(added.position(),
                positionInItem(fileOffset)));
    }

    /** Stops the background count. */
    @Override
    public void close()
    {
        // Not an interrupt: it would close a file channel both threads read.
        closed = true;
        marks.clear();
    }

    /**
     * The line and column of a byte of the file, counting on from the last
     * one asked for, or from the last mark before it.
     *
     * @param offset the byte's index in the file
     * @return its position
     * @throws IOException if the file can't be read, or ends before the
     *                     offset
     */
    private SourcePosition positionInItem(long offset) throws IOException
    {
        if (offset < counter.count())
        {
            counter = new LineCounter();
        }
        jumpToLastMark(offset);
        if (block == null)
        {
            block = new byte[BLOCK];
        }
        while (counter.count() < offset)
        {
            long counted = counter.count();
            int length = (int) Math.min(BLOCK, offset - counted);
            int read = source.read(counted, ByteBuffer.wrap(block, 0, length));
            if (read <= 0)
            {
                throw new IOException("The file ended before a record");
            }
            counter.add(block, 0, read);
        }
        return counter.position();
    }

    /**
     * Moves the count on to the background count's last mark at or before a
     * byte, if it has made one past the count. Marks before it are dropped.
     *
     * @param offset the byte's index in the file
     */
    private void jumpToLastMark(long offset)
    {
        while (true)
        {
            if (nextMark == null)
            {
                nextMark = marks.poll();
                if (nextMark == null)
                {
                    return;
                }
            }
            if (nextMark.count() > offset)
            {
                return;
            }
            if (nextMark.count() > counter.count())
            {
                counter = nextMark;
            }
            nextMark = null;
        }
    }

    /**
     * Counts the file from its start on the background thread, making a
     * mark after each block, until the file ends or the count is closed. If
     * the file can't be read, it stops, and the split thread counts the rest
     * by itself.
     */
    private void countAhead()
    {
        LineCounter ahead = new LineCounter();
        byte[] buffer = new byte[BLOCK];
        try
        {
            while (!closed)
            {
                int read = source.read(ahead.count(), ByteBuffer.wrap(buffer));
                if (read <= 0)
                {
                    return;
                }
                ahead.add(buffer, 0, read);
                LineCounter mark = ahead.copy();
                while (!closed && !marks.offer(mark, WAIT_MILLIS,
                        TimeUnit.MILLISECONDS))
                {
                    // The split thread is a long way behind: wait for it.
                }
            }
        }
        catch (IOException | InterruptedException stopped)
        {
            // The split thread counts by itself from the last mark.
        }
    }
}
