/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;

/**
 * Splitters by format name, loaded with {@link ServiceLoader}. Also the
 * place where {@link SplitLimits} are enforced.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitterRegistry
{

    /** The splitters, by format name. */
    private final Map<String, RecordSplitter> byFormat;

    /**
     * Creates a registry.
     *
     * @param byFormat the splitters, by format name
     */
    private SplitterRegistry(Map<String, RecordSplitter> byFormat)
    {
        this.byFormat = Collections.unmodifiableMap(byFormat);
    }

    /**
     * Loads the splitters on this class's class path.
     *
     * @return the registry
     * @throws IllegalStateException if two splitters share a format
     */
    public static SplitterRegistry load()
    {
        return load(SplitterRegistry.class.getClassLoader());
    }

    /**
     * Loads the splitters a class loader can see.
     *
     * @param classLoader the class loader
     * @return the registry
     * @throws IllegalStateException if two splitters share a format
     */
    public static SplitterRegistry load(ClassLoader classLoader)
    {
        Map<String, RecordSplitter> splitters = new TreeMap<>();
        for (RecordSplitter splitter
                : ServiceLoader.load(RecordSplitter.class, classLoader))
        {
            add(splitters, splitter);
        }
        return new SplitterRegistry(splitters);
    }

    /**
     * A registry of the splitters given.
     *
     * @param splitters the splitters
     * @return the registry
     * @throws IllegalStateException if two splitters share a format
     */
    public static SplitterRegistry of(RecordSplitter... splitters)
    {
        Map<String, RecordSplitter> byName = new TreeMap<>();
        for (RecordSplitter splitter : splitters)
        {
            add(byName, splitter);
        }
        return new SplitterRegistry(byName);
    }

    /**
     * Adds a splitter under its format.
     *
     * @param splitters the splitters so far
     * @param splitter  the splitter to add
     * @throws IllegalStateException if its format already has one
     */
    private static void add(Map<String, RecordSplitter> splitters,
            RecordSplitter splitter)
    {
        RecordSplitter previous =
                splitters.putIfAbsent(splitter.format(), splitter);
        if (previous != null)
        {
            throw new IllegalStateException("Two splitters for format '"
                    + splitter.format() + "': "
                    + previous.getClass().getName() + " and "
                    + splitter.getClass().getName());
        }
    }

    /**
     * The formats there are splitters for.
     *
     * @return the format names
     */
    public Set<String> formats()
    {
        return byFormat.keySet();
    }

    /**
     * The splitter for a format.
     *
     * @param format the format's name
     * @return the splitter
     * @throws IllegalArgumentException if there is none
     */
    public RecordSplitter get(String format)
    {
        RecordSplitter splitter = byFormat.get(format);
        if (splitter == null)
        {
            throw new IllegalArgumentException("Unknown split format '"
                    + format + "'; known: " + formats());
        }
        return splitter;
    }

    /**
     * Opens a session that also enforces {@link SplitLimits} (design
     * §6.3), and the rules for an item without records (STAGES.md §16,
     * {@link WholeItemSession}).
     *
     * @param format  the format's name
     * @param source  the file
     * @param from    where to start
     * @param nextSeq the seq the next record will get; used for the
     *                per-file record count
     * @param config  the split settings
     * @return the session
     * @throws IOException    if the file can't be read
     * @throws SplitException if the file is too large, empty or blank, or
     *                        can't be split from there
     */
    public SplitSession open(String format, SeekableByteSource source,
            ResumePoint from, long nextSeq, SplitConfig config)
            throws IOException, SplitException
    {
        SplitLimits limits = config.limits();
        if (source.size() > limits.maxItemBytes())
        {
            throw new SplitException(ErrorCode.LIMIT_ITEM_SIZE, 0);
        }
        return new LimitedSession(WholeItemSession.open(get(format), source,
                from, config), limits, nextSeq, from);
    }

    /**
     * Opens a session over a message batch that splits each message on its
     * own (design §5.3.16), and enforces {@link SplitLimits} on the whole:
     * a message that can't be split, holds a record over the size limit,
     * or is empty or blank, fails alone. A batch without messages fails
     * as a whole ({@code SPLIT_EMPTY}).
     *
     * @param format  the format's name
     * @param source  the item
     * @param offsets where each message starts in the item
     * @param from    where to start
     * @param nextSeq the seq the next record will get; used for the
     *                per-file record count
     * @param config  the split settings
     * @return the session
     * @throws IOException    if the item can't be read
     * @throws SplitException if the item is too large, or has no messages
     */
    public SplitSession openMessages(String format, SeekableByteSource source,
            List<Long> offsets, ResumePoint from, long nextSeq,
            SplitConfig config) throws IOException, SplitException
    {
        SplitLimits limits = config.limits();
        if (source.size() > limits.maxItemBytes())
        {
            throw new SplitException(ErrorCode.LIMIT_ITEM_SIZE, 0);
        }
        if (offsets.isEmpty() && ResumePoint.START.equals(from))
        {
            throw new SplitException(ErrorCode.SPLIT_EMPTY, 0);
        }
        return new LimitedSession(new MessageSession(get(format), source,
                offsets, from, config), limits, nextSeq, from);
    }

    /**
     * A session that enforces the record limits, and checks the splitter
     * makes progress.
     */
    private static final class LimitedSession implements SplitSession
    {

        /** The splitter's session. */
        private final SplitSession inner;

        /** The limits. */
        private final SplitLimits limits;

        /** The seq the next record will get. */
        private long seq;

        /** The furthest resume point so far. */
        private ResumePoint lastResume;

        /**
         * Wraps a session.
         *
         * @param inner   the splitter's session
         * @param limits  the limits
         * @param nextSeq the seq the next record will get
         * @param from    where the session starts
         */
        LimitedSession(SplitSession inner, SplitLimits limits, long nextSeq,
                ResumePoint from)
        {
            this.inner = inner;
            this.limits = limits;
            this.seq = nextSeq;
            this.lastResume = from;
        }

        /**
         * {@inheritDoc}
         *
         * @throws SplitException also if a record is too large or there
         *                        are too many
         */
        @Override
        public Optional<SplitRecord> next() throws IOException, SplitException
        {
            Optional<SplitRecord> next = inner.next();
            if (next.isEmpty())
            {
                return next;
            }
            SplitRecord record = next.get();
            if (record.failure() != null)
            {
                // The rest of a message that couldn't be split: kept as it
                // is, so not size-checked, and may be empty when the error
                // came at the message's very end.
                if (seq >= limits.maxRecordsPerItem())
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_COUNT,
                            record.sourceOffset());
                }
                if (record.resume().isAfter(lastResume))
                {
                    lastResume = record.resume();
                }
                seq++;
                return next;
            }
            if (record.plaintextLength() > limits.maxRecordBytes())
            {
                throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
                        record.sourceOffset());
            }
            if (seq >= limits.maxRecordsPerItem())
            {
                throw new SplitException(ErrorCode.LIMIT_RECORD_COUNT,
                        record.sourceOffset());
            }
            if (!record.resume().isAfter(lastResume))
            {
                throw new IllegalStateException(
                        "Splitter made no progress at offset "
                                + lastResume.offset());
            }
            lastResume = record.resume();
            seq++;
            return next;
        }

        /**
         * Closes the splitter's session.
         */
        @Override
        public void close()
        {
            inner.close();
        }
    }
}
