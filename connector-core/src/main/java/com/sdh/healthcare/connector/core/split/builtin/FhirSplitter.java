/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.core.split.ByteScanner;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.split.RecordSplitter;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitSession;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * FHIR JSON splitter (design §6.2). Accepts a single resource, a JSON array
 * of resources, or a stream of resources separated by whitespace (NDJSON).
 * Each top-level resource is one record, and its {@code resourceType} must
 * match the boundary (default {@code Bundle}; {@code *} accepts any).
 * <p>
 * {@code fhir.ndjson-group-size} = N &gt; 0 groups N resources per record,
 * for bulk-export NDJSON. The boundary then defaults to {@code *}. Grouped
 * records from an array are wrapped in {@code [ ]}.
 * <p>
 * Uses a byte-level structural scanner rather than a full JSON parser: it
 * checks strings, escapes and bracket nesting, which is enough to find
 * exact byte boundaries and to restart mid-array. Full JSON validation is
 * left to downstream.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FhirSplitter implements RecordSplitter
{

    /** The format's name. */
    public static final String FORMAT = "fhir";

    /** The key that names a resource's type. */
    private static final byte[] RESOURCE_TYPE =
            "resourceType".getBytes(UTF_8);

    /** The deepest nesting allowed. */
    private static final int MAX_DEPTH = 4096;

    /**
     * Creates the splitter; the service loader calls this.
     */
    public FhirSplitter()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String format()
    {
        return FORMAT;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the group size is negative
     */
    @Override
    public void validate(SplitConfig config)
    {
        if (config.intOption("fhir.ndjson-group-size", 0) < 0)
        {
            throw new IllegalArgumentException(
                    "fhir.ndjson-group-size must be 0 or more");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the group size is negative
     */
    @Override
    public SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config)
            throws IOException, SplitException
    {
        validate(config);
        int group = config.intOption("fhir.ndjson-group-size", 0);
        String boundary = config.boundaryOr(group > 0 ? "*" : "Bundle");

        ByteScanner head = new ByteScanner(source, 0, 64);
        if (head.peek() == 0xEF && head.peek(1) == 0xBB
                && head.peek(2) == 0xBF)
        {
            head.skip(3);
        }
        skipWhitespace(head);
        int first = head.peek();
        if (first < 0)
        {
            return Optional::empty;
        }
        if (first != '[' && first != '{')
        {
            throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                    head.position());
        }
        boolean array = first == '[';
        ByteScanner scanner;
        State state;
        if (from.offset() == 0)
        {
            scanner = new ByteScanner(source,
                    head.position() + (array ? 1 : 0),
                    config.readBufferBytes());
            state = array ? State.ARRAY_FIRST : State.STREAM;
        }
        else
        {
            scanner = new ByteScanner(source, from.offset(),
                    config.readBufferBytes());
            state = array ? State.ARRAY_NEXT : State.STREAM;
        }
        return new Session(scanner, state, Math.max(1, group), boundary,
                config.limits().maxRecordBytes());
    }

    /** Where a session is in the file. */
    enum State
    {
        /** Inside an array, before its first value. */
        ARRAY_FIRST,

        /** Inside an array, after a value. */
        ARRAY_NEXT,

        /** In a whitespace-separated stream of values. */
        STREAM,

        /** After the array; only whitespace may follow. */
        DONE
    }

    /**
     * Skips JSON whitespace.
     *
     * @param scanner reads the file
     * @throws IOException if the file can't be read
     */
    static void skipWhitespace(ByteScanner scanner) throws IOException
    {
        int next;
        while ((next = scanner.peek()) == ' ' || next == '\n'
                || next == '\r' || next == '\t')
        {
            scanner.read();
        }
    }

    /** Splits one file. */
    private static final class Session implements SplitSession
    {

        /** Reads the file. */
        private final ByteScanner scanner;

        /** How many resources go in one record. */
        private final int group;

        /** The resource type required; {@code *} for any. */
        private final String boundary;

        /** The largest record allowed, in bytes. */
        private final long max;

        /** Where the session is in the file. */
        private State state;

        /** The closing bracket expected at each nesting depth. */
        private byte[] stack = new byte[64];

        /**
         * Creates a session.
         *
         * @param scanner  reads the file
         * @param state    where it starts
         * @param group    how many resources go in one record
         * @param boundary the resource type required; {@code *} for any
         * @param max      the largest record allowed
         */
        Session(ByteScanner scanner, State state, int group, String boundary,
                long max)
        {
            this.scanner = scanner;
            this.state = state;
            this.group = group;
            this.boundary = boundary;
            this.max = max;
        }

        /**
         * Moves to the start of the next value.
         *
         * @return false if there is none
         * @throws IOException    if the file can't be read
         * @throws SplitException if the array is malformed, or something
         *                        follows it
         */
        private boolean toNextValue() throws IOException, SplitException
        {
            skipWhitespace(scanner);
            switch (state)
            {
                case ARRAY_FIRST ->
                {
                    if (scanner.peek() == ']')
                    {
                        scanner.read();
                        state = State.DONE;
                        return toNextValue();
                    }
                    state = State.ARRAY_NEXT;
                    return true;
                }
                case ARRAY_NEXT ->
                {
                    int next = scanner.read();
                    if (next == ']')
                    {
                        state = State.DONE;
                        return toNextValue();
                    }
                    if (next != ',')
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_JSON_MALFORMED,
                                scanner.position() - 1);
                    }
                    skipWhitespace(scanner);
                    return true;
                }
                case STREAM ->
                {
                    return scanner.peek() >= 0;
                }
                case DONE ->
                {
                    if (scanner.peek() >= 0)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_JSON_MALFORMED,
                                scanner.position());
                    }
                    return false;
                }
            }
            throw new IllegalStateException();
        }

        /**
         * Tells whether another value follows within the current group,
         * without consuming the closing bracket.
         *
         * @return true if one follows
         * @throws IOException if the file can't be read
         */
        private boolean moreInGroup() throws IOException
        {
            skipWhitespace(scanner);
            int next = scanner.peek();
            if (state == State.STREAM)
            {
                return next >= 0;
            }
            if (next == ',')
            {
                scanner.read();
                skipWhitespace(scanner);
                return true;
            }
            return false;
        }

        /**
         * {@inheritDoc}
         *
         * @throws SplitException if the JSON is malformed, a resource has
         *                        the wrong type, or a record is too large
         */
        @Override
        public Optional<SplitRecord> next() throws IOException, SplitException
        {
            if (!toNextValue())
            {
                return Optional.empty();
            }
            long start = scanner.position();
            long end = scanResource(start);
            for (int count = 1; count < group && moreInGroup(); count++)
            {
                end = scanResource(start);
            }
            ResumePoint resume = ResumePoint.at(end);
            if (group > 1 && state != State.STREAM)
            {
                return Optional.of(new SplitRecord(start, end - start, List.of(
                        new SplitRecord.Literal(new byte[] {'['}),
                        new SplitRecord.SourceRange(start, end - start),
                        new SplitRecord.Literal(new byte[] {']'})), resume));
            }
            return Optional.of(SplitRecord.ofRange(start, end - start, resume));
        }

        /**
         * Scans one top-level object and checks its resourceType.
         *
         * @param recordStart where the record began, for the size limit
         * @return the offset just after it
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is malformed, has the wrong type, or
         *                        makes the record too large
         */
        private long scanResource(long recordStart)
                throws IOException, SplitException
        {
            long start = scanner.position();
            if (scanner.read() != '{')
            {
                throw new SplitException(
                        ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE, start);
            }
            int depth = 1;
            stack[0] = '}';
            boolean expectKey = true;
            boolean keyIsType = false;
            boolean valueIsType = false;
            byte[] resourceType = null;
            while (true)
            {
                int next = scanner.read();
                if (next < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                switch (next)
                {
                    case '"' ->
                    {
                        if (depth == 1 && expectKey)
                        {
                            keyIsType = Arrays.equals(readString(start, 32),
                                    RESOURCE_TYPE);
                        }
                        else if (depth == 1 && valueIsType)
                        {
                            resourceType = readString(start, 256);
                            valueIsType = false;
                        }
                        else
                        {
                            skipString(start);
                        }
                    }
                    case '{', '[' ->
                    {
                        if (depth == MAX_DEPTH)
                        {
                            throw new SplitException(
                                    ErrorCode.PARSE_JSON_MALFORMED, start);
                        }
                        if (depth == stack.length)
                        {
                            stack = Arrays.copyOf(stack, depth * 2);
                        }
                        stack[depth++] = (byte) (next == '{' ? '}' : ']');
                        valueIsType = false;
                    }
                    case '}', ']' ->
                    {
                        if (stack[--depth] != next)
                        {
                            throw new SplitException(
                                    ErrorCode.PARSE_JSON_MALFORMED, start);
                        }
                        if (depth == 0)
                        {
                            String type = resourceType == null
                                    ? null : new String(resourceType, UTF_8);
                            if (!boundary.equals("*")
                                    && !boundary.equals(type))
                            {
                                throw new SplitException(ErrorCode
                                        .PARSE_FHIR_UNEXPECTED_RESOURCE,
                                        start);
                            }
                            return scanner.position();
                        }
                    }
                    case ',' ->
                    {
                        if (depth == 1)
                        {
                            expectKey = true;
                        }
                    }
                    case ':' ->
                    {
                        if (depth == 1)
                        {
                            expectKey = false;
                            valueIsType = keyIsType;
                            keyIsType = false;
                        }
                    }
                    case ' ', '\n', '\r', '\t' ->
                    {
                    }
                    default ->
                        valueIsType = false;
                }
                if (scanner.position() - recordStart > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
                            recordStart);
                }
            }
        }

        /**
         * After an opening quote: skips to just past the closing quote.
         *
         * @param start the resource's offset, for errors
         * @throws IOException    if the file can't be read
         * @throws SplitException if the string is never closed
         */
        private void skipString(long start)
                throws IOException, SplitException
        {
            while (true)
            {
                int found = scanner.skipToAny((byte) '"', (byte) '\\');
                if (found < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                scanner.read();
                if (found == '"')
                {
                    return;
                }
                if (scanner.read() < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
            }
        }

        /**
         * After an opening quote: reads a string's raw bytes.
         *
         * @param start the resource's offset, for errors
         * @param cap   the most bytes kept
         * @return the bytes; null if longer than {@code cap} or escaped
         * @throws IOException    if the file can't be read
         * @throws SplitException if the string is never closed
         */
        private byte[] readString(long start, int cap)
                throws IOException, SplitException
        {
            byte[] bytes = new byte[cap];
            int count = 0;
            boolean overflow = false;
            while (true)
            {
                int next = scanner.read();
                if (next < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                if (next == '"')
                {
                    return overflow ? null : Arrays.copyOf(bytes, count);
                }
                if (next == '\\')
                {
                    // Escaped names never match; keep scanning correctly.
                    overflow = true;
                    if (scanner.read() < 0)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_JSON_MALFORMED, start);
                    }
                    continue;
                }
                if (count == cap)
                {
                    overflow = true;
                }
                else
                {
                    bytes[count++] = (byte) next;
                }
            }
        }
    }
}
