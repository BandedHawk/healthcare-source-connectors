package com.sdh.healthcare.connector.core.split.builtin;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.core.split.ByteScanner;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SectionSplitter;
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
 * FHIR JSON splitter (design §6.2). Accepts a single resource, a JSON array of
 * resources, or a stream of resources separated by whitespace (NDJSON). Each
 * top-level resource is one section, and its {@code resourceType} must match
 * the boundary (default {@code Bundle}; {@code *} accepts any).
 *
 * <p>
 * {@code fhir.ndjson-group-size} = N &gt; 0 groups N resources per section, for
 * bulk-export NDJSON. The boundary then defaults to {@code *}. Grouped sections
 * from an array are wrapped in {@code [ ]}.
 *
 * <p>
 * Uses a byte-level structural scanner rather than a full JSON parser: it
 * checks strings, escapes and bracket nesting, which is enough to find exact
 * byte boundaries and to restart mid-array. Full JSON validation is left to
 * downstream.
 */
public final class FhirSplitter implements SectionSplitter
{

    public static final String FORMAT = "fhir";
    private static final byte[] RESOURCE_TYPE = "resourceType".getBytes(UTF_8);
    private static final int MAX_DEPTH = 4096;

    @Override
    public String format()
    {
        return FORMAT;
    }

    @Override
    public void validate(SplitConfig cfg)
    {
        if (cfg.intOption("fhir.ndjson-group-size", 0) < 0)
        {
            throw new IllegalArgumentException(
                    "fhir.ndjson-group-size must be 0 or more");
        }
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from,
            SplitConfig cfg)
            throws IOException, SplitException
    {
        validate(cfg);
        int group = cfg.intOption("fhir.ndjson-group-size", 0);
        String boundary = cfg.boundaryOr(group > 0 ? "*" : "Bundle");

        ByteScanner head = new ByteScanner(src, 0, 64);
        if (head.peek() == 0xEF && head.peek(1) == 0xBB && head.peek(2) == 0xBF)
        {
            head.skip(3);
        }
        skipWs(head);
        int first = head.peek();
        if (first < 0)
        {
            return Optional::empty;
        }
        if (first != '[' && first != '{')
        {
            throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED, head.
                    position());
        }
        boolean array = first == '[';
        ByteScanner sc;
        State state;
        if (from.offset() == 0)
        {
            sc = new ByteScanner(src, head.position() + (array ? 1 : 0), cfg.
                    readBufferBytes());
            state = array ? State.ARRAY_FIRST : State.STREAM;
        } else
        {
            sc = new ByteScanner(src, from.offset(), cfg.readBufferBytes());
            state = array ? State.ARRAY_NEXT : State.STREAM;
        }
        return new Session(sc, state, Math.max(1, group), boundary,
                cfg.limits().maxSectionBytes());
    }

    enum State
    {
        ARRAY_FIRST, ARRAY_NEXT, STREAM, DONE
    }

    static void skipWs(ByteScanner sc) throws IOException
    {
        int c;
        while ((c = sc.peek()) == ' ' || c == '\n' || c == '\r' || c == '\t')
        {
            sc.read();
        }
    }

    private static final class Session implements SplitSession
    {

        private final ByteScanner sc;
        private final int group;
        private final String boundary;
        private final long max;
        private State state;
        private byte[] stack = new byte[64];

        Session(ByteScanner sc, State state, int group, String boundary,
                long max)
        {
            this.sc = sc;
            this.state = state;
            this.group = group;
            this.boundary = boundary;
            this.max = max;
        }

        /**
         * Moves to the start of the next value, or returns false if there is
         * none.
         */
        private boolean toNextValue() throws IOException, SplitException
        {
            skipWs(sc);
            switch (state)
            {
                case ARRAY_FIRST ->
                {
                    if (sc.peek() == ']')
                    {
                        sc.read();
                        state = State.DONE;
                        return toNextValue();
                    }
                    state = State.ARRAY_NEXT;
                    return true;
                }
                case ARRAY_NEXT ->
                {
                    int c = sc.read();
                    if (c == ']')
                    {
                        state = State.DONE;
                        return toNextValue();
                    }
                    if (c != ',')
                    {
                        throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                                sc.position() - 1);
                    }
                    skipWs(sc);
                    return true;
                }
                case STREAM ->
                {
                    return sc.peek() >= 0;
                }
                case DONE ->
                {
                    if (sc.peek() >= 0)
                    {
                        throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                                sc.position());
                    }
                    return false;
                }
            }
            throw new IllegalStateException();
        }

        /**
         * Peeks whether another value follows within the current group, without
         * consuming the closing bracket.
         */
        private boolean moreInGroup() throws IOException, SplitException
        {
            skipWs(sc);
            int c = sc.peek();
            if (state == State.STREAM)
            {
                return c >= 0;
            }
            if (c == ',')
            {
                sc.read();
                skipWs(sc);
                return true;
            }
            return false;
        }

        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            if (!toNextValue())
            {
                return Optional.empty();
            }
            long start = sc.position();
            long end = scanResource(start);
            for (int n = 1; n < group && moreInGroup(); n++)
            {
                end = scanResource(start);
            }
            ResumePoint resume = ResumePoint.at(end);
            if (group > 1 && state != State.STREAM)
            {
                return Optional.of(new Section(start, end - start, List.of(
                        new Section.Literal(new byte[]
                        {
                            '['
                }),
                        new Section.SourceRange(start, end - start),
                        new Section.Literal(new byte[]
                        {
                            ']'
                })), resume));
            }
            return Optional.of(Section.ofRange(start, end - start, resume));
        }

        /**
         * Scans one top-level object, checks its resourceType, and returns the
         * offset just after it.
         */
        private long scanResource(long sectionStart) throws IOException,
                SplitException
        {
            long start = sc.position();
            if (sc.read() != '{')
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
                int c = sc.read();
                if (c < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                switch (c)
                {
                    case '"' ->
                    {
                        if (depth == 1 && expectKey)
                        {
                            keyIsType = Arrays.equals(readString(start, 32),
                                    RESOURCE_TYPE);
                        } else if (depth == 1 && valueIsType)
                        {
                            resourceType = readString(start, 256);
                            valueIsType = false;
                        } else
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
                        stack[depth++] = (byte) (c == '{' ? '}' : ']');
                        valueIsType = false;
                    }
                    case '}', ']' ->
                    {
                        if (stack[--depth] != c)
                        {
                            throw new SplitException(
                                    ErrorCode.PARSE_JSON_MALFORMED, start);
                        }
                        if (depth == 0)
                        {
                            String type = resourceType == null ? null : new String(
                                    resourceType, UTF_8);
                            if (!boundary.equals("*") && !boundary.equals(type))
                            {
                                throw new SplitException(
                                        ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE,
                                        start);
                            }
                            return sc.position();
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
                if (sc.position() - sectionStart > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            sectionStart);
                }
            }
        }

        /**
         * After an opening quote: skips to just past the closing quote.
         */
        private void skipString(long start) throws IOException, SplitException
        {
            while (true)
            {
                int c = sc.skipToAny((byte) '"', (byte) '\\');
                if (c < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                sc.read();
                if (c == '"')
                {
                    return;
                }
                if (sc.read() < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
            }
        }

        /**
         * After an opening quote: reads a string's raw bytes, keeping at most
         * {@code cap} (null if longer).
         */
        private byte[] readString(long start, int cap) throws IOException,
                SplitException
        {
            byte[] b = new byte[cap];
            int n = 0;
            boolean overflow = false;
            while (true)
            {
                int c = sc.read();
                if (c < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                            start);
                }
                if (c == '"')
                {
                    return overflow ? null : Arrays.copyOf(b, n);
                }
                if (c == '\\')
                {
                    overflow = true; // escaped names never match; keep scanning correctly
                    if (sc.read() < 0)
                    {
                        throw new SplitException(ErrorCode.PARSE_JSON_MALFORMED,
                                start);
                    }
                    continue;
                }
                if (n == cap)
                {
                    overflow = true;
                } else
                {
                    b[n++] = (byte) c;
                }
            }
        }
    }
}
