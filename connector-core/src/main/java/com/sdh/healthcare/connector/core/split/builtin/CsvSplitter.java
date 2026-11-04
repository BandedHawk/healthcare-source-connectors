/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

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
import java.util.List;
import java.util.Optional;

/**
 * One section per CSV record (RFC 4180; design §6.2). Newlines inside
 * quoted fields are respected, and content is otherwise not interpreted.
 * <p>
 * Options: {@code csv.header} (default false) prepends the first record to
 * every section; {@code csv.delimiter} (default {@code ,}) is needed to
 * know where a quoted field may start. Empty lines are skipped. A quote
 * that doesn't open a field is kept as an ordinary character.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CsvSplitter implements SectionSplitter
{

    /** The format's name. */
    public static final String FORMAT = "csv";

    /**
     * Creates the splitter; the service loader calls this.
     */
    public CsvSplitter()
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
     * @throws IllegalArgumentException if the delimiter is invalid
     */
    @Override
    public void validate(SplitConfig config)
    {
        delimiter(config);
    }

    /**
     * The configured delimiter; also used by the CSV converter.
     *
     * @param config the split settings
     * @return the delimiter; {@code \t} is read as a tab
     * @throws IllegalArgumentException unless it is one ASCII character
     *                                  other than a quote or newline
     */
    public static char delimiter(SplitConfig config)
    {
        String delimiter = config.option("csv.delimiter", ",");
        if (delimiter.equals("\\t"))
        {
            delimiter = "\t";
        }
        if (delimiter.length() != 1 || delimiter.charAt(0) > 0x7f
                || delimiter.charAt(0) == '"' || delimiter.charAt(0) == '\n')
        {
            throw new IllegalArgumentException("csv.delimiter must be one"
                    + " ASCII character other than quote or newline");
        }
        return delimiter.charAt(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config)
            throws IOException, SplitException
    {
        byte delimiter = (byte) delimiter(config);
        long max = config.limits().maxSectionBytes();
        byte[] header = null;
        long start = from.offset();
        if (config.booleanOption("csv.header", false))
        {
            long[] headerRecord = nextRecord(new ByteScanner(source, 0,
                    Math.min(config.readBufferBytes(), 64 << 10)),
                    delimiter, max);
            if (headerRecord == null)
            {
                return Optional::empty;
            }
            header = source.readFully(headerRecord[0],
                    (int) (headerRecord[1] - headerRecord[0]));
            start = Math.max(start, headerRecord[1]);
        }
        ByteScanner scanner = new ByteScanner(source, start,
                config.readBufferBytes());
        byte[] headerBytes = header;
        return () ->
        {
            long[] record = nextRecord(scanner, delimiter, max);
            if (record == null)
            {
                return Optional.empty();
            }
            Section.SourceRange range = new Section.SourceRange(record[0],
                    record[1] - record[0]);
            List<Section.Part> parts = headerBytes == null
                    ? List.of(range)
                    : List.of(new Section.Literal(headerBytes), range);
            return Optional.of(new Section(record[0], record[1] - record[0],
                    parts, ResumePoint.at(record[1])));
        };
    }

    /**
     * The next non-empty record.
     *
     * @param scanner   reads the file
     * @param delimiter the field delimiter
     * @param max       the largest record allowed, in bytes
     * @return {start, end}, end being after its line terminator; null at
     *         the end
     * @throws IOException    if the file can't be read
     * @throws SplitException if a record is too large, or a quote is never
     *                        closed
     */
    static long[] nextRecord(ByteScanner scanner, byte delimiter, long max)
            throws IOException, SplitException
    {
        while (true)
        {
            long start = scanner.position();
            int next = scanner.read();
            if (next < 0)
            {
                return null;
            }
            boolean inQuotes = false;
            boolean fieldStart = true;
            boolean empty = true;
            while (true)
            {
                if (inQuotes)
                {
                    if (next == '"')
                    {
                        if (scanner.peek() == '"')
                        {
                            scanner.read();
                        }
                        else
                        {
                            inQuotes = false;
                        }
                    }
                    empty = false;
                }
                else if (next == '\n')
                {
                    break;
                }
                else if (next == delimiter)
                {
                    fieldStart = true;
                    empty = false;
                }
                else
                {
                    if (next == '"' && fieldStart)
                    {
                        inQuotes = true;
                    }
                    if (next != '\r')
                    {
                        empty = false;
                    }
                    fieldStart = false;
                }
                if (scanner.position() - start > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            start);
                }
                next = scanner.read();
                if (next < 0)
                {
                    if (inQuotes)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNTERMINATED_QUOTE, start);
                    }
                    break;
                }
            }
            if (!empty)
            {
                return new long[]
                {
                    start, scanner.position()
                };
            }
        }
    }
}
