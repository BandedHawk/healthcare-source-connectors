/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

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
import java.util.List;
import java.util.Optional;

/**
 * One record per CSV row (RFC 4180; design §6.2). Newlines inside
 * quoted fields are respected, and content is otherwise not interpreted.
 * <p>
 * Options: {@code csv.header} (default false) prepends the first row to
 * every record; {@code csv.delimiter} (default {@code ,}) is needed to
 * know where a quoted field may start. Blank lines (only spaces, tabs and
 * CR) are skipped, unless they hold the delimiter. A quote
 * that doesn't open a field is kept as an ordinary character.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CsvSplitter implements RecordSplitter
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
        long max = config.limits().maxRecordBytes();
        byte[] header = null;
        long start = from.offset();
        if (config.booleanOption("csv.header", false))
        {
            long[] headerRow = nextRow(new ByteScanner(source, 0,
                    Math.min(config.readBufferBytes(), 64 << 10)),
                    delimiter, max);
            if (headerRow == null)
            {
                return Optional::empty;
            }
            header = source.readFully(headerRow[0],
                    (int) (headerRow[1] - headerRow[0]));
            start = Math.max(start, headerRow[1]);
        }
        ByteScanner scanner = new ByteScanner(source, start,
                config.readBufferBytes());
        byte[] headerBytes = header;
        return new SplitSession()
        {
            /**
             * {@inheritDoc}
             */
            @Override
            public Optional<SplitRecord> next()
                    throws IOException, SplitException
            {
                long[] row = nextRow(scanner, delimiter, max);
                if (row == null)
                {
                    return Optional.empty();
                }
                SplitRecord.SourceRange range = new SplitRecord.SourceRange(
                        row[0], row[1] - row[0]);
                List<SplitRecord.Part> parts = headerBytes == null
                        ? List.of(range)
                        : List.of(new SplitRecord.Literal(headerBytes),
                                range);
                return Optional.of(new SplitRecord(row[0], row[1] - row[0],
                        parts, ResumePoint.at(row[1])));
            }

            /**
             * {@inheritDoc}
             *
             * @return true for a header without rows
             */
            @Override
            public boolean emptyContainer()
            {
                return headerBytes != null;
            }
        };
    }

    /**
     * {@inheritDoc}
     *
     * @return 2: blank lines are skipped, and a header without rows is an
     *         empty container (STAGES.md §16)
     */
    @Override
    public int version()
    {
        return 2;
    }

    /**
     * The next row that isn't blank: a line of only spaces, tabs and CR is
     * skipped, unless it holds the delimiter (a tab, say), which makes it a
     * row of empty fields.
     *
     * @param scanner   reads the file
     * @param delimiter the field delimiter
     * @param max       the largest row allowed, in bytes
     * @return {start, end}, end being after its line terminator; null at
     *         the end
     * @throws IOException    if the file can't be read
     * @throws SplitException if a row is too large, or a quote is never
     *                        closed
     */
    static long[] nextRow(ByteScanner scanner, byte delimiter, long max)
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
                    if (next != '\r' && next != ' ' && next != '\t')
                    {
                        empty = false;
                    }
                    fieldStart = false;
                }
                if (scanner.position() - start > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
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
