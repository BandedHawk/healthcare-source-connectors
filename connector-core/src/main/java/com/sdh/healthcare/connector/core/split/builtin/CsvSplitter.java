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
 * One section per CSV record (RFC 4180; design §6.2). Newlines inside quoted fields are respected, and
 * content is otherwise not interpreted.
 *
 * <p>Options: {@code csv.header} (default false) prepends the first record to every section;
 * {@code csv.delimiter} (default {@code ,}) is needed to know where a quoted field may start.
 * Empty lines are skipped. A quote that doesn't open a field is kept as an ordinary character.
 */
public final class CsvSplitter implements SectionSplitter {
    public static final String FORMAT = "csv";

    @Override
    public String format() {
        return FORMAT;
    }

    @Override
    public void validate(SplitConfig cfg) {
        delimiter(cfg);
    }

    private static byte delimiter(SplitConfig cfg) {
        String d = cfg.option("csv.delimiter", ",");
        if (d.equals("\\t")) {
            d = "\t";
        }
        if (d.length() != 1 || d.charAt(0) > 0x7f || d.charAt(0) == '"' || d.charAt(0) == '\n') {
            throw new IllegalArgumentException("csv.delimiter must be one ASCII character other than quote or newline");
        }
        return (byte) d.charAt(0);
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from, SplitConfig cfg)
            throws IOException, SplitException {
        byte delim = delimiter(cfg);
        long max = cfg.limits().maxSectionBytes();
        byte[] header = null;
        long start = from.offset();
        if (cfg.booleanOption("csv.header", false)) {
            long[] h = nextRecord(new ByteScanner(src, 0, Math.min(cfg.readBufferBytes(), 64 << 10)), delim, max);
            if (h == null) {
                return Optional::empty;
            }
            header = src.readFully(h[0], (int) (h[1] - h[0]));
            start = Math.max(start, h[1]);
        }
        ByteScanner sc = new ByteScanner(src, start, cfg.readBufferBytes());
        byte[] hdr = header;
        return () -> {
            long[] r = nextRecord(sc, delim, max);
            if (r == null) {
                return Optional.empty();
            }
            Section.SourceRange range = new Section.SourceRange(r[0], r[1] - r[0]);
            List<Section.Part> parts = hdr == null ? List.of(range) : List.of(new Section.Literal(hdr), range);
            return Optional.of(new Section(r[0], r[1] - r[0], parts, ResumePoint.at(r[1])));
        };
    }

    /** The next non-empty record as {start, end}, end being after its line terminator; null at the end. */
    static long[] nextRecord(ByteScanner sc, byte delim, long max) throws IOException, SplitException {
        while (true) {
            long start = sc.position();
            int c = sc.read();
            if (c < 0) {
                return null;
            }
            boolean inQuotes = false;
            boolean fieldStart = true;
            boolean empty = true;
            while (true) {
                if (inQuotes) {
                    if (c == '"') {
                        if (sc.peek() == '"') {
                            sc.read();
                        } else {
                            inQuotes = false;
                        }
                    }
                    empty = false;
                } else if (c == '\n') {
                    break;
                } else if (c == delim) {
                    fieldStart = true;
                    empty = false;
                } else {
                    if (c == '"' && fieldStart) {
                        inQuotes = true;
                    }
                    if (c != '\r') {
                        empty = false;
                    }
                    fieldStart = false;
                }
                if (sc.position() - start > max) {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE, start);
                }
                c = sc.read();
                if (c < 0) {
                    if (inQuotes) {
                        throw new SplitException(ErrorCode.PARSE_UNTERMINATED_QUOTE, start);
                    }
                    break;
                }
            }
            if (!empty) {
                return new long[] {start, sc.position()};
            }
        }
    }
}
