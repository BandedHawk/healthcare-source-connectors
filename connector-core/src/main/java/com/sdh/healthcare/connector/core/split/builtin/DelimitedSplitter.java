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
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Generic line-based splitter for proprietary formats (design §6.2).
 *
 * <p>
 * Each line (up to its LF) is tested against a regex with {@code find()}. Only
 * the first {@code delimited.match-window} bytes (default 4096) of a line are
 * tested, so very long lines stay cheap.
 *
 * <ul>
 * <li>{@code delimited.pattern}: the regex; falls back to the boundary
 * setting</li>
 * <li>{@code delimited.literal}: treat the pattern as plain text (default
 * false)</li>
 * <li>{@code delimited.mode}: {@code START} a matching line begins a section;
 * {@code END} it ends one (inclusive); {@code SEPARATOR} it sits between
 * sections and is dropped</li>
 * <li>{@code delimited.charset}: used to decode lines for matching (default
 * UTF-8)</li>
 * </ul>
 *
 * Lines that are empty or whitespace only never start a section. In START mode,
 * any lines before the first match become a section of their own, so nothing is
 * dropped.
 */
public final class DelimitedSplitter implements SectionSplitter
{

    public static final String FORMAT = "delimited";

    enum Mode
    {
        START, END, SEPARATOR
    }

    private record Settings(Pattern pattern, Mode mode, Charset charset,
            int window) {}

    @Override
    public String format()
    {
        return FORMAT;
    }

    @Override
    public void validate(SplitConfig cfg)
    {
        settings(cfg);
    }

    private static Settings settings(SplitConfig cfg)
    {
        String p = cfg.option("delimited.pattern", cfg.boundaryOr(null));
        if (p == null)
        {
            throw new IllegalArgumentException("delimited.pattern is required");
        }
        Pattern pattern = Pattern.compile(cfg.booleanOption("delimited.literal",
                false) ? Pattern.quote(p) : p);
        Mode mode = Mode.valueOf(cfg.option("delimited.mode", "START").
                toUpperCase(Locale.ROOT));
        Charset cs = Charset.forName(cfg.option("delimited.charset", "UTF-8"));
        int window = cfg.intOption("delimited.match-window", 4096);
        if (window < 1)
        {
            throw new IllegalArgumentException(
                    "delimited.match-window must be positive");
        }
        return new Settings(pattern, mode, cs, window);
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from,
            SplitConfig cfg)
    {
        return new Session(new ByteScanner(src, from.offset(), cfg.
                readBufferBytes()), settings(cfg),
                cfg.limits().maxSectionBytes());
    }

    private record Line(long start, long end, boolean blank, boolean match) {}

    private static final class Session implements SplitSession
    {

        private final ByteScanner sc;
        private final Settings s;
        private final long max;
        private final byte[] window;
        private Line pending;

        Session(ByteScanner sc, Settings s, long max)
        {
            this.sc = sc;
            this.s = s;
            this.max = max;
            this.window = new byte[s.window()];
        }

        private Line readLine() throws IOException
        {
            long start = sc.position();
            int n = 0;
            int c;
            while (n < window.length && (c = sc.read()) >= 0)
            {
                window[n++] = (byte) c;
                if (c == '\n')
                {
                    break;
                }
            }
            if (n == 0)
            {
                return null;
            }
            boolean ended = window[n - 1] == '\n';
            if (!ended && n == window.length && sc.skipTo((byte) '\n'))
            {
                sc.skip(1);
            }
            boolean fullLine = sc.position() - start == n;
            int content = n;
            while (content > 0 && (window[content - 1] == '\n' || window[content - 1] == '\r'))
            {
                content--;
            }
            boolean blank = fullLine;
            for (int i = 0; i < content && blank; i++)
            {
                blank = window[i] == ' ' || window[i] == '\t';
            }
            boolean match = s.pattern().matcher(new String(window, 0, content,
                    s.charset())).find();
            return new Line(start, sc.position(), blank, match);
        }

        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            long secStart = -1;
            long secEnd = -1;
            boolean content = false;
            while (true)
            {
                Line line = pending != null ? pending : readLine();
                pending = null;
                if (line == null)
                {
                    return content ? Optional.of(Section.ofRange(secStart,
                            secEnd - secStart,
                            ResumePoint.at(sc.position()))) : Optional.empty();
                }
                switch (s.mode())
                {
                    case START ->
                    {
                        if (line.match() && content)
                        {
                            pending = line;
                            return emit(secStart, secEnd, line.start());
                        }
                    }
                    case SEPARATOR ->
                    {
                        if (line.match())
                        {
                            if (content)
                            {
                                return emit(secStart, secEnd, line.end());
                            }
                            secStart = -1;
                            continue;
                        }
                    }
                    case END ->
                    {
                    }
                }
                if (secStart < 0 || !content)
                {
                    if (line.blank())
                    {
                        continue;
                    }
                    secStart = line.start();
                }
                secEnd = line.end();
                content = true;
                if (secEnd - secStart > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            secStart);
                }
                if (s.mode() == Mode.END && line.match())
                {
                    return emit(secStart, secEnd, secEnd);
                }
            }
        }

        private static Optional<Section> emit(long start, long end, long resume)
        {
            return Optional.of(Section.ofRange(start, end - start, ResumePoint.
                    at(resume)));
        }
    }

}
