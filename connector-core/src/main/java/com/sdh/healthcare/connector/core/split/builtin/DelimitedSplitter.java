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
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Generic line-based splitter for proprietary formats (design §6.2).
 * <p>
 * Each line (up to its LF) is tested against a regex with {@code find()}.
 * Only the first {@code delimited.match-window} bytes (default 4096) of a
 * line are tested, so very long lines stay cheap.
 * <ul>
 * <li>{@code delimited.pattern}: the regex; falls back to the boundary
 * setting</li>
 * <li>{@code delimited.literal}: treat the pattern as plain text (default
 * false)</li>
 * <li>{@code delimited.mode}: {@code START} a matching line begins a
 * section; {@code END} it ends one (inclusive); {@code SEPARATOR} it sits
 * between sections and is dropped</li>
 * <li>{@code delimited.charset}: used to decode lines for matching
 * (default UTF-8)</li>
 * </ul>
 * <p>
 * Lines that are empty or whitespace only never start a section. In START
 * mode, any lines before the first match become a section of their own, so
 * nothing is dropped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DelimitedSplitter implements SectionSplitter
{

    /** The format's name. */
    public static final String FORMAT = "delimited";

    /**
     * Creates the splitter; the service loader calls this.
     */
    public DelimitedSplitter()
    {
    }

    /** What a matching line does. */
    enum Mode
    {
        /** It begins a section. */
        START,

        /** It ends a section, and belongs to it. */
        END,

        /** It sits between sections, and is dropped. */
        SEPARATOR
    }

    /**
     * The settings, read from the configuration.
     *
     * @param pattern what a matching line contains
     * @param mode    what a matching line does
     * @param charset decodes lines for matching
     * @param window  how many bytes of each line are tested
     */
    private record Settings(Pattern pattern, Mode mode, Charset charset,
            int window)
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
     * @throws IllegalArgumentException if a setting is missing or invalid
     */
    @Override
    public void validate(SplitConfig config)
    {
        settings(config);
    }

    /**
     * Reads the settings.
     *
     * @param config the split settings
     * @return the settings
     * @throws IllegalArgumentException if a setting is missing or invalid
     */
    private static Settings settings(SplitConfig config)
    {
        String regex = config.option("delimited.pattern",
                config.boundaryOr(null));
        if (regex == null)
        {
            throw new IllegalArgumentException(
                    "delimited.pattern is required");
        }
        Pattern pattern = Pattern.compile(
                config.booleanOption("delimited.literal", false)
                        ? Pattern.quote(regex) : regex);
        Mode mode = Mode.valueOf(config.option("delimited.mode", "START")
                .toUpperCase(Locale.ROOT));
        Charset charset = Charset.forName(
                config.option("delimited.charset", "UTF-8"));
        int window = config.intOption("delimited.match-window", 4096);
        if (window < 1)
        {
            throw new IllegalArgumentException(
                    "delimited.match-window must be positive");
        }
        return new Settings(pattern, mode, charset, window);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config)
    {
        return new Session(new ByteScanner(source, from.offset(),
                config.readBufferBytes()), settings(config),
                config.limits().maxSectionBytes());
    }

    /**
     * One line.
     *
     * @param start its source offset
     * @param end   the offset after its LF
     * @param blank whether it is empty or whitespace only
     * @param match whether it matches the pattern
     */
    private record Line(long start, long end, boolean blank, boolean match)
    {
    }

    /** Splits one file. */
    private static final class Session implements SplitSession
    {

        /** Reads the file. */
        private final ByteScanner scanner;

        /** The settings. */
        private final Settings settings;

        /** The largest section allowed, in bytes. */
        private final long max;

        /** The start of the current line, for matching. */
        private final byte[] window;

        /** A line read but not yet used: the start of the next section. */
        private Line pending;

        /**
         * Creates a session.
         *
         * @param scanner  reads the file
         * @param settings the settings
         * @param max      the largest section allowed
         */
        Session(ByteScanner scanner, Settings settings, long max)
        {
            this.scanner = scanner;
            this.settings = settings;
            this.max = max;
            this.window = new byte[settings.window()];
        }

        /**
         * Reads the next line.
         *
         * @return the line, or null at the end
         * @throws IOException if the file can't be read
         */
        private Line readLine() throws IOException
        {
            long start = scanner.position();
            int count = 0;
            int next;
            while (count < window.length && (next = scanner.read()) >= 0)
            {
                window[count++] = (byte) next;
                if (next == '\n')
                {
                    break;
                }
            }
            if (count == 0)
            {
                return null;
            }
            boolean ended = window[count - 1] == '\n';
            if (!ended && count == window.length
                    && scanner.skipTo((byte) '\n'))
            {
                scanner.skip(1);
            }
            boolean fullLine = scanner.position() - start == count;
            int content = count;
            while (content > 0 && (window[content - 1] == '\n'
                    || window[content - 1] == '\r'))
            {
                content--;
            }
            boolean blank = fullLine;
            for (int index = 0; index < content && blank; index++)
            {
                blank = window[index] == ' ' || window[index] == '\t';
            }
            boolean match = settings.pattern().matcher(
                    new String(window, 0, content, settings.charset()))
                    .find();
            return new Line(start, scanner.position(), blank, match);
        }

        /**
         * {@inheritDoc}
         *
         * @throws SplitException if a section is too large
         */
        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            long sectionStart = -1;
            long sectionEnd = -1;
            boolean content = false;
            while (true)
            {
                Line line = pending != null ? pending : readLine();
                pending = null;
                if (line == null)
                {
                    return content
                            ? Optional.of(Section.ofRange(sectionStart,
                                    sectionEnd - sectionStart,
                                    ResumePoint.at(scanner.position())))
                            : Optional.empty();
                }
                switch (settings.mode())
                {
                    case START ->
                    {
                        if (line.match() && content)
                        {
                            pending = line;
                            return emit(sectionStart, sectionEnd,
                                    line.start());
                        }
                    }
                    case SEPARATOR ->
                    {
                        if (line.match())
                        {
                            if (content)
                            {
                                return emit(sectionStart, sectionEnd,
                                        line.end());
                            }
                            sectionStart = -1;
                            continue;
                        }
                    }
                    case END ->
                    {
                    }
                }
                if (sectionStart < 0 || !content)
                {
                    if (line.blank())
                    {
                        continue;
                    }
                    sectionStart = line.start();
                }
                sectionEnd = line.end();
                content = true;
                if (sectionEnd - sectionStart > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            sectionStart);
                }
                if (settings.mode() == Mode.END && line.match())
                {
                    return emit(sectionStart, sectionEnd, sectionEnd);
                }
            }
        }

        /**
         * A section.
         *
         * @param start  its source offset
         * @param end    the offset after it
         * @param resume where splitting continues
         * @return the section
         */
        private static Optional<Section> emit(long start, long end,
                long resume)
        {
            return Optional.of(Section.ofRange(start, end - start,
                    ResumePoint.at(resume)));
        }
    }
}
