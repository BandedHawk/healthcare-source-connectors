package com.sdh.healthcare.connector.core.split;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;

/** Splitters by format name, loaded with {@link ServiceLoader}. Also the place where {@link SplitLimits} are enforced. */
public final class SplitterRegistry {
    private final Map<String, SectionSplitter> byFormat;

    private SplitterRegistry(Map<String, SectionSplitter> byFormat) {
        this.byFormat = Collections.unmodifiableMap(byFormat);
    }

    public static SplitterRegistry load() {
        return load(SplitterRegistry.class.getClassLoader());
    }

    public static SplitterRegistry load(ClassLoader cl) {
        Map<String, SectionSplitter> m = new TreeMap<>();
        for (SectionSplitter s : ServiceLoader.load(SectionSplitter.class, cl)) {
            add(m, s);
        }
        return new SplitterRegistry(m);
    }

    public static SplitterRegistry of(SectionSplitter... splitters) {
        Map<String, SectionSplitter> m = new TreeMap<>();
        for (SectionSplitter s : splitters) {
            add(m, s);
        }
        return new SplitterRegistry(m);
    }

    private static void add(Map<String, SectionSplitter> m, SectionSplitter s) {
        SectionSplitter prev = m.putIfAbsent(s.format(), s);
        if (prev != null) {
            throw new IllegalStateException("Two splitters for format '" + s.format() + "': "
                    + prev.getClass().getName() + " and " + s.getClass().getName());
        }
    }

    public Set<String> formats() {
        return byFormat.keySet();
    }

    public SectionSplitter get(String format) {
        SectionSplitter s = byFormat.get(format);
        if (s == null) {
            throw new IllegalArgumentException("Unknown split format '" + format + "'; known: " + formats());
        }
        return s;
    }

    /**
     * Opens a session that also enforces {@link SplitLimits} (design §6.3).
     *
     * @param nextSeq the seq the next section will get; used for the per-file section count
     */
    public SplitSession open(String format, SeekableByteSource src, ResumePoint from, long nextSeq, SplitConfig cfg)
            throws IOException, SplitException {
        SplitLimits limits = cfg.limits();
        if (src.size() > limits.maxFileBytes()) {
            throw new SplitException(ErrorCode.LIMIT_FILE_SIZE, 0);
        }
        return new LimitedSession(get(format).open(src, from, cfg), limits, nextSeq, from.offset());
    }

    private static final class LimitedSession implements SplitSession {
        private final SplitSession inner;
        private final SplitLimits limits;
        private long seq;
        private long lastResume;

        LimitedSession(SplitSession inner, SplitLimits limits, long nextSeq, long from) {
            this.inner = inner;
            this.limits = limits;
            this.seq = nextSeq;
            this.lastResume = from;
        }

        @Override
        public Optional<Section> next() throws IOException, SplitException {
            Optional<Section> s = inner.next();
            if (s.isEmpty()) {
                return s;
            }
            Section section = s.get();
            if (section.plaintextLength() > limits.maxSectionBytes()) {
                throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE, section.sourceOffset());
            }
            if (seq >= limits.maxSectionsPerFile()) {
                throw new SplitException(ErrorCode.LIMIT_SECTION_COUNT, section.sourceOffset());
            }
            if (section.resume().offset() <= lastResume) {
                throw new IllegalStateException("Splitter made no progress at offset " + lastResume);
            }
            lastResume = section.resume().offset();
            seq++;
            return s;
        }

        @Override
        public void close() {
            inner.close();
        }
    }
}
