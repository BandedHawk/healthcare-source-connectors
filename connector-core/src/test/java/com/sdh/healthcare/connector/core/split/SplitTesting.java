package com.sdh.healthcare.connector.core.split;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Helpers shared by the splitter tests. */
public final class SplitTesting {
    private SplitTesting() {}

    /** Tiny read buffer, so tokens straddle buffer refills. */
    public static SplitConfig cfg(String format) {
        return SplitConfig.defaults(format).withReadBuffer(ByteScanner.MIN_BUFFER);
    }

    public static List<Section> sections(SectionSplitter s, byte[] data, SplitConfig cfg, ResumePoint from)
            throws Exception {
        SeekableByteSource src = SeekableByteSource.of(data);
        List<Section> out = new ArrayList<>();
        try (SplitSession session = SplitterRegistry.of(s).open(s.format(), src, from, 0, cfg)) {
            Optional<Section> next;
            while ((next = session.next()).isPresent()) {
                out.add(next.get());
            }
        }
        return out;
    }

    public static List<String> texts(byte[] data, List<Section> sections) throws Exception {
        SeekableByteSource src = SeekableByteSource.of(data);
        List<String> out = new ArrayList<>();
        for (Section s : sections) {
            byte[] b = s.readAll(src);
            assertThat((long) b.length).isEqualTo(s.plaintextLength());
            out.add(new String(b, UTF_8));
        }
        return out;
    }

    /** Splits the whole input, then checks that resuming after each section yields exactly the rest. */
    public static List<String> splitAndCheckResume(SectionSplitter s, String input, SplitConfig cfg)
            throws Exception {
        byte[] data = input.getBytes(UTF_8);
        List<Section> all = sections(s, data, cfg, ResumePoint.START);
        List<String> texts = texts(data, all);
        for (int i = 0; i < all.size(); i++) {
            ResumePoint rp = ResumePoint.parse(all.get(i).resume().encode());
            List<Section> rest = sections(s, data, cfg, rp);
            assertThat(texts(data, rest)).as("resume after section %d", i).isEqualTo(texts.subList(i + 1, texts.size()));
            assertThat(rest).extracting(Section::sourceOffset)
                    .isEqualTo(all.subList(i + 1, all.size()).stream().map(Section::sourceOffset).toList());
        }
        // Same answer with a normal-sized buffer.
        assertThat(texts(data, sections(s, data, cfg.withReadBuffer(SplitConfig.DEFAULT_READ_BUFFER),
                ResumePoint.START))).isEqualTo(texts);
        return texts;
    }

    public static SplitException splitError(SectionSplitter s, String input, SplitConfig cfg) {
        try {
            sections(s, input.getBytes(UTF_8), cfg, ResumePoint.START);
        } catch (SplitException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        throw new AssertionError("Expected a SplitException");
    }
}
