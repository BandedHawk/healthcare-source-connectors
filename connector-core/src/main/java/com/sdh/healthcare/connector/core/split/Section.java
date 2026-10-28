package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * One self-contained section, described as byte ranges of the source plus any synthesised bytes (design §6.1).
 * A section never holds the source bytes themselves, so a splitter needs almost no memory however large
 * the section is.
 *
 * @param sourceOffset where the section starts in the source file
 * @param sourceLength how many source bytes it spans
 * @param parts        the section's plaintext, in order: literal bytes (such as a CSV header or an X12
 *                     envelope) and source ranges
 * @param resume       where a new session starts to produce the sections after this one
 */
public record Section(long sourceOffset, long sourceLength, List<Part> parts, ResumePoint resume) {

    public sealed interface Part permits Literal, SourceRange {
        long length();
    }

    /** Synthesised bytes. Not copied; callers must not change the array. */
    public record Literal(byte[] bytes) implements Part {
        @Override
        public long length() {
            return bytes.length;
        }
    }

    public record SourceRange(long offset, long length) implements Part {}

    public Section {
        parts = List.copyOf(parts);
    }

    public static Section ofRange(long offset, long length, ResumePoint resume) {
        return new Section(offset, length, List.of(new SourceRange(offset, length)), resume);
    }

    public long plaintextLength() {
        long n = 0;
        for (Part p : parts) {
            n += p.length();
        }
        return n;
    }

    /** Streams the section's plaintext from {@code src}. */
    public InputStream open(SeekableByteSource src) {
        return new SectionStream(src, parts);
    }

    /** Reads the whole section into memory. For small sections and tests. */
    public byte[] readAll(SeekableByteSource src) throws IOException {
        long n = plaintextLength();
        if (n > Integer.MAX_VALUE - 8) {
            throw new IOException("Section too large to read into memory");
        }
        try (InputStream in = open(src)) {
            return in.readNBytes((int) n);
        }
    }

    private static final class SectionStream extends InputStream {
        private final SeekableByteSource src;
        private final List<Part> parts;
        private int index = -1;
        private Part current;
        private long done;

        SectionStream(SeekableByteSource src, List<Part> parts) {
            this.src = src;
            this.parts = parts;
            advance();
        }

        private void advance() {
            do {
                index++;
                current = index < parts.size() ? parts.get(index) : null;
                done = 0;
            } while (current != null && current.length() == 0);
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (current == null) {
                return -1;
            }
            int n = (int) Math.min(len, current.length() - done);
            switch (current) {
                case Literal l -> System.arraycopy(l.bytes(), (int) done, b, off, n);
                case SourceRange r -> {
                    int r2 = src.read(r.offset() + done, ByteBuffer.wrap(b, off, n));
                    if (r2 < 0) {
                        throw new IOException("Source ended inside a section");
                    }
                    n = r2;
                }
            }
            done += n;
            if (done == current.length()) {
                advance();
            }
            return n;
        }
    }
}
