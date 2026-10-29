package com.sdh.healthcare.connector.core.split;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.builtin.CsvSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class SplitterRegistryTest {

    @Test
    void serviceLoaderFindsBuiltIns() {
        assertThat(SplitterRegistry.load().formats())
                .containsExactlyInAnyOrder("ccda", "xml", "x12", "edifact", "fhir", "csv", "delimited");
    }

    @Test
    void unknownFormatIsRejected() {
        assertThatThrownBy(() -> SplitterRegistry.load().get("hl7v2")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fileSizeLimitRejectsBeforeSplitting() {
        SplitConfig cfg = SplitTesting.cfg("csv").withLimits(new SplitLimits(10, 100, 100));
        assertThatThrownBy(() -> SplitTesting.sections(new CsvSplitter(), "a\nb\nc\nd\ne\nf\n".getBytes(UTF_8), cfg,
                ResumePoint.START))
                .isInstanceOfSatisfying(SplitException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.LIMIT_FILE_SIZE);
                    assertThat(e.getMessage()).doesNotContain("a\n");
                });
    }

    @Test
    void sectionCountLimitCountsFromResumedSeq() throws Exception {
        SplitConfig cfg = SplitTesting.cfg("csv").withLimits(new SplitLimits(1000, 100, 3));
        SeekableByteSource src = SeekableByteSource.of("a\nb\nc\n".getBytes(UTF_8));
        try (SplitSession s = SplitterRegistry.of(new CsvSplitter()).open("csv", src, ResumePoint.at(2), 2, cfg)) {
            assertThat(s.next()).isPresent();
            assertThatThrownBy(s::next).isInstanceOfSatisfying(SplitException.class,
                    e -> assertThat(e.code()).isEqualTo(ErrorCode.LIMIT_SECTION_COUNT));
        }
    }

    @Test
    void sectionSizeLimitIncludesSynthesisedBytes() {
        SplitConfig cfg = SplitTesting.cfg("csv").withOption("csv.header", "true")
                .withLimits(new SplitLimits(1000, 12, 100));
        // "id,name\n" + "1,abc\n" = 14 bytes > 12, though the record alone is 6.
        SplitException e = SplitTesting.splitError(new CsvSplitter(), "id,name\n1,abc\n", cfg);
        assertThat(e.code()).isEqualTo(ErrorCode.LIMIT_SECTION_SIZE);
    }

    @Test
    void resumePointRoundTrips() {
        assertThat(ResumePoint.parse("42").encode()).isEqualTo("42");
        ResumePoint p = ResumePoint.at(123, 0, 77);
        assertThat(ResumePoint.parse(p.encode())).isEqualTo(p);
    }

    @Test
    void sectionStreamsLiteralAndRangeParts() throws Exception {
        SeekableByteSource src = SeekableByteSource.of("0123456789".getBytes(UTF_8));
        Section s = new Section(2, 5, List.of(new Section.Literal("<".getBytes(UTF_8)),
                new Section.SourceRange(2, 5), new Section.SourceRange(9, 0), new Section.Literal(">".getBytes(UTF_8))),
                ResumePoint.at(7));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = s.open(src)) {
            byte[] buf = new byte[2];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        }
        assertThat(out.toString(UTF_8)).isEqualTo("<23456>");
        assertThat(s.plaintextLength()).isEqualTo(7);
    }
}
