/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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

/**
 * Tests for {@link SplitterRegistry}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SplitterRegistryTest
{

    /** The service loader finds every built-in splitter. */
    @Test
    void serviceLoaderFindsBuiltIns()
    {
        assertThat(SplitterRegistry.load().formats())
                .containsExactlyInAnyOrder("ccda", "xml", "x12", "edifact",
                        "fhir", "csv", "delimited", "parquet");
    }

    /** An unknown format is rejected. */
    @Test
    void unknownFormatIsRejected()
    {
        assertThatThrownBy(() -> SplitterRegistry.load().get("hl7v2"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** A file over the size limit fails before any of it is split. */
    @Test
    void fileSizeLimitRejectsBeforeSplitting()
    {
        SplitConfig settings = SplitTesting.config("csv")
                .withLimits(new SplitLimits(10, 100, 100));
        assertThatThrownBy(() -> SplitTesting.records(new CsvSplitter(),
                "a\nb\nc\nd\ne\nf\n".getBytes(UTF_8), settings,
                ResumePoint.START))
                .isInstanceOfSatisfying(SplitException.class, error ->
                {
                    assertThat(error.code())
                            .isEqualTo(ErrorCode.LIMIT_ITEM_SIZE);
                    assertThat(error.getMessage()).doesNotContain("a\n");
                });
    }

    /**
     * The record count includes the records before the resume point.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recordCountLimitCountsFromResumedSeq() throws Exception
    {
        SplitConfig settings = SplitTesting.config("csv")
                .withLimits(new SplitLimits(1000, 100, 3));
        SeekableByteSource source =
                SeekableByteSource.of("a\nb\nc\n".getBytes(UTF_8));
        try (SplitSession session = SplitterRegistry.of(new CsvSplitter())
                .open("csv", source, ResumePoint.at(2), 2, settings))
        {
            assertThat(session.next()).isPresent();
            assertThatThrownBy(session::next)
                    .isInstanceOfSatisfying(SplitException.class,
                            error -> assertThat(error.code())
                                    .isEqualTo(ErrorCode.LIMIT_RECORD_COUNT));
        }
    }

    /** The record size limit counts bytes the splitter adds. */
    @Test
    void recordSizeLimitIncludesSynthesisedBytes()
    {
        SplitConfig settings = SplitTesting.config("csv")
                .withOption("csv.header", "true")
                .withLimits(new SplitLimits(1000, 12, 100));
        // "id,name\n" + "1,abc\n" = 14 bytes > 12, though the record alone
        // is 6.
        SplitException error = SplitTesting.splitError(new CsvSplitter(),
                "id,name\n1,abc\n", settings);
        assertThat(error.code()).isEqualTo(ErrorCode.LIMIT_RECORD_SIZE);
    }

    /** A resume point's text form reads back. */
    @Test
    void resumePointRoundTrips()
    {
        assertThat(ResumePoint.parse("42").encode()).isEqualTo("42");
        ResumePoint point = ResumePoint.at(123, 0, 77);
        assertThat(ResumePoint.parse(point.encode())).isEqualTo(point);
    }

    /**
     * A record streams its literal and range parts in order.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recordStreamsLiteralAndRangeParts() throws Exception
    {
        SeekableByteSource source =
                SeekableByteSource.of("0123456789".getBytes(UTF_8));
        SplitRecord record = new SplitRecord(2, 5, List.of(
                new SplitRecord.Literal("<".getBytes(UTF_8)),
                new SplitRecord.SourceRange(2, 5),
                new SplitRecord.SourceRange(9, 0),
                new SplitRecord.Literal(">".getBytes(UTF_8))),
                ResumePoint.at(7));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = record.open(source))
        {
            byte[] buffer = new byte[2];
            int count;
            while ((count = in.read(buffer)) > 0)
            {
                out.write(buffer, 0, count);
            }
        }
        assertThat(out.toString(UTF_8)).isEqualTo("<23456>");
        assertThat(record.plaintextLength()).isEqualTo(7);
    }
}
