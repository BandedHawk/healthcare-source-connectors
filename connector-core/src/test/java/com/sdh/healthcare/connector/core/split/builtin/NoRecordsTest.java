/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.records;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitAndCheckResume;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitError;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.sdh.healthcare.connector.core.split.RecordSplitter;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitLimits;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rules for an item without records (STAGES.md §16), the same for
 * every format: an empty or blank item fails {@code SPLIT_EMPTY}, an empty
 * container of the format fails {@code SPLIT_NO_RECORDS}, and anything else
 * the splitter cuts nothing from is one record of the whole item.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class NoRecordsTest
{

    /** Blank input: spaces, tabs, CR and LF. */
    static final String BLANK = " \n\r\n\t \n";

    /** Every text format, with its settings. */
    static final List<Format> TEXT = List.of(
            new Format(new CcdaSplitter(), config("ccda")),
            new Format(new XmlSplitter(),
                    config("xml").withBoundary("item")),
            new Format(new CsvSplitter(), config("csv")),
            new Format(new DelimitedSplitter(), config("delimited")
                    .withOption("delimited.pattern", "^MSH")),
            new Format(new FhirSplitter(), config("fhir")),
            new Format(new X12Splitter(), config("x12")),
            new Format(new EdifactSplitter(), config("edifact")));

    /**
     * A splitter and its settings.
     *
     * @param splitter the splitter
     * @param config   its settings
     */
    record Format(RecordSplitter splitter, SplitConfig config)
    {
    }

    /** Nothing, blank, or blank after a byte-order mark, is empty. */
    @Test
    void emptyAndBlankFailInEveryFormat()
    {
        for (Format format : TEXT)
        {
            for (String input : List.of("", BLANK, "﻿" + BLANK))
            {
                assertThat(splitError(format.splitter(), input,
                        format.config()).code())
                        .as("%s [%s]", format.splitter().format(), input)
                        .isEqualTo(ErrorCode.SPLIT_EMPTY);
            }
        }
        assertThat(splitError(new ParquetSplitter(), "", config("parquet"))
                .code()).isEqualTo(ErrorCode.SPLIT_EMPTY);
    }

    /**
     * Each format's container with nothing in it, which its splitter read
     * and reports as empty.
     *
     * @param dir where to write a Parquet file
     * @throws Exception if the test fails to run
     */
    @Test
    void emptyContainersFail(@TempDir Path dir) throws Exception
    {
        String isa = X12SplitterTest.ISA;
        assertThat(splitError(new X12Splitter(), isa + "IEA*0*000000905~\n",
                config("x12")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(new X12Splitter(), isa + X12SplitterTest.GS1
                + "GE*0*1~\nIEA*1*000000905~\n", config("x12")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        String unb = EdifactSplitterTest.UNA + EdifactSplitterTest.UNB;
        assertThat(splitError(new EdifactSplitter(),
                unb + "UNZ+0+REF0001'\n", config("edifact")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(new FhirSplitter(), "[]", config("fhir"))
                .code()).isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(new CsvSplitter(), "a,b\n",
                config("csv").withOption("csv.header", "true")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(new DelimitedSplitter(), "---\n\n---\n",
                config("delimited").withOption("delimited.pattern", "^---")
                        .withOption("delimited.mode", "SEPARATOR")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        byte[] noRows = parquetWithoutRows(dir);
        assertThatThrownBy(() -> records(new ParquetSplitter(), noRows,
                config("parquet"), ResumePoint.START))
                .isInstanceOfSatisfying(SplitException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS));
    }

    /**
     * A Parquet file with a schema and no row group, as a writer leaves one
     * it was given no rows.
     *
     * @param dir where to write it
     * @return the file
     * @throws IOException if it can't be written
     */
    static byte[] parquetWithoutRows(Path dir) throws IOException
    {
        Path file = dir.resolve("empty.parquet");
        ParquetFileWriter writer = new ParquetFileWriter(
                new LocalOutputFile(file), MessageTypeParser.parseMessageType(
                        "message row { required int64 id; }"),
                ParquetFileWriter.Mode.CREATE, 1 << 20, 0);
        writer.start();
        writer.end(Map.of());
        return Files.readAllBytes(file);
    }

    /**
     * Content the XML formats find nothing to cut in is taken as one
     * document, the whole item, for the stages after to judge; resuming
     * after it gives nothing more.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void contentWithoutABoundaryIsOneRecord() throws Exception
    {
        String text = "not a clinical document\n";
        String other = "<?xml version=\"1.0\"?>\n<batch><x/></batch>\n";
        assertThat(splitAndCheckResume(new CcdaSplitter(), text,
                config("ccda"))).containsExactly(text);
        assertThat(splitAndCheckResume(new CcdaSplitter(), other,
                config("ccda"))).containsExactly(other);
        assertThat(splitAndCheckResume(new XmlSplitter(), other,
                config("xml").withBoundary("item")))
                .containsExactly(other);
        List<SplitRecord> one = records(new CcdaSplitter(),
                text.getBytes(UTF_8), config("ccda"), ResumePoint.START);
        assertThat(one.get(0).sourceOffset()).isZero();
        assertThat(one.get(0).resume())
                .isEqualTo(ResumePoint.at(text.length()));
        assertThat(one.get(0).failure()).isNull();
    }

    /**
     * The whole item, taken as one record, is held to the record size
     * limit like any other.
     */
    @Test
    void theWholeItemRecordIsSizeLimited()
    {
        SplitConfig small = config("ccda")
                .withLimits(new SplitLimits(1 << 20, 8, 100));
        assertThat(splitError(new CcdaSplitter(), "longer than eight\n",
                small).code()).isEqualTo(ErrorCode.LIMIT_RECORD_SIZE);
    }

    /**
     * A single document stays one record of its own, as before.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aSingleDocumentIsOneRecord() throws Exception
    {
        String document = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<x/></ClinicalDocument>";
        assertThat(splitAndCheckResume(new CcdaSplitter(),
                "\n" + document + "\n", config("ccda")))
                .containsExactly(document);
        assertThat(splitAndCheckResume(new CsvSplitter(), "1,a\n",
                config("csv"))).containsExactly("1,a\n");
    }
}
