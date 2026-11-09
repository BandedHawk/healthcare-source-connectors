/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitAndCheckResume;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitError;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitLimits;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CsvSplitter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CsvSplitterTest
{

    /** The splitter under test. */
    private final CsvSplitter splitter = new CsvSplitter();

    /**
     * Each record is a record; quoted newlines, delimiters and quotes
     * stay inside it, and empty lines are skipped.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void oneRecordPerRowRespectingQuotes() throws Exception
    {
        String csv = "1,\"Smith, John\",\"line one\nline two\"\r\n"
                + "\n"
                + "2,\"He said \"\"hi\"\"\",x\n"
                + "3,ab\"c,\"\"\n"
                + "4,last-without-newline";
        assertThat(splitAndCheckResume(splitter, csv, config("csv")))
                .containsExactly(
                        "1,\"Smith, John\",\"line one\nline two\"\r\n",
                        "2,\"He said \"\"hi\"\"\",x\n",
                        "3,ab\"c,\"\"\n",
                        "4,last-without-newline");
    }

    /**
     * The header goes before every record, after a resume too.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void headerIsPrependedToEveryRecordIncludingAfterResume()
            throws Exception
    {
        String csv = "id,note\n1,\"a\nb\"\n2,c\n";
        assertThat(splitAndCheckResume(splitter, csv,
                config("csv").withOption("csv.header", "true")))
                .containsExactly(
                        "id,note\n1,\"a\nb\"\n",
                        "id,note\n2,c\n");
    }

    /**
     * A file of only a header is an empty container; one of nothing, or of
     * blank lines, is empty (STAGES.md §16).
     */
    @Test
    void headerOnlyFileIsAnEmptyContainer()
    {
        SplitConfig header = config("csv").withOption("csv.header", "true");
        assertThat(splitError(splitter, "id,note\n", header).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(splitter, " \n\nid,note\n \t\r\n", header)
                .code()).isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(splitter, "", header).code())
                .isEqualTo(ErrorCode.SPLIT_EMPTY);
        assertThat(splitError(splitter, " \t\r\n\n", config("csv")).code())
                .isEqualTo(ErrorCode.SPLIT_EMPTY);
    }

    /**
     * A line of only spaces, tabs and CR is skipped, before the header too,
     * unless it holds the delimiter: with a tab delimiter, a line of tabs is
     * a row of empty fields.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void blankLinesAreSkipped() throws Exception
    {
        assertThat(splitAndCheckResume(splitter, "a,b\n  \n\t\r\nc,d\n \n",
                config("csv"))).containsExactly("a,b\n", "c,d\n");
        assertThat(splitAndCheckResume(splitter, " \nid\n \n1\n",
                config("csv").withOption("csv.header", "true")))
                .containsExactly("id\n1\n");
        assertThat(splitAndCheckResume(splitter, "a\tb\n\t\n  \n",
                config("csv").withOption("csv.delimiter", "\\t")))
                .containsExactly("a\tb\n", "\t\n");
    }

    /**
     * Another delimiter decides where quoted fields may start.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void otherDelimiter() throws Exception
    {
        String csv = "1|\"a|b\nc\"\n2|d\n";
        assertThat(splitAndCheckResume(splitter, csv,
                config("csv").withOption("csv.delimiter", "|")))
                .containsExactly("1|\"a|b\nc\"\n", "2|d\n");
    }

    /** A quote never closed fails the file at its record. */
    @Test
    void unterminatedQuoteFailsTheFile()
    {
        SplitException error = splitError(splitter,
                "1,ok\n2,\"never closed\n3,x\n", config("csv"));
        assertThat(error.code()).isEqualTo(ErrorCode.PARSE_UNTERMINATED_QUOTE);
        assertThat(error.offset()).isEqualTo(5);
    }

    /** A record over the size limit fails without being read to its end. */
    @Test
    void oversizedRecordFailsEarly()
    {
        SplitConfig settings = config("csv")
                .withLimits(new SplitLimits(1 << 20, 8, 100));
        assertThat(splitError(splitter, "1,ok\n2,\"" + "x".repeat(1000),
                settings).code()).isEqualTo(ErrorCode.LIMIT_RECORD_SIZE);
    }
}
