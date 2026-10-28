package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

class CsvSplitterTest {
    private final CsvSplitter splitter = new CsvSplitter();

    @Test
    void oneSectionPerRecordRespectingQuotes() throws Exception {
        String csv = "1,\"Smith, John\",\"line one\nline two\"\r\n"
                + "\n"
                + "2,\"He said \"\"hi\"\"\",x\n"
                + "3,ab\"c,\"\"\n"
                + "4,last-without-newline";
        assertThat(splitAndCheckResume(splitter, csv, cfg("csv"))).containsExactly(
                "1,\"Smith, John\",\"line one\nline two\"\r\n",
                "2,\"He said \"\"hi\"\"\",x\n",
                "3,ab\"c,\"\"\n",
                "4,last-without-newline");
    }

    @Test
    void headerIsPrependedToEverySectionIncludingAfterResume() throws Exception {
        String csv = "id,note\n1,\"a\nb\"\n2,c\n";
        assertThat(splitAndCheckResume(splitter, csv, cfg("csv").withOption("csv.header", "true"))).containsExactly(
                "id,note\n1,\"a\nb\"\n",
                "id,note\n2,c\n");
    }

    @Test
    void headerOnlyFileHasNoSections() throws Exception {
        assertThat(splitAndCheckResume(splitter, "id,note\n", cfg("csv").withOption("csv.header", "true"))).isEmpty();
        assertThat(splitAndCheckResume(splitter, "", cfg("csv").withOption("csv.header", "true"))).isEmpty();
    }

    @Test
    void otherDelimiter() throws Exception {
        String csv = "1|\"a|b\nc\"\n2|d\n";
        assertThat(splitAndCheckResume(splitter, csv, cfg("csv").withOption("csv.delimiter", "|")))
                .containsExactly("1|\"a|b\nc\"\n", "2|d\n");
    }

    @Test
    void unterminatedQuoteFailsTheFile() {
        var e = splitError(splitter, "1,ok\n2,\"never closed\n3,x\n", cfg("csv"));
        assertThat(e.code()).isEqualTo(ErrorCode.PARSE_UNTERMINATED_QUOTE);
        assertThat(e.offset()).isEqualTo(5);
    }

    @Test
    void oversizedRecordFailsEarly() {
        var cfg = cfg("csv").withLimits(new com.sdh.healthcare.connector.core.split.SplitLimits(1 << 20, 8, 100));
        assertThat(splitError(splitter, "1,ok\n2,\"" + "x".repeat(1000), cfg).code())
                .isEqualTo(ErrorCode.LIMIT_SECTION_SIZE);
    }
}
