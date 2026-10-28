package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import org.junit.jupiter.api.Test;

class DelimitedSplitterTest {
    private final DelimitedSplitter splitter = new DelimitedSplitter();

    private static SplitConfig cfg(String pattern, String mode) {
        return com.sdh.healthcare.connector.core.split.SplitTesting.cfg("delimited")
                .withOption("delimited.pattern", pattern).withOption("delimited.mode", mode);
    }

    @Test
    void startModeSplitsHl7BatchAndKeepsPreamble() throws Exception {
        String in = "\nBHS|^~\\&|batch\n"
                + "MSH|^~\\&|A|1\nPID|1||x\n\n"
                + "MSH|^~\\&|A|2\rPID|1||y\n"
                + "MSH|^~\\&|A|3\nBTS|2";
        assertThat(splitAndCheckResume(splitter, in, cfg("^MSH\\|", "START"))).containsExactly(
                "BHS|^~\\&|batch\n",
                "MSH|^~\\&|A|1\nPID|1||x\n\n",
                "MSH|^~\\&|A|2\rPID|1||y\n",
                "MSH|^~\\&|A|3\nBTS|2");
    }

    @Test
    void endModeIncludesTheMatchingLine() throws Exception {
        String in = "a\nb\n$$$$\n\nc\n$$$$\ntrailing\n";
        assertThat(splitAndCheckResume(splitter, in, cfg("$$$$", "END").withOption("delimited.literal", "true")))
                .containsExactly("a\nb\n$$$$\n", "c\n$$$$\n", "trailing\n");
    }

    @Test
    void separatorModeDropsSeparatorsAndEmptySections() throws Exception {
        String in = "---\na\n---\n---\n\nb\nc\n---\n  \n";
        assertThat(splitAndCheckResume(splitter, in, cfg("^---$", "SEPARATOR")))
                .containsExactly("a\n", "b\nc\n");
    }

    @Test
    void onlyTheMatchWindowIsTested() throws Exception {
        String in = "x".repeat(100) + "KEY\nKEY\n";
        SplitConfig c = cfg("KEY", "START").withOption("delimited.match-window", "10");
        assertThat(splitAndCheckResume(splitter, in, c)).containsExactly("x".repeat(100) + "KEY\n", "KEY\n");
    }

    @Test
    void missingPatternIsAConfigError() {
        assertThatThrownBy(() -> splitter.validate(SplitConfig.defaults("delimited")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
