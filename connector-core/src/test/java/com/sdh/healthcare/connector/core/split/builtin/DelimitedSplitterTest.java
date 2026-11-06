/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitAndCheckResume;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DelimitedSplitter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DelimitedSplitterTest
{

    /** The splitter under test. */
    private final DelimitedSplitter splitter = new DelimitedSplitter();

    /**
     * Settings with a pattern and mode.
     *
     * @param pattern the pattern
     * @param mode    the mode
     * @return the settings
     */
    private static SplitConfig delimited(String pattern, String mode)
    {
        return config("delimited")
                .withOption("delimited.pattern", pattern)
                .withOption("delimited.mode", mode);
    }

    /**
     * In START mode an HL7 batch splits at each MSH, and the lines before
     * the first are kept as a record.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void startModeSplitsHl7BatchAndKeepsPreamble() throws Exception
    {
        String input = "\nBHS|^~\\&|batch\n"
                + "MSH|^~\\&|A|1\nPID|1||x\n\n"
                + "MSH|^~\\&|A|2\rPID|1||y\n"
                + "MSH|^~\\&|A|3\nBTS|2";
        assertThat(splitAndCheckResume(splitter, input,
                delimited("^MSH\\|", "START"))).containsExactly(
                        "BHS|^~\\&|batch\n",
                        "MSH|^~\\&|A|1\nPID|1||x\n\n",
                        "MSH|^~\\&|A|2\rPID|1||y\n",
                        "MSH|^~\\&|A|3\nBTS|2");
    }

    /**
     * In END mode the matching line ends its record.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void endModeIncludesTheMatchingLine() throws Exception
    {
        String input = "a\nb\n$$$$\n\nc\n$$$$\ntrailing\n";
        assertThat(splitAndCheckResume(splitter, input,
                delimited("$$$$", "END")
                        .withOption("delimited.literal", "true")))
                .containsExactly("a\nb\n$$$$\n", "c\n$$$$\n", "trailing\n");
    }

    /**
     * In SEPARATOR mode separators and empty records are dropped.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void separatorModeDropsSeparatorsAndEmptyRecords() throws Exception
    {
        String input = "---\na\n---\n---\n\nb\nc\n---\n  \n";
        assertThat(splitAndCheckResume(splitter, input,
                delimited("^---$", "SEPARATOR")))
                .containsExactly("a\n", "b\nc\n");
    }

    /**
     * A match past the window isn't seen.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void onlyTheMatchWindowIsTested() throws Exception
    {
        String input = "x".repeat(100) + "KEY\nKEY\n";
        SplitConfig settings = delimited("KEY", "START")
                .withOption("delimited.match-window", "10");
        assertThat(splitAndCheckResume(splitter, input, settings))
                .containsExactly("x".repeat(100) + "KEY\n", "KEY\n");
    }

    /** Without a pattern the settings are rejected. */
    @Test
    void missingPatternIsAConfigError()
    {
        assertThatThrownBy(() -> splitter.validate(
                SplitConfig.defaults("delimited")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
