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

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EdifactSplitter}. Every section must be a standalone
 * interchange; resume from every section is checked by
 * {@code splitAndCheckResume}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class EdifactSplitterTest
{

    /** A service string advice with the default delimiters. */
    static final String UNA = "UNA:+.? '\n";

    /** An interchange header. */
    static final String UNB = "UNB+UNOC:3+SENDER01:14+RECEIVER01:14"
            + "+261028:1200+REF0001'\n";

    /** The first message, with an escaped quote. */
    static final String M1 = "UNH+1+PAYMUL:D:96A:UN'\n"
            + "BGM+452+PAY?'0001+9'\nUNT+3+1'\n";

    /** The second message, with an escaped quote. */
    static final String M2 = "UNH+2+PAYMUL:D:96A:UN'\n"
            + "FTX+AAA+++ESCAPED ?' QUOTE'\nUNT+3+2'\n";

    /** An interchange trailer for both messages. */
    static final String UNZ = "UNZ+2+REF0001'\n";

    /** One interchange of two messages. */
    static final String FILE = UNA + UNB + M1 + M2 + UNZ;

    /** The splitter under test. */
    private final EdifactSplitter splitter = new EdifactSplitter();

    /**
     * Each message is a section, in its own interchange.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void eachMessageBecomesAStandaloneInterchange() throws Exception
    {
        String unz1 = "UNZ+1+REF0001'\n";
        assertThat(splitAndCheckResume(splitter, FILE, config("edifact")))
                .containsExactly(
                        UNA + UNB + M1 + unz1,
                        UNA + UNB + M2 + unz1);
    }

    /**
     * A message keeps its group around it; at the UNG boundary, a group
     * is one section.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void groupsAreKeptAroundTheirMessages() throws Exception
    {
        String ung = "UNG+PAYMUL+S+R+261028:1200+G7+UN+D:96A'\n";
        String file = UNB + ung + M1 + M2 + "UNE+2+G7'\n"
                + "UNZ+1+REF0001'\n";
        assertThat(splitAndCheckResume(splitter, file, config("edifact")))
                .containsExactly(
                        UNB + ung + M1 + "UNE+1+G7'\nUNZ+1+REF0001'\n",
                        UNB + ung + M2 + "UNE+1+G7'\nUNZ+1+REF0001'\n");
        assertThat(splitAndCheckResume(splitter, file,
                config("edifact").withBoundary("UNG")))
                .containsExactly(UNB + ung + M1 + M2
                        + "UNE+2+G7'\nUNZ+1+REF0001'\n");
    }

    /**
     * At the UNB boundary an interchange is one section, and each
     * interchange's UNA applies to it alone.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void interchangeBoundaryAndCustomDelimitersPerInterchange()
            throws Exception
    {
        String second = "UNA|*.# !UNB*UNOC|3*S*R*261028|1200*R2!"
                + "UNH*9*X|D|96A|UN!FTX*A***B#!C!UNT*3*9!UNZ*1*R2!";
        assertThat(splitAndCheckResume(splitter, FILE + second,
                config("edifact").withBoundary("UNB")))
                .containsExactly(FILE, second);
        // The second interchange's own UNA applies to it only; a third
        // without UNA is back to the defaults.
        String third = UNB.replace("REF0001", "REF0003") + M1
                + "UNZ+1+REF0003'\n";
        assertThat(splitAndCheckResume(splitter, FILE + second + third,
                config("edifact"))).containsExactly(
                        UNA + UNB + M1 + "UNZ+1+REF0001'\n",
                        UNA + UNB + M2 + "UNZ+1+REF0001'\n",
                        "UNA|*.# !UNB*UNOC|3*S*R*261028|1200*R2!"
                                + "UNH*9*X|D|96A|UN!FTX*A***B#!C!UNT*3*9!"
                                + "UNZ*1*R2!",
                        UNB.replace("REF0001", "REF0003") + M1
                                + "UNZ+1+REF0003'\n");
    }

    /** Mismatched references and broken structure fail the file. */
    @Test
    void controlReferencesAndStructureAreChecked()
    {
        assertThat(splitError(splitter,
                UNB + M1.replace("UNT+3+1", "UNT+3+9") + "UNZ+1+REF0001'",
                config("edifact")).code())
                .isEqualTo(ErrorCode.PARSE_EDIFACT_CONTROL_MISMATCH);
        assertThat(splitError(splitter, UNB + M1 + "UNZ+1+OTHER'",
                config("edifact")).code())
                .isEqualTo(ErrorCode.PARSE_EDIFACT_CONTROL_MISMATCH);
        assertThat(splitError(splitter, UNB + "UNH+1+X'BGM+1'",
                config("edifact")).code())
                .isEqualTo(ErrorCode.PARSE_EDIFACT_UNBALANCED);
        assertThat(splitError(splitter, "BGM+1'", config("edifact")).code())
                .isEqualTo(ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE);
        assertThat(splitError(splitter, UNB + "BGM+1'",
                config("edifact")).code())
                .isEqualTo(ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT);
    }
}
