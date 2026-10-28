package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

class X12SplitterTest {
    private final X12Splitter splitter = new X12Splitter();

    static final String ISA = "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       "
            + "*261028*1200*^*00501*000000905*0*P*:~\n";
    static final String GS1 = "GS*HC*S*R*20261028*1200*1*X*005010X222A1~\n";
    static final String ST1 = "ST*837*0001~\nBHT*0019*00*1*20261028*1200*CH~\nSE*3*0001~\n";
    static final String ST2 = "ST*837*0002~\nBHT*0019*00*2*20261028*1200*CH~\nSE*3*0002~\n";
    static final String GE1 = "GE*2*1~\n";
    static final String GS2 = "GS*HC*S*R*20261028*1200*2*X*005010X222A1~\n";
    static final String ST3 = "ST*837*0003~\nSE*2*0003~\n";
    static final String GE2 = "GE*1*2~\n";
    static final String IEA = "IEA*2*000000905~\n";
    static final String FILE = ISA + GS1 + ST1 + ST2 + GE1 + GS2 + ST3 + GE2 + IEA;

    @Test
    void eachTransactionSetBecomesAStandaloneInterchange() throws Exception {
        String ge1 = "GE*1*1~\n";
        String ge2 = "GE*1*2~\n";
        String iea = "IEA*1*000000905~\n";
        assertThat(splitAndCheckResume(splitter, FILE, cfg("x12"))).containsExactly(
                ISA + GS1 + ST1 + ge1 + iea,
                ISA + GS1 + ST2 + ge1 + iea,
                ISA + GS2 + ST3 + ge2 + iea);
    }

    @Test
    void groupBoundary() throws Exception {
        String iea = "IEA*1*000000905~\n";
        assertThat(splitAndCheckResume(splitter, FILE, cfg("x12").withBoundary("GS"))).containsExactly(
                ISA + GS1 + ST1 + ST2 + GE1 + iea,
                ISA + GS2 + ST3 + GE2 + iea);
    }

    @Test
    void interchangeBoundaryAndSeveralInterchanges() throws Exception {
        String isa2 = ISA.replace("000000905", "000000906").replace("~\n", "~");
        String second = isa2 + "GS*HC*S*R*20261028*1200*7*X*005010X222A1~ST*837*0009~SE*2*0009~GE*1*7~IEA*1*000000906~";
        assertThat(splitAndCheckResume(splitter, FILE + second, cfg("x12").withBoundary("ISA")))
                .containsExactly(FILE, second);
        // Delimiters and line style are taken per interchange.
        assertThat(splitAndCheckResume(splitter, FILE + second, cfg("x12"))).hasSize(4).last().isEqualTo(
                isa2 + "GS*HC*S*R*20261028*1200*7*X*005010X222A1~ST*837*0009~SE*2*0009~GE*1*7~IEA*1*000000906~");
    }

    @Test
    void ta1BecomesItsOwnInterchange() throws Exception {
        String ta1 = "TA1*000000905*261028*1200*A*000~\n";
        String file = ISA + ta1 + GS2 + ST3 + GE2 + "IEA*1*000000905~\n";
        assertThat(splitAndCheckResume(splitter, file, cfg("x12"))).containsExactly(
                ISA + ta1 + "IEA*0*000000905~\n",
                ISA + GS2 + ST3 + GE2 + "IEA*1*000000905~\n");
    }

    @Test
    void otherDelimiters() throws Exception {
        String isa = ISA.replace('*', '|').replace("~\n", "\n");
        String file = isa + "GS|HC|S|R|20261028|1200|1|X|005010X222A1\nST|837|0001\nSE|2|0001\nGE|1|1\nIEA|1|000000905\n";
        assertThat(splitAndCheckResume(splitter, file, cfg("x12"))).containsExactly(file);
    }

    @Test
    void truncatedFileIsUnbalanced() {
        String truncated = ISA + GS1 + "ST*837*0001~\nBHT*0019*00*1*20261028*1200*CH~\n";
        assertThat(splitError(splitter, truncated, cfg("x12")).code()).isEqualTo(ErrorCode.PARSE_X12_UNBALANCED);
        // Missing IEA.
        assertThat(splitError(splitter, ISA + GS1 + ST1 + GE1, cfg("x12")).code())
                .isEqualTo(ErrorCode.PARSE_X12_UNBALANCED);
        assertThat(splitError(splitter, ISA + GS1 + ST1 + "GE*1*9~\n" + IEA, cfg("x12")).code())
                .isEqualTo(ErrorCode.PARSE_X12_CONTROL_MISMATCH);
    }

    @Test
    void controlNumberMismatchAndJunk() {
        assertThat(splitError(splitter, ISA + GS1 + "ST*837*0001~\nSE*2*0002~\n", cfg("x12")).code())
                .isEqualTo(ErrorCode.PARSE_X12_CONTROL_MISMATCH);
        assertThat(splitError(splitter, "hello", cfg("x12")).code()).isEqualTo(ErrorCode.PARSE_X12_INVALID_ENVELOPE);
        assertThat(splitError(splitter, ISA + "BHT*1~\n", cfg("x12")).code())
                .isEqualTo(ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT);
    }
}
