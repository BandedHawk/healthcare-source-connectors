package com.sdh.healthcare.connector.core.convert;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

/** CSV, X12 and EDIFACT conversion (design §6.4). */
class FlatConvertersTest {
    static final SplitConfig CSV = SplitConfig.defaults("csv");
    static final SplitConfig CSV_HEADER = CSV.withOption("csv.header", "true");

    static String convert(SectionConverter c, String in, SplitConfig cfg) throws Exception {
        return new String(c.convert(in.getBytes(UTF_8), cfg), UTF_8);
    }

    static ErrorCode failure(SectionConverter c, byte[] in, SplitConfig cfg) {
        try {
            c.convert(in, cfg);
        } catch (ConversionException e) {
            return e.code();
        }
        throw new AssertionError("expected a conversion failure");
    }

    // ---- CSV ----

    @Test
    void csvRecordBecomesAnArrayOfStringsByPositionWithoutTheHeader() throws Exception {
        CsvConverter c = new CsvConverter();
        assertThat(convert(c, "id,name,note,zip\r\n1,\"Doe, Jane\",,\"say \"\"hi\"\"\nthen leave\"\r\n", CSV_HEADER))
                .isEqualTo("[\"1\",\"Doe, Jane\",\"\",\"say \\\"hi\\\"\\nthen leave\"]");
        // Leading zeros, spaces and a quote inside an unquoted field are kept as written.
        assertThat(convert(c, "a,b,c\n007, x ,5\"7\n", CSV_HEADER)).isEqualTo("[\"007\",\" x \",\"5\\\"7\"]");
        assertThat(convert(c, "1,2,3", CSV)).isEqualTo("[\"1\",\"2\",\"3\"]");
        assertThat(convert(c, "a\tb\n1\t\n", CSV_HEADER.withOption("csv.delimiter", "\\t"))).isEqualTo("[\"1\",\"\"]");
        // A UTF-8 byte order mark at the start of the header is ignored; non-ASCII text is kept.
        assertThat(convert(c, "\uFEFFname\nJosé\n", CSV_HEADER)).isEqualTo("[\"José\"]");
    }

    @Test
    void csvFieldCountMismatchEncodingAndQuotingFailTheSection() {
        CsvConverter c = new CsvConverter();
        assertThat(failure(c, "a,b,c\n1,2\n".getBytes(UTF_8), CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_FIELD_COUNT);
        assertThat(failure(c, "a,b\n1,2,3\n".getBytes(UTF_8), CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_FIELD_COUNT);
        assertThat(failure(c, "a\nJos\u00e9\n".getBytes(ISO_8859_1), CSV_HEADER))
                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
        assertThat(failure(c, "a,b\n\"x\"y,2\n".getBytes(UTF_8), CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_MALFORMED);
    }

    // ---- X12 ----

    static final String ISA = "ISA*00*          *00*          *ZZ*SUBMITTER01    *ZZ*PAYER0000001   "
            + "*261028*1200*^*00501*000000001*0*P*:~\n";

    @Test
    void x12KeepsEmptyElementsSpacesAndCompositesByPosition() throws Exception {
        String in = ISA + "GS*HC*S*R*20261028*1200*1*X*005010X222A1~\nST*837*0001~\n"
                + "NM1*41*2*SAMPLE CLINIC*****46*123456789~\nSV1*HC:99213:25*150.00*UN*1***1~\n"
                + "SE*4*0001~\nGE*1*1~\nIEA*1*000000001~\n";
        String json = convert(new X12Converter(), in, SplitConfig.defaults("x12"));
        assertThat(json).startsWith("{\"format\":\"X12\",\"segments\":[{\"segment_id\":\"ISA\",\"elements\":[\"00\","
                + "\"          \",\"00\"");
        assertThat(json).contains("{\"segment_id\":\"NM1\",\"elements\":[\"41\",\"2\",\"SAMPLE CLINIC\",\"\",\"\",\"\","
                + "\"\",\"46\",\"123456789\"]}");
        assertThat(json).contains("{\"segment_id\":\"SV1\",\"elements\":[\"HC:99213:25\",\"150.00\",\"UN\",\"1\",\"\","
                + "\"\",\"1\"]}");
        assertThat(json).endsWith("{\"segment_id\":\"IEA\",\"elements\":[\"1\",\"000000001\"]}]}");
    }

    @Test
    void x12WithNewlineTerminatorsAndCrlf() throws Exception {
        String in = ISA.replace('*', '|').replace("~\n", "\r\n") + "ST|837|0001\r\nSE|2|0001\r\nIEA|1|000000001\r\n";
        assertThat(convert(new X12Converter(), in, SplitConfig.defaults("x12")))
                .contains("{\"segment_id\":\"SE\",\"elements\":[\"2\",\"0001\"]}");
    }

    @Test
    void x12InvalidInputFailsTheSection() {
        assertThat(failure(new X12Converter(), "GS*HC~".getBytes(UTF_8), SplitConfig.defaults("x12")))
                .isEqualTo(ErrorCode.CONVERT_X12_MALFORMED);
        assertThat(failure(new X12Converter(), (ISA + "NM1*\u00e9~").getBytes(ISO_8859_1), SplitConfig.defaults("x12")))
                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
    }

    // ---- EDIFACT ----

    @Test
    void edifactElementsAreComponentListsWithReleaseCharactersApplied() throws Exception {
        String in = "UNA:+.? '\nUNB+UNOC:3+SENDER01:14+RECEIVER01:14+261028:1200+REF0001'\n"
                + "UNH+1+PAYMUL:D:96A:UN'\nNAD+PE++SAMPLE CLINIC+1 MAIN ST:SUITE 2'\n"
                + "FTX+AAA+++NOTE WITH ?+ PLUS AND ?:COLON::EMPTY'\nUNT+4+1'\nUNZ+1+REF0001'\n";
        String json = convert(new EdifactConverter(), in, SplitConfig.defaults("edifact"));
        assertThat(json).startsWith("{\"format\":\"EDIFACT\",\"segments\":[{\"segment_id\":\"UNB\",\"elements\":"
                + "[[\"UNOC\",\"3\"],[\"SENDER01\",\"14\"]");
        assertThat(json).contains("{\"segment_id\":\"NAD\",\"elements\":[[\"PE\"],[\"\"],[\"SAMPLE CLINIC\"],"
                + "[\"1 MAIN ST\",\"SUITE 2\"]]}");
        assertThat(json).contains("{\"segment_id\":\"FTX\",\"elements\":[[\"AAA\"],[\"\"],[\"\"],"
                + "[\"NOTE WITH + PLUS AND :COLON\",\"\",\"EMPTY\"]]}");
    }

    @Test
    void edifactCharsetComesFromTheSyntaxIdentifier() throws Exception {
        byte[] latin1 = "UNB+UNOC:3+S+R+261028:1200+R1'UNH+1+X:D:96A:UN'NAD+PE++M\u00fcller'UNT+3+1'UNZ+1+R1'"
                .getBytes(ISO_8859_1);
        assertThat(new String(new EdifactConverter().convert(latin1, SplitConfig.defaults("edifact")), UTF_8))
                .contains("\"M\u00fcller\"");
        byte[] ascii = new String(latin1, ISO_8859_1).replace("UNOC", "UNOA").getBytes(ISO_8859_1);
        assertThat(failure(new EdifactConverter(), ascii, SplitConfig.defaults("edifact")))
                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
        byte[] unknown = "UNB+UNOX:3+S+R+261028:1200+R1'UNZ+0+R1'".getBytes(ISO_8859_1);
        assertThat(failure(new EdifactConverter(), unknown, SplitConfig.defaults("edifact")))
                .isEqualTo(ErrorCode.CONVERT_EDIFACT_CHARSET);
        assertThatThrownBy(() -> new EdifactConverter().convert("UNB+UNOC:3+S'NAD+X".getBytes(ISO_8859_1),
                SplitConfig.defaults("edifact"))).isInstanceOf(ConversionException.class);
    }

    @Test
    void registryFindsTheBuiltInConvertersAndNoneForFhir() {
        ConverterRegistry r = ConverterRegistry.load();
        assertThat(r.get("csv")).containsInstanceOf(CsvConverter.class);
        assertThat(r.get("ccda")).containsInstanceOf(CcdaConverter.class);
        assertThat(r.get("x12")).containsInstanceOf(X12Converter.class);
        assertThat(r.get("edifact")).containsInstanceOf(EdifactConverter.class);
        assertThat(r.get("fhir")).isEmpty();
        assertThat(r.get("xml")).isEmpty();
    }
}
