/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * CSV, X12 and EDIFACT conversion (design §6.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class FlatConvertersTest
{

    /** CSV without a header. */
    static final SplitConfig CSV = SplitConfig.defaults("csv");

    /** CSV with a header. */
    static final SplitConfig CSV_HEADER = CSV.withOption("csv.header", "true");

    /** An X12 interchange header. */
    static final String ISA = "ISA*00*          *00*          "
            + "*ZZ*SUBMITTER01    *ZZ*PAYER0000001   "
            + "*261028*1200*^*00501*000000001*0*P*:~\n";

    /**
     * Converts text.
     *
     * @param converter the converter
     * @param input     the record, as text
     * @param config    the split settings
     * @return the JSON, as text
     * @throws Exception if conversion fails
     */
    static String convert(RecordConverter converter, String input,
            SplitConfig config) throws Exception
    {
        return new String(converter.convert(input.getBytes(UTF_8), config),
                UTF_8);
    }

    /**
     * The code conversion fails with.
     *
     * @param converter the converter
     * @param input     the record
     * @param config    the split settings
     * @return the error code
     * @throws AssertionError if conversion succeeds
     */
    static ErrorCode failure(RecordConverter converter, byte[] input,
            SplitConfig config)
    {
        try
        {
            converter.convert(input, config);
        }
        catch (ConversionException expected)
        {
            return expected.code();
        }
        throw new AssertionError("expected a conversion failure");
    }

    // ---- CSV ----

    /**
     * A record is an array of its fields, as written, without the header.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void csvRowBecomesAnArrayOfStringsByPositionWithoutTheHeader()
            throws Exception
    {
        CsvConverter converter = new CsvConverter();
        assertThat(convert(converter, "id,name,note,zip\r\n"
                + "1,\"Doe, Jane\",,\"say \"\"hi\"\"\nthen leave\"\r\n",
                CSV_HEADER))
                .isEqualTo("[\"1\",\"Doe, Jane\",\"\","
                        + "\"say \\\"hi\\\"\\nthen leave\"]");
        // Leading zeros, spaces and a quote inside an unquoted field are
        // kept as written.
        assertThat(convert(converter, "a,b,c\n007, x ,5\"7\n", CSV_HEADER))
                .isEqualTo("[\"007\",\" x \",\"5\\\"7\"]");
        assertThat(convert(converter, "1,2,3", CSV))
                .isEqualTo("[\"1\",\"2\",\"3\"]");
        assertThat(convert(converter, "a\tb\n1\t\n",
                CSV_HEADER.withOption("csv.delimiter", "\\t")))
                .isEqualTo("[\"1\",\"\"]");
        // A UTF-8 byte order mark at the start of the header is ignored;
        // non-ASCII text is kept.
        assertThat(convert(converter, "﻿name\nJosé\n", CSV_HEADER))
                .isEqualTo("[\"José\"]");
    }

    /** Wrong field counts, bad encoding and bad quoting fail the record. */
    @Test
    void csvFieldCountMismatchEncodingAndQuotingFailTheRecord()
    {
        CsvConverter converter = new CsvConverter();
        assertThat(failure(converter, "a,b,c\n1,2\n".getBytes(UTF_8),
                CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_FIELD_COUNT);
        assertThat(failure(converter, "a,b\n1,2,3\n".getBytes(UTF_8),
                CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_FIELD_COUNT);
        assertThat(failure(converter, "a\nJosé\n".getBytes(ISO_8859_1),
                CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
        assertThat(failure(converter, "a,b\n\"x\"y,2\n".getBytes(UTF_8),
                CSV_HEADER)).isEqualTo(ErrorCode.CONVERT_CSV_MALFORMED);
    }

    // ---- X12 ----

    /**
     * Elements keep their positions, empties, spaces and composites.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void x12KeepsEmptyElementsSpacesAndCompositesByPosition()
            throws Exception
    {
        String input = ISA + "GS*HC*S*R*20261028*1200*1*X*005010X222A1~\n"
                + "ST*837*0001~\n"
                + "NM1*41*2*SAMPLE CLINIC*****46*123456789~\n"
                + "SV1*HC:99213:25*150.00*UN*1***1~\n"
                + "SE*4*0001~\nGE*1*1~\nIEA*1*000000001~\n";
        String json = convert(new X12Converter(), input,
                SplitConfig.defaults("x12"));
        assertThat(json).startsWith("{\"format\":\"X12\",\"segments\":["
                + "{\"segment_id\":\"ISA\",\"elements\":[\"00\","
                + "\"          \",\"00\"");
        assertThat(json).contains("{\"segment_id\":\"NM1\",\"elements\":["
                + "\"41\",\"2\",\"SAMPLE CLINIC\",\"\",\"\",\"\","
                + "\"\",\"46\",\"123456789\"]}");
        assertThat(json).contains("{\"segment_id\":\"SV1\",\"elements\":["
                + "\"HC:99213:25\",\"150.00\",\"UN\",\"1\",\"\","
                + "\"\",\"1\"]}");
        assertThat(json).endsWith("{\"segment_id\":\"IEA\",\"elements\":"
                + "[\"1\",\"000000001\"]}]}");
    }

    /**
     * Newline terminators with CRLF line ends.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void x12WithNewlineTerminatorsAndCrlf() throws Exception
    {
        String input = ISA.replace('*', '|').replace("~\n", "\r\n")
                + "ST|837|0001\r\nSE|2|0001\r\nIEA|1|000000001\r\n";
        assertThat(convert(new X12Converter(), input,
                SplitConfig.defaults("x12")))
                .contains("{\"segment_id\":\"SE\",\"elements\":"
                        + "[\"2\",\"0001\"]}");
    }

    /** Malformed input and bad encoding fail the record. */
    @Test
    void x12InvalidInputFailsTheRecord()
    {
        assertThat(failure(new X12Converter(), "GS*HC~".getBytes(UTF_8),
                SplitConfig.defaults("x12")))
                .isEqualTo(ErrorCode.CONVERT_X12_MALFORMED);
        assertThat(failure(new X12Converter(),
                (ISA + "NM1*é~").getBytes(ISO_8859_1),
                SplitConfig.defaults("x12")))
                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
        // Separators are single ASCII characters; part of a multi-byte
        // character can't be one.
        assertThat(failure(new X12Converter(),
                ISA.replace("~\n", "§\n").getBytes(UTF_8),
                SplitConfig.defaults("x12")))
                .isEqualTo(ErrorCode.CONVERT_X12_MALFORMED);
    }

    // ---- EDIFACT ----

    /**
     * Elements are lists of components, with release characters applied.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void edifactElementsAreComponentListsWithReleaseCharactersApplied()
            throws Exception
    {
        String input = "UNA:+.? '\n"
                + "UNB+UNOC:3+SENDER01:14+RECEIVER01:14+261028:1200+REF0001'\n"
                + "UNH+1+PAYMUL:D:96A:UN'\n"
                + "NAD+PE++SAMPLE CLINIC+1 MAIN ST:SUITE 2'\n"
                + "FTX+AAA+++NOTE WITH ?+ PLUS AND ?:COLON::EMPTY'\n"
                + "UNT+4+1'\nUNZ+1+REF0001'\n";
        String json = convert(new EdifactConverter(), input,
                SplitConfig.defaults("edifact"));
        assertThat(json).startsWith("{\"format\":\"EDIFACT\",\"segments\":"
                + "[{\"segment_id\":\"UNB\",\"elements\":"
                + "[[\"UNOC\",\"3\"],[\"SENDER01\",\"14\"]");
        assertThat(json).contains("{\"segment_id\":\"NAD\",\"elements\":"
                + "[[\"PE\"],[\"\"],[\"SAMPLE CLINIC\"],"
                + "[\"1 MAIN ST\",\"SUITE 2\"]]}");
        assertThat(json).contains("{\"segment_id\":\"FTX\",\"elements\":"
                + "[[\"AAA\"],[\"\"],[\"\"],"
                + "[\"NOTE WITH + PLUS AND :COLON\",\"\",\"EMPTY\"]]}");
    }

    /**
     * The character set comes from the UNB syntax identifier.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void edifactCharsetComesFromTheSyntaxIdentifier() throws Exception
    {
        byte[] latin1 = ("UNB+UNOC:3+S+R+261028:1200+R1'UNH+1+X:D:96A:UN'"
                + "NAD+PE++Müller'UNT+3+1'UNZ+1+R1'")
                .getBytes(ISO_8859_1);
        assertThat(new String(new EdifactConverter().convert(latin1,
                SplitConfig.defaults("edifact")), UTF_8))
                .contains("\"Müller\"");
        byte[] ascii = new String(latin1, ISO_8859_1).replace("UNOC", "UNOA")
                .getBytes(ISO_8859_1);
        assertThat(failure(new EdifactConverter(), ascii,
                SplitConfig.defaults("edifact")))
                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
        byte[] unknown = "UNB+UNOX:3+S+R+261028:1200+R1'UNZ+0+R1'"
                .getBytes(ISO_8859_1);
        assertThat(failure(new EdifactConverter(), unknown,
                SplitConfig.defaults("edifact")))
                .isEqualTo(ErrorCode.CONVERT_EDIFACT_CHARSET);
        assertThatThrownBy(() -> new EdifactConverter().convert(
                "UNB+UNOC:3+S'NAD+X".getBytes(ISO_8859_1),
                SplitConfig.defaults("edifact")))
                .isInstanceOf(ConversionException.class);
    }

    /**
     * An invalid byte is reported at its position in the record, also
     * when release characters come before it in the same component: the
     * component is decoded without them, and the position is mapped back.
     */
    @Test
    void edifactEncodingErrorIsReportedAtTheInvalidByte()
    {
        String head = "UNB+UNOY:3+S+R+261028:1200+R1'NAD+PE++";
        String tail = "'UNZ+1+R1'";
        // 0xFF is never valid in UTF-8. "#" marks where it goes.
        for (String component : new String[] {"AB#C", "A?+B?:#C", "??#"})
        {
            String text = head + component + tail;
            int expected = text.indexOf('#');
            byte[] input = text.getBytes(ISO_8859_1);
            input[expected] = (byte) 0xFF;
            assertThatThrownBy(() -> new EdifactConverter().convert(input,
                    SplitConfig.defaults("edifact")))
                    .as(component)
                    .isInstanceOfSatisfying(ConversionException.class,
                            error ->
                    {
                        assertThat(error.code())
                                .isEqualTo(ErrorCode.CONVERT_INVALID_ENCODING);
                        assertThat(error.position()).isEqualTo(expected);
                    });
        }
    }

    /**
     * Records from {@code JsonWriter.EXACT_SIZE_FROM} are written twice,
     * counting first. The result must be what the ordinary path gives: the
     * same JSON for a record just under the size as for the same content
     * made larger.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void largeRecordsConvertTheSameWay() throws Exception
    {
        String x12Body = "NM1*IL*1*MÜLLER*JOSÉ~\n";
        String x12 = ISA + x12Body.repeat(
                JsonWriter.EXACT_SIZE_FROM / x12Body.length() + 10)
                + "IEA*1*1~\n";
        String json = convert(new X12Converter(), x12,
                SplitConfig.defaults("x12"));
        assertThat(json)
                .startsWith("{\"format\":\"X12\",\"segments\":"
                        + "[{\"segment_id\":\"ISA\"")
                .contains("{\"segment_id\":\"NM1\",\"elements\":"
                        + "[\"IL\",\"1\",\"MÜLLER\",\"JOSÉ\"]}")
                .endsWith("{\"segment_id\":\"IEA\",\"elements\":"
                        + "[\"1\",\"1\"]}]}");
        String edifactBody = "NAD+PE++M?+LLER:X'\n";
        String edifact = "UNB+UNOY:3+S+R'" + edifactBody.repeat(
                JsonWriter.EXACT_SIZE_FROM / edifactBody.length() + 10)
                + "UNZ+1+R'";
        assertThat(convert(new EdifactConverter(), edifact,
                SplitConfig.defaults("edifact")))
                .contains("{\"segment_id\":\"NAD\",\"elements\":"
                        + "[[\"PE\"],[\"\"],[\"M+LLER\",\"X\"]]}");
        String field = "say \"\"hi\"\" é\n"
                .repeat(JsonWriter.EXACT_SIZE_FROM / 10);
        assertThat(convert(new CsvConverter(), "a,b\n1,\"" + field + "\"\n",
                CSV_HEADER))
                .isEqualTo("[\"1\",\"" + "say \\\"hi\\\" é\\n"
                        .repeat(JsonWriter.EXACT_SIZE_FROM / 10) + "\"]");
    }

    /** The built-in converters are found; FHIR and XML have none. */
    @Test
    void registryFindsTheBuiltInConvertersAndNoneForFhir()
    {
        ConverterRegistry registry = ConverterRegistry.load();
        assertThat(registry.get("csv")).containsInstanceOf(CsvConverter.class);
        assertThat(registry.get("ccda"))
                .containsInstanceOf(CcdaConverter.class);
        assertThat(registry.get("x12")).containsInstanceOf(X12Converter.class);
        assertThat(registry.get("edifact"))
                .containsInstanceOf(EdifactConverter.class);
        assertThat(registry.get("fhir")).isEmpty();
        assertThat(registry.get("xml")).isEmpty();
    }
}
