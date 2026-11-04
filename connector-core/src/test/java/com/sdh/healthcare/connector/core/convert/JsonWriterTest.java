/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JsonWriter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class JsonWriterTest
{

    /**
     * One string, as JSON.
     *
     * @param text the string
     * @return its JSON
     */
    private static byte[] one(String text)
    {
        return new JsonWriter(0).value(text).toBytes();
    }

    /**
     * One string written in pieces, as JSON.
     *
     * @param pieces the pieces
     * @return its JSON
     */
    private static byte[] pieces(String... pieces)
    {
        return new JsonWriter(0).value(List.of(pieces)).toBytes();
    }

    /**
     * Strings are UTF-8 exactly as {@code String.getBytes}, with JSON's
     * escapes.
     */
    @Test
    void writesStringsAsUtf8WithEscapes()
    {
        String text = "a\"\\\n\r\t\b\f\u0001é€😀\uD83Dx\uDE00";
        String escaped = "a\\\"\\\\\\n\\r\\t\\b\\f\\u0001é€😀"
                + "\uD83Dx\uDE00";
        assertThat(one(text))
                .isEqualTo(("\"" + escaped + "\"").getBytes(UTF_8));
        assertThat(one(text)).containsSubsequence("??".getBytes(UTF_8)[0]);
    }

    /**
     * Pieces are written exactly as their joined text would be, wherever
     * the joins fall: inside a surrogate pair, around empty pieces, and
     * with a high surrogate left unpaired at the end of a piece or of the
     * whole text.
     */
    @Test
    void piecesAreWrittenAsTheirJoinedText()
    {
        String[][] cases = {
            {"abc", "def"},
            {"a\uD83D", "\uDE00b"},
            {"a\uD83D", "", "\uDE00b"},
            {"a\uD83D", "x"},
            {"a\uD83D", "\uD83D", "\uDE00"},
            {"a\uD83D"},
            {"a\uD83D", ""},
            {},
            {"", ""},
            {"\"q\"\n", "\u0001é"},
        };
        for (String[] parts : cases)
        {
            assertThat(pieces(parts)).as(String.join("|", parts))
                    .isEqualTo(one(String.join("", parts)));
        }
    }

    /**
     * A value after pieces is separated by a comma, and nothing carries
     * over from one string to the next.
     */
    @Test
    void piecesTakePartInTheStructure()
    {
        byte[] json = new JsonWriter(0).beginArray()
                .value(List.of("a\uD83D")).value("\uDE00").endArray()
                .toBytes();
        assertThat(new String(json, UTF_8)).isEqualTo("[\"a?\",\"?\"]");
    }

    /**
     * UTF-8 bytes written as they are give exactly what writing the
     * decoded text gives.
     */
    @Test
    void utf8BytesAreWrittenAsTheirDecodedText()
    {
        String text = "plain \" \\ \n\r\t\b\f \u0000\u001f é € 😀"
                + " end";
        byte[] bytes = text.getBytes(UTF_8);
        assertThat(new JsonWriter(0).valueUtf8(bytes, 0, bytes.length)
                .toBytes()).isEqualTo(one(text));
        byte[] pieces = new JsonWriter(0).beginString().utf8(bytes, 0, 7)
                .utf8(bytes, 7, bytes.length).endString().toBytes();
        assertThat(pieces).isEqualTo(one(text));
    }

    /**
     * From {@link JsonWriter#EXACT_SIZE_FROM}, the JSON is counted first
     * and then written into an array of exactly its size; the bytes are
     * the same as written the ordinary way.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void largeJsonIsBuiltAtExactlyItsSize() throws Exception
    {
        String big = "xé😀\"\n".repeat(200_000);
        byte[] bytes = big.getBytes(UTF_8);
        JsonWriter.Body body = writer ->
        {
            writer.beginArray().value(big).valueUtf8(bytes, 0, bytes.length)
                    .value(List.of("a", big)).literal("1");
            writer.endArray();
        };
        byte[] small = JsonWriter.build(16, body);
        byte[] exact = JsonWriter.build(JsonWriter.EXACT_SIZE_FROM, body);
        assertThat(exact).isEqualTo(small);
        assertThat(exact.length).isGreaterThan(JsonWriter.EXACT_SIZE_FROM);
    }
}
