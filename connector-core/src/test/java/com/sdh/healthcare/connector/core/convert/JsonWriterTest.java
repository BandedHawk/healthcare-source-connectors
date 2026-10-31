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
class JsonWriterTest {

    private static byte[] one(String s) {
        return new JsonWriter(0).value(s).toBytes();
    }

    private static byte[] pieces(String... pieces) {
        return new JsonWriter(0).value(List.of(pieces)).toBytes();
    }

    /** Strings are UTF-8 exactly as {@code String.getBytes}, with JSON's escapes. */
    @Test
    void writesStringsAsUtf8WithEscapes() {
        String text = "a\"\\\n\r\t\b\f\u0001é€\uD83D\uDE00\uD83Dx\uDE00";
        String escaped = "a\\\"\\\\\\n\\r\\t\\b\\f\\u0001é€\uD83D\uDE00\uD83Dx\uDE00";
        assertThat(one(text)).isEqualTo(("\"" + escaped + "\"").getBytes(UTF_8));
        assertThat(one(text)).containsSubsequence("??".getBytes(UTF_8)[0]);
    }

    /**
     * Pieces are written exactly as their joined text would be, wherever the joins fall: inside a surrogate pair,
     * around empty pieces, and with a high surrogate left unpaired at the end of a piece or of the whole text.
     */
    @Test
    void piecesAreWrittenAsTheirJoinedText() {
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
        for (String[] parts : cases) {
            assertThat(pieces(parts)).as(String.join("|", parts)).isEqualTo(one(String.join("", parts)));
        }
    }

    /** A value after pieces is separated by a comma, and nothing carries over from one string to the next. */
    @Test
    void piecesTakePartInTheStructure() {
        byte[] json = new JsonWriter(0).beginArray().value(List.of("a\uD83D")).value("\uDE00").endArray().toBytes();
        assertThat(new String(json, UTF_8)).isEqualTo("[\"a?\",\"?\"]");
    }
}
