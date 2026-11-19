/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/**
 * {@link Fields#failure}: what a failure tells without its messages.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class FieldsTest
{

    /** A message standing in for one that quotes a document. */
    static final String VALUE = "Smith, born 1970-01-01";

    /** Creates the tests; JUnit runs each test on a new instance. */
    FieldsTest()
    {
    }

    /**
     * Throws, so that the frames are this class's.
     *
     * @throws IllegalStateException always, with {@link #VALUE}
     */
    static void check()
    {
        throw new IllegalStateException(VALUE);
    }

    /**
     * A failure's class, its chain of causes and where the innermost was
     * thrown are kept, and no message is, of the failure or its cause.
     */
    @Test
    void keepsTheCausesAndWhereButNoMessage()
    {
        Throwable failure;
        try
        {
            check();
            throw new AssertionError("check() returned");
        }
        catch (IllegalStateException thrown)
        {
            failure = new ExecutionException(VALUE, thrown);
        }
        Attributes attributes = Fields.of(Attr.STAGE, "convert")
                .failure(failure).toAttributes();
        assertThat(attributes.get(AttributeKey.stringKey("sdh.reason")))
                .isEqualTo("ExecutionException");
        assertThat(attributes.get(AttributeKey.stringKey("sdh.causes")))
                .isEqualTo("ExecutionException,IllegalStateException");
        String where = attributes.get(AttributeKey.stringKey("sdh.thrown_at"));
        assertThat(where).startsWith(FieldsTest.class.getName() + ".check:")
                .contains(".keepsTheCausesAndWhereButNoMessage:");
        assertThat(where.split(",")).hasSize(Fields.FRAMES);
        assertThat(attributes.toString()).doesNotContain("Smith")
                .doesNotContain("1970");
        assertThat(Fields.of().failure(failure).toLogString())
                .as("one key=value pair to a space, for the log follower")
                .matches("( \\w+=\\S+){3}");
    }

    /**
     * A chain that comes round to itself ends at the first repeat, and a
     * failure with no frames adds no place.
     */
    @Test
    void endsACircularChainAndSkipsMissingFrames()
    {
        IllegalStateException outer = new IllegalStateException();
        IllegalArgumentException inner = new IllegalArgumentException(outer);
        outer.initCause(inner);
        outer.setStackTrace(new StackTraceElement[0]);
        inner.setStackTrace(new StackTraceElement[0]);
        Attributes attributes = Fields.of().failure(outer).toAttributes();
        assertThat(attributes.get(AttributeKey.stringKey("sdh.causes")))
                .isEqualTo("IllegalStateException,IllegalArgumentException");
        assertThat(attributes.get(AttributeKey.stringKey("sdh.thrown_at")))
                .isNull();
        assertThat(Fields.of().failure(null).toAttributes().isEmpty())
                .isTrue();
    }
}
