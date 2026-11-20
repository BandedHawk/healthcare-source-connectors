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
import java.sql.SQLException;
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
     * A database failure thrown in a driver's code says where in this
     * product's code the call was made, and its SQLSTATE, but not its
     * message; a failure whose top frames are already this product's adds
     * no second place.
     */
    @Test
    void saysWhereOurCodeCalledAndTheSqlState()
    {
        SQLException deadlock = new SQLException(VALUE, "40P01");
        deadlock.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.postgresql.core.v3.QueryExecutorImpl",
                    "receiveErrorResponse", "QueryExecutorImpl.java", 2993),
            new StackTraceElement("org.postgresql.core.v3.QueryExecutorImpl",
                    "processResults", "QueryExecutorImpl.java", 2656),
            new StackTraceElement("org.postgresql.core.v3.QueryExecutorImpl",
                    "execute", "QueryExecutorImpl.java", 446),
            new StackTraceElement("org.postgresql.jdbc.PgStatement",
                    "executeInternal", "PgStatement.java", 525),
            new StackTraceElement(
                    "com.sdh.healthcare.connector.stage.StageRegistry",
                    "store", "StageRegistry.java", 1012)});
        Attributes attributes = Fields.of()
                .failure(new IllegalStateException(VALUE, deadlock))
                .toAttributes();
        assertThat(attributes.get(AttributeKey.stringKey("sdh.thrown_at")))
                .startsWith("org.postgresql.core.v3.QueryExecutorImpl");
        assertThat(attributes.get(AttributeKey.stringKey("sdh.thrown_from")))
                .isEqualTo("com.sdh.healthcare.connector.stage.StageRegistry"
                        + ".store:1012");
        assertThat(attributes.get(AttributeKey.stringKey("sdh.sql_state")))
                .isEqualTo("40P01");
        assertThat(attributes.toString()).doesNotContain("Smith");

        Throwable ours;
        try
        {
            check();
            throw new AssertionError("check() returned");
        }
        catch (IllegalStateException thrown)
        {
            ours = thrown;
        }
        Attributes own = Fields.of().failure(ours).toAttributes();
        assertThat(own.get(AttributeKey.stringKey("sdh.thrown_from")))
                .isNull();
        assertThat(own.get(AttributeKey.stringKey("sdh.sql_state"))).isNull();
        assertThat(Fields.of().failure(new SQLException(VALUE, VALUE))
                .toAttributes().get(AttributeKey.stringKey("sdh.sql_state")))
                .as("a state that isn't one is left out").isNull();
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
