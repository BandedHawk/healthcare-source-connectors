/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A transform for the library's tests: upper-cases a record's text. A
 * piece the splitter couldn't parse is stored as it came with its error,
 * a record of format {@code raw} as it came with none, and one larger than
 * {@link #MAX_BYTES} as it came, too large. A record that says
 * {@code bad} fails there; one that says {@code check} fails at line 2,
 * column 3 of the record, as a mapping's issue gives it, and one that says
 * {@code nowhere} fails with no place; one that says {@code boom} throws.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class UpperTransform implements RecordTransform
{

    /** The largest record transformed. */
    static final long MAX_BYTES = 1 << 10;

    /** What made the output. */
    static final String MAPPING = "upper/1";

    /** The only version. */
    static final String VERSION = "test-build/" + MAPPING + "/0";

    /** The stage it runs as. */
    private final StageMessages messages;

    /**
     * A transform for a stage.
     *
     * @param messages the stage's messages
     */
    UpperTransform(StageMessages messages)
    {
        this.messages = messages;
    }

    @Override
    public StageMessages messages()
    {
        return messages;
    }

    @Override
    public String version(RecordInStorage input)
    {
        return VERSION;
    }

    @Override
    public SortedSet<String> versions()
    {
        return new TreeSet<>(java.util.List.of(VERSION));
    }

    @Override
    public long maxBytes()
    {
        return MAX_BYTES;
    }

    /** The record's own type, and for a stored record its mapping. */
    @Override
    public Representation asIs(RecordInStorage input)
    {
        return new Representation(input.contentType(),
                input instanceof RecordStored stored ? stored.mapping()
                        : null);
    }

    @Override
    public Plan plan(RecordInStorage input, int attempt)
    {
        if (input instanceof RecordSplit split && split.errorCode() != null)
        {
            return new Plan.AsIs(ErrorCode.valueOf(split.errorCode()),
                    split.errorAt());
        }
        if ("raw".equals(input.format()))
        {
            return new Plan.AsIs(null, null);
        }
        if (input.plaintextLength() > MAX_BYTES)
        {
            return new Plan.AsIs(ErrorCode.CONVERT_TOO_LARGE, null);
        }
        return new Plan.Apply(input.plaintextLength() * 2);
    }

    @Override
    public Result apply(RecordInStorage input, byte[] bytes)
    {
        String text = new String(bytes, UTF_8);
        if (text.contains("boom"))
        {
            throw new IllegalStateException("a bug");
        }
        if (text.contains("check"))
        {
            return Result.Failed.at(ErrorCode.TRANSFORM_CHECK_FAILED,
                    new SourcePosition(2, 3));
        }
        if (text.contains("nowhere"))
        {
            return Result.Failed.at(ErrorCode.TRANSFORM_FAILED, null);
        }
        int bad = text.indexOf("bad");
        if (bad >= 0)
        {
            return new Result.Failed(ErrorCode.CONVERT_CSV_MALFORMED, bad);
        }
        return new Result.Made(text.toUpperCase(Locale.ROOT).getBytes(UTF_8),
                new Representation(messages == StageMessages.TRANSFORM
                        ? "application/fhir+json" : Representation.JSON,
                        MAPPING));
    }

    @Override
    public ErrorCode unexpected()
    {
        return ErrorCode.CONVERT_FAILED;
    }
}
