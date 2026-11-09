/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

/**
 * What a stage's processor made of one record: its output, or the record
 * as it was with the error that stopped it, either of which is stored.
 *
 * @param <O>    what the processor stores
 * @param digest the output's digest, the same for the same output each
 *               time; it must cover whatever decides the output beyond
 *               the record itself, such as the converter's version or the
 *               WSTL set's digest, so that a changed output is seen as a
 *               conflict, not a duplicate (STAGES.md §6)
 * @param failed whether the record failed, and is stored as it was
 * @param value  what is stored
 * @param errorCode why it failed, for the fail store (STAGES.md §15.3);
 *               null if it passed
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record StageOutput<O>(String digest, boolean failed, O value,
        String errorCode)
{

    /**
     * An output with no error code noted: a failed one is recorded in the
     * fail store as {@code STAGE_FAILED}.
     *
     * @param digest the output's digest
     * @param failed whether the record failed
     * @param value  what is stored
     */
    public StageOutput(String digest, boolean failed, O value)
    {
        this(digest, failed, value, null);
    }
}
