/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.util.SortedSet;

/**
 * A record stage's own step, from one record's bytes to its output
 * (STAGES.md §17.2): the convert stage's converters, the transform stage's
 * mapping. Everything around it (queues, tables, keys, claims, failures,
 * reprocessing, the drain check, results, reconciling) is the library's,
 * so it never sees a table, a key or a queue.
 * <p>
 * For each record the library first asks for a {@link Plan}: either to
 * store the record as it came ({@link Plan.AsIs}), with an error or none,
 * without reading it; or to read it and {@link #apply} the step, with the
 * memory the step needs. A record the step fails is stored as it came,
 * with the error and where in the record it was found, and kept in the
 * fail store (§15.1).
 * <p>
 * Called from several threads at once. Must never log a record's content,
 * names or IDs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface RecordTransform
{

    /**
     * What the library does with a record.
     */
    sealed interface Plan permits Plan.AsIs, Plan.Apply
    {

        /**
         * Store the record as it came, without reading it here: such as a
         * piece the splitter couldn't parse, a record too large, or one of
         * a format with nothing to apply.
         *
         * @param error   why it isn't transformed; null if there is nothing
         *                to apply to it, and it passes as it came
         * @param errorAt where in the item it failed; null if unknown
         */
        record AsIs(ErrorCode error, SourcePosition errorAt) implements Plan
        {
        }

        /**
         * Read the record and apply the step to it.
         *
         * @param memoryBytes the heap the step takes for it, shared with the
         *                    other records being transformed at once
         */
        record Apply(long memoryBytes) implements Plan
        {
        }
    }

    /**
     * What the step made of a record.
     */
    sealed interface Result permits Result.Made, Result.Failed
    {

        /**
         * The record's output.
         *
         * @param bytes          what to store
         * @param representation what the bytes are, and the mapping that
         *                       made them
         */
        record Made(byte[] bytes, Representation representation)
                implements Result
        {
        }

        /**
         * The step failed on the record: it is stored as it came.
         *
         * @param code     why
         * @param position where in the record it failed, in bytes, from 0
         */
        record Failed(ErrorCode code, long position) implements Result
        {
        }
    }

    /**
     * The stage's messages: what it takes from the stage before and what
     * it sends.
     *
     * @return the stage's messages
     */
    StageMessages messages();

    /**
     * The stage's version for a record (STAGES.md §14.2): the build, what
     * makes the output, and a digest of the settings that change it.
     *
     * @param input the record's message
     * @return the version, one of {@link #versions()}
     */
    String version(RecordInStorage input);

    /**
     * Every version this pod stamps, its release (STAGES.md §14.4a).
     *
     * @return the versions, sorted
     */
    SortedSet<String> versions();

    /**
     * The largest record the step reads, and the largest correction an
     * operator may submit.
     *
     * @return the size, in bytes
     */
    long maxBytes();

    /**
     * How a record is stored as it came: its own type, for the convert
     * stage the format's; for the transform stage, the JSON it was given.
     *
     * @param input the record's message
     * @return the representation
     */
    Representation asIs(RecordInStorage input);

    /**
     * What to do with a record.
     *
     * @param input   the record's message
     * @param attempt how many workers have claimed it, from 1: a record on
     *                a later claim may have stopped an earlier worker
     * @return the plan
     */
    Plan plan(RecordInStorage input, int attempt);

    /**
     * Applies the step to a record. Must not throw: a failure is a
     * {@link Result.Failed}. Anything thrown is taken for a bug in the
     * step, and the record failed with {@link #unexpected()}.
     *
     * @param input the record's message
     * @param bytes its bytes, as stored; the library wipes them after
     * @return the output, or why there is none
     */
    Result apply(RecordInStorage input, byte[] bytes);

    /**
     * The error a record is failed with when {@link #apply} throws.
     *
     * @return the code, such as {@code CONVERT_FAILED}
     */
    ErrorCode unexpected();
}
