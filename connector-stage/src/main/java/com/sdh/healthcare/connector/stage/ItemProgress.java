/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import java.util.List;
import java.util.UUID;

/**
 * An item at a stage: its records all handled once (STAGES.md §5), with
 * how many failed, and of those how many are still open (§15.6).
 *
 * @param itemId       the item's id
 * @param expected     how many records it has, or has so far while the
 *                     stage before still has failures open
 * @param done         how many the stage stored, failed ones included
 * @param failed       how many of those failed
 * @param total        the message that gave its total, opened; null if it
 *                     isn't kept or no longer opens
 * @param versions     the stage's versions its records were made under, in
 *                     the order first used (STAGES.md §14.3); empty if not
 *                     known
 * @param open         its failures at this stage neither passed nor
 *                     declined
 * @param declined     its failures an operator declined to correct
 * @param upstreamOpen whether the total is from the stage before's
 *                     {@code item.progress}, which may grow: that stage
 *                     still has failures open, so the item can't complete
 *                     here
 * @param resolved     its failures at this stage that later passed, by a
 *                     correction (STAGES.md §15.4a): still counted in
 *                     {@code failed}, which counts what failed once
 * @param completion   how many times the stage has completed the item:
 *                     its next item-complete message is the next number
 *                     (STAGES.md §15.6e), counted within its generation
 * @param generation   the item's generation the counts are of (STAGES.md
 *                     §15.8): the stage counts only its records
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ItemProgress(UUID itemId, long expected, long done,
        long failed, ConnectorMessage total, List<String> versions,
        long open, long declined, boolean upstreamOpen, long resolved,
        int completion, int generation)
{

    /**
     * Copies the versions; a generation not given is the first.
     */
    public ItemProgress
    {
        versions = versions == null ? List.of() : List.copyOf(versions);
        generation = Math.max(1, generation);
    }

    /**
     * An item's counts in its first generation.
     *
     * @param itemId       the item's id
     * @param expected     how many records it has
     * @param done         how many the stage stored, failed ones included
     * @param failed       how many of those failed
     * @param total        the message that gave its total, or null
     * @param versions     the versions, in the order first used
     * @param open         its failures neither passed nor declined
     * @param declined     its failures declined
     * @param upstreamOpen whether its total may still grow
     * @param resolved     its failures that later passed
     * @param completion   its completions so far
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed,
            ConnectorMessage total, List<String> versions, long open,
            long declined, boolean upstreamOpen, long resolved,
            int completion)
    {
        this(itemId, expected, done, failed, total, versions, open, declined,
                upstreamOpen, resolved, completion, 1);
    }

    /**
     * An item's counts, never completed.
     *
     * @param itemId       the item's id
     * @param expected     how many records it has
     * @param done         how many the stage stored, failed ones included
     * @param failed       how many of those failed
     * @param total        the message that gave its total, or null
     * @param versions     the versions, in the order first used
     * @param open         its failures neither passed nor declined
     * @param declined     its failures declined
     * @param upstreamOpen whether its total may still grow
     * @param resolved     its failures that later passed
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed,
            ConnectorMessage total, List<String> versions, long open,
            long declined, boolean upstreamOpen, long resolved)
    {
        this(itemId, expected, done, failed, total, versions, open, declined,
                upstreamOpen, resolved, 0);
    }

    /**
     * An item's counts with none of its failures resolved.
     *
     * @param itemId       the item's id
     * @param expected     how many records it has
     * @param done         how many the stage stored, failed ones included
     * @param failed       how many of those failed
     * @param total        the message that gave its total, or null
     * @param versions     the versions, in the order first used
     * @param open         its failures neither passed nor declined
     * @param declined     its failures declined
     * @param upstreamOpen whether its total may still grow
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed,
            ConnectorMessage total, List<String> versions, long open,
            long declined, boolean upstreamOpen)
    {
        this(itemId, expected, done, failed, total, versions, open, declined,
                upstreamOpen, 0);
    }

    /**
     * An item's counts, with the message that gave its total and the
     * versions, each failure open.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @param done     how many the stage stored, failed ones included
     * @param failed   how many of those failed
     * @param total    the message that gave its total, or null
     * @param versions the versions, in the order first used
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed,
            ConnectorMessage total, List<String> versions)
    {
        this(itemId, expected, done, failed, total, versions, failed, 0,
                false);
    }

    /**
     * An item's counts, with the message that gave its total, and no
     * versions.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @param done     how many the stage stored, failed ones included
     * @param failed   how many of those failed
     * @param total    the message that gave its total, or null
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed,
            ConnectorMessage total)
    {
        this(itemId, expected, done, failed, total, List.of());
    }

    /**
     * An item's counts, without the message that gave its total.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @param done     how many the stage stored, failed ones included
     * @param failed   how many of those failed
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed)
    {
        this(itemId, expected, done, failed, null);
    }

    /**
     * The same counts, with the message that gave the total.
     *
     * @param message the message, opened; null if none
     * @return the progress
     */
    public ItemProgress withTotal(ConnectorMessage message)
    {
        return new ItemProgress(itemId, expected, done, failed, message,
                versions, open, declined, upstreamOpen, resolved, completion,
                generation);
    }

    /**
     * The same counts, with the versions its records were made under.
     *
     * @param made the versions, in the order first used
     * @return the progress
     */
    public ItemProgress withVersions(List<String> made)
    {
        return new ItemProgress(itemId, expected, done, failed, total, made,
                open, declined, upstreamOpen, resolved, completion,
                generation);
    }

    /**
     * The same counts, with its failures as the fail store has them, and
     * whether the stage before still has failures open.
     *
     * @param openNow     its failures neither passed nor declined
     * @param declinedNow its failures declined
     * @param upstream    whether its total may still grow
     * @return the progress
     */
    public ItemProgress withFailures(long openNow, long declinedNow,
            boolean upstream)
    {
        return new ItemProgress(itemId, expected, done, failed, total,
                versions, openNow, declinedNow, upstream, resolved,
                completion, generation);
    }

    /**
     * The same counts, with how many of its failures later passed.
     *
     * @param resolvedNow its failures corrected (§15.4a)
     * @return the progress
     */
    public ItemProgress withResolved(long resolvedNow)
    {
        return new ItemProgress(itemId, expected, done, failed, total,
                versions, open, declined, upstreamOpen, resolvedNow,
                completion, generation);
    }

    /**
     * The same counts, with how many times the stage has completed the
     * item.
     *
     * @param completions its completions sent so far
     * @return the progress
     */
    public ItemProgress withCompletion(int completions)
    {
        return new ItemProgress(itemId, expected, done, failed, total,
                versions, open, declined, upstreamOpen, resolved,
                completions, generation);
    }

    /**
     * The same counts, as of a generation of the item.
     *
     * @param of the generation, from 1
     * @return the progress
     */
    public ItemProgress withGeneration(int of)
    {
        return new ItemProgress(itemId, expected, done, failed, total,
                versions, open, declined, upstreamOpen, resolved, completion,
                of);
    }

    /**
     * The number of the item's next completion here, from 1, for its
     * item-complete message.
     *
     * @return the number
     */
    public int nextCompletion()
    {
        return completion + 1;
    }

    /**
     * Whether the item is complete at the stage: its total is final, and
     * every record passed or was declined (STAGES.md §15.6). Otherwise the
     * stage reports its progress instead.
     *
     * @return true if complete
     */
    public boolean complete()
    {
        return !upstreamOpen && open == 0;
    }

    /**
     * The records that passed the stage and were sent on, a failure
     * corrected since included.
     *
     * @return their number
     */
    public long passed()
    {
        return done - failed + resolved;
    }

    /**
     * The records that failed here and haven't passed since: open or
     * declined, or failed before the fail store kept them.
     *
     * @return their number
     */
    public long unresolved()
    {
        return failed - resolved;
    }

    /**
     * What {@code item.progress} is sent again for: a change in any of
     * these counts.
     *
     * @return the counts, as text
     */
    public String progressKey()
    {
        return expected + "/" + done + "/" + failed + "/" + open + "/"
                + declined + "/" + upstreamOpen
                + (resolved == 0 ? "" : "/" + resolved)
                + (completion == 0 ? "" : "/c" + completion)
                + (generation == 1 ? "" : "/g" + generation);
    }
}
