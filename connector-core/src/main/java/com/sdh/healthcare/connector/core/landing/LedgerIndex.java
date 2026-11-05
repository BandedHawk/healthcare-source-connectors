/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;

/**
 * The ledger in memory (design §5.3.2): what has been landed, so an intake
 * can tell at once whether something it has found is new. It is loaded from
 * the ledger table at startup and added to as items land.
 * <p>
 * It holds two compact sets, to stay small with tens of millions of items:
 * the 128-bit key of each sighting ({@link SourceRef#key()}), 16 bytes a
 * slot, and 60 bits of each item's digest, 8 bytes a slot. A set is at most
 * two-thirds full and doubles when it would be fuller, so an item costs 36
 * to 72 bytes. Both are sized for the ledger when it is loaded, so loading
 * doesn't grow them. The digest set only says that some earlier item may
 * have the same content; the ledger table says which.
 * <p>
 * It is given a share of the heap. When its next growth would take it past
 * that share, it reports {@code ledger.memory_low} once, so an operator can
 * raise the memory before a restart finds the ledger too large. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LedgerIndex
{

    /** How many characters of a base32 digest make the digest prefix. */
    private static final int PREFIX_CHARACTERS = 12;

    /** How many bits a base32 character encodes. */
    private static final int BITS_PER_CHARACTER = 5;

    /** The keys of the sightings landed. */
    private final PairSet keys;

    /** The digest prefixes of the items landed. */
    private final LongSet digests;

    /** The bytes it may take, before it warns. */
    private final long budget;

    /** The intake, for the warning. */
    private final String intake;

    /** Whether {@code ledger.memory_low} has been reported. */
    private boolean warned;

    /**
     * Creates an empty index.
     *
     * @param expected how many items it should hold without growing
     * @param budget   the bytes of heap it may take; {@link Long#MAX_VALUE}
     *                 for no limit
     * @param intake   the intake, for the warning
     */
    public LedgerIndex(long expected, long budget, String intake)
    {
        keys = new PairSet(expected);
        digests = new LongSet(expected);
        this.budget = budget;
        this.intake = intake;
    }

    /**
     * How many bytes of heap an index sized for some items takes.
     *
     * @param items how many items
     * @return the bytes
     */
    public static long bytesFor(long items)
    {
        return (long) PairSet.slotsFor(items) * (2 * Long.BYTES + Long.BYTES);
    }

    /**
     * A sighting's 128-bit key.
     *
     * @param high the first 64 bits
     * @param low  the last 64 bits
     */
    public record Key(long high, long low)
    {
    }

    /**
     * Records a landed item.
     *
     * @param key    its sighting's key; null if it had no source reference
     * @param digest its digest, in base32
     */
    public synchronized void add(Key key, String digest)
    {
        long before = bytes();
        if (key != null)
        {
            keys.add(key.high(), key.low());
        }
        digests.add(digestPrefix(digest));
        if (bytes() != before)
        {
            checkBudget();
        }
    }

    /**
     * Tells whether a sighting has already been landed.
     *
     * @param key the sighting's key
     * @return true if it has
     */
    public synchronized boolean contains(Key key)
    {
        return keys.contains(key.high(), key.low());
    }

    /**
     * Tells whether an earlier item may have this content. A true answer
     * can be wrong, about once in 2<sup>60</sup> / (items landed); the
     * ledger table gives the exact answer.
     *
     * @param digest the item's digest, in base32
     * @return true if an item with the same digest prefix has landed
     */
    public synchronized boolean mayHaveContent(String digest)
    {
        return digests.contains(digestPrefix(digest));
    }

    /**
     * How many sightings the index holds.
     *
     * @return the number of keys
     */
    public synchronized long size()
    {
        return keys.size();
    }

    /**
     * How many bytes of heap the index takes.
     *
     * @return the bytes
     */
    public synchronized long bytes()
    {
        return keys.bytes() + digests.bytes();
    }

    /**
     * Reports, once, that the next growth would take the index past its
     * share of the heap. It is checked when the ledger has been loaded and
     * whenever the index grows.
     */
    synchronized void checkBudget()
    {
        long next = 2 * bytes();
        if (!warned && next > budget)
        {
            warned = true;
            Telemetry.get().auditWarn("ledger.memory_low",
                    Fields.of(Attr.INTAKE, intake)
                            .with(Attr.COUNT, size())
                            .with(Attr.BYTES, next));
        }
    }

    /**
     * The first 60 bits of a base32 digest, as a number.
     *
     * @param digest the digest, in lower-case base32 (RFC 4648)
     * @return its first 12 characters' bits
     * @throws IllegalArgumentException if it is too short or isn't base32
     */
    static long digestPrefix(String digest)
    {
        if (digest.length() < PREFIX_CHARACTERS)
        {
            throw new IllegalArgumentException("Digest too short");
        }
        long prefix = 0;
        for (int index = 0; index < PREFIX_CHARACTERS; index++)
        {
            char character = digest.charAt(index);
            int value;
            if (character >= 'a' && character <= 'z')
            {
                value = character - 'a';
            }
            else if (character >= '2' && character <= '7')
            {
                value = 26 + character - '2';
            }
            else
            {
                throw new IllegalArgumentException("Digest isn't base32");
            }
            prefix = (prefix << BITS_PER_CHARACTER) | value;
        }
        return prefix;
    }
}
