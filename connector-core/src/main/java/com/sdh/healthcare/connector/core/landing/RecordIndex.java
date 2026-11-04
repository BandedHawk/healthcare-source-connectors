/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Where each message of a message batch starts, and the keyed hash of each
 * message's broker ID (design §5.3.16). A batch is landed as its messages'
 * bodies one after another, so a message runs from its offset to the next
 * one's, or to the end of the item.
 * <p>
 * The hashes, made like a {@link SourceRef}'s, recognise a message the
 * broker delivers again. They carry no PHI, so they are stored in plain;
 * the IDs themselves aren't stored.
 *
 * @param offsets where each message starts, from 0, in order
 * @param keys    each message's keyed hash, as {@link SourceRef#hash()};
 *                null for a message the broker gave no ID
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record RecordIndex(List<Long> offsets, List<String> keys)
{

    /**
     * Creates an index, checking that its offsets start at 0 and never go
     * back, and that there is a key, or a null, for each.
     *
     * @param offsets where each message starts
     * @param keys    each message's keyed hash, or null
     * @throws IllegalArgumentException if they don't fit together
     */
    public RecordIndex
    {
        Objects.requireNonNull(offsets, "offsets");
        Objects.requireNonNull(keys, "keys");
        if (offsets.isEmpty() || offsets.size() != keys.size()
                || offsets.get(0) != 0)
        {
            throw new IllegalArgumentException(
                    "A record index needs an offset and key for each message,"
                            + " from offset 0");
        }
        for (int index = 1; index < offsets.size(); index++)
        {
            if (offsets.get(index) < offsets.get(index - 1))
            {
                throw new IllegalArgumentException(
                        "Record offsets must not go back");
            }
        }
        offsets = List.copyOf(offsets);
        keys = Collections.unmodifiableList(new ArrayList<>(keys));
    }

    /**
     * How many messages the batch holds.
     *
     * @return the count
     */
    public int count()
    {
        return offsets.size();
    }

    /**
     * Where a message ends.
     *
     * @param index    the message, from 0
     * @param itemSize the item's size
     * @return the offset just after it
     */
    public long end(int index, long itemSize)
    {
        return index + 1 < offsets.size() ? offsets.get(index + 1) : itemSize;
    }
}
