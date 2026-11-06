/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.data.Record;

/**
 * What the landing node kept about an item that its producer's result
 * names (design §5.3.8, STAGES.md §9): the producer's own reference, the
 * item it replaces, and for a batch of queue messages where each message
 * starts, its broker ID and the earlier message it replaces. The split
 * stage reads it from {@code landed_items} and passes it on in its
 * messages, so later stages need no landing key. It may hold PHI: never
 * log it.
 *
 * @param producerRef     the producer's own reference; null if none
 * @param replaces        the item it replaces; null if none
 * @param messageIndex    where each message of a batch starts; null if
 *                        the item isn't a batch
 * @param messageRefs     each message's broker ID, or null; null if not
 *                        kept
 * @param messageReplaces the earlier message each message replaces, or
 *                        null; null if none does
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LandedRefs(String producerRef, UUID replaces,
        MessageIndex messageIndex, List<String> messageRefs,
        List<String> messageReplaces)
{

    /** An item with nothing kept: not landed, or its summary is gone. */
    public static final LandedRefs NONE =
            new LandedRefs(null, null, null, null, null);

    /**
     * Reads what an item's summary row keeps, decrypting what is
     * encrypted.
     *
     * @param summary the item's summary row in {@code landed_items}
     * @param keys    unwraps the item's data key
     * @return what it keeps
     * @throws ChunkIntegrityException if an encrypted field doesn't decrypt
     *                                 or parse
     */
    public static LandedRefs of(Record summary, KeyProvider keys)
    {
        return new LandedRefs(
                LandedItems.originalName(summary, keys).orElse(null),
                LandedItems.replaces(summary).orElse(null),
                LandedItems.messageIndex(summary).orElse(null),
                LandedItems.messageRefs(summary, keys).orElse(null),
                LandedItems.messageReplaces(summary, keys).orElse(null));
    }

    /**
     * Finds the message of a batch that holds a byte offset: the last one
     * starting at or before it.
     *
     * @param offset the byte offset in the item
     * @return the message's number, from 0; -1 if the item isn't a batch
     */
    public int message(long offset)
    {
        if (messageIndex == null)
        {
            return -1;
        }
        List<Long> offsets = messageIndex.offsets();
        int found = Collections.binarySearch(offsets, offset);
        if (found >= 0)
        {
            // Empty messages share an offset with the next: take the last.
            while (found + 1 < offsets.size()
                    && offsets.get(found + 1) == offset)
            {
                found++;
            }
            return found;
        }
        return Math.max(0, -found - 2);
    }

    /**
     * A message's broker ID.
     *
     * @param message the message's number; -1 if not a batch
     * @return the ID; null if there is none
     */
    public String messageRef(int message)
    {
        return at(messageRefs, message);
    }

    /**
     * The earlier message a message replaces.
     *
     * @param message the message's number; -1 if not a batch
     * @return its ID; null if there is none
     */
    public String messageReplaced(int message)
    {
        return at(messageReplaces, message);
    }

    /**
     * Places a problem in the message of a batch that holds its offset.
     *
     * @param problem the problem, with no message set
     * @return the problem, with its message, broker ID and replaced
     *         message; the same problem if the item isn't a batch
     */
    public Problem place(Problem problem)
    {
        int message = message(problem.offset());
        if (message < 0)
        {
            return problem;
        }
        return new Problem(problem.code(), problem.stage(), problem.seq(),
                problem.offset(), problem.at(), message, messageRef(message),
                messageReplaced(message));
    }

    /**
     * One message's entry in a per-message list.
     *
     * @param list    the list, one entry per message; null if not kept
     * @param message the message's number; -1 if not a batch
     * @return the entry; null if there is none
     */
    private static String at(List<String> list, int message)
    {
        return message < 0 || list == null || message >= list.size()
                ? null : list.get(message);
    }
}
