/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * An item an intake is about to land: what {@link ItemLander} needs to know
 * besides its bytes.
 *
 * @param itemId       the item's ID, a UUIDv7 the intake assigns; the same
 *                     ID again resumes an unfinished landing
 * @param kind         what kind of item it is
 * @param source       where it came from, without PHI; null if it has no
 *                     source reference, such as a push without an
 *                     idempotency key
 * @param originalName its name or path, kept only encrypted; null if it has
 *                     none
 * @param replaces     the item this one replaces, when its producer says
 *                     so; null otherwise
 * @param records      where each message starts, for a message batch; null
 *                     for other items
 * @param messageRefs  each message's broker ID, for a message batch (null
 *                     for a message without one), kept only encrypted;
 *                     null if not recorded, and for other items
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record NewItem(UUID itemId, ItemKind kind, SourceRef source,
        String originalName, UUID replaces, RecordIndex records,
        List<String> messageRefs)
{

    /**
     * Creates an item, checking that it has an ID and a kind.
     *
     * @param itemId       the item's ID
     * @param kind         what kind of item it is
     * @param source       where it came from; null if not known
     * @param originalName its name or path; null if it has none
     * @param replaces     the item it replaces; null if none
     * @param records      where each message starts, for a message batch;
     *                     null for other items
     * @param messageRefs  each message's broker ID; null if not recorded
     * @throws NullPointerException     if the ID or kind is null
     * @throws IllegalArgumentException if a message batch has no record
     *                                  index, or another item has one, or
     *                                  there are broker IDs without an
     *                                  index or of another count
     */
    public NewItem
    {
        Objects.requireNonNull(itemId, "itemId");
        Objects.requireNonNull(kind, "kind");
        if ((records != null) != (kind == ItemKind.MESSAGE_BATCH))
        {
            throw new IllegalArgumentException(
                    "A message batch, and only one, has a record index");
        }
        if (messageRefs != null && (records == null
                || messageRefs.size() != records.count()))
        {
            throw new IllegalArgumentException(
                    "A batch's broker IDs must match its record index");
        }
        messageRefs = messageRefs == null ? null
                : java.util.Collections.unmodifiableList(
                        new java.util.ArrayList<>(messageRefs));
    }

    /**
     * Creates an item without broker IDs.
     *
     * @param itemId       the item's ID
     * @param kind         what kind of item it is
     * @param source       where it came from; null if not known
     * @param originalName its name or path; null if it has none
     * @param replaces     the item it replaces; null if none
     * @param records      where each message starts, for a message batch;
     *                     null for other items
     */
    public NewItem(UUID itemId, ItemKind kind, SourceRef source,
            String originalName, UUID replaces, RecordIndex records)
    {
        this(itemId, kind, source, originalName, replaces, records, null);
    }

    /**
     * Creates an item that isn't a message batch.
     *
     * @param itemId       the item's ID
     * @param kind         what kind of item it is
     * @param source       where it came from; null if not known
     * @param originalName its name or path; null if it has none
     * @param replaces     the item it replaces; null if none
     */
    public NewItem(UUID itemId, ItemKind kind, SourceRef source,
            String originalName, UUID replaces)
    {
        this(itemId, kind, source, originalName, replaces, null, null);
    }
}
