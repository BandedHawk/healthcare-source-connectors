/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * An item that is fully landed: what its {@code item.landed} message says.
 *
 * @param itemId     the item's ID
 * @param intake     the intake it came in through
 * @param kind       what kind of item it is
 * @param source     where it came from; null if it had no source reference
 * @param size       its size, in bytes
 * @param digest     its keyed, chained digest, in base32
 * @param landedAt   when its final commit was made
 * @param snapshotId a snapshot of {@value LandingTables#LANDED_ITEMS} that
 *                   holds the whole item
 * @param kekRef     the KEK that wrapped its summary row's data key
 * @param sha256     the plain SHA-256 of its bytes, in lower-case hex; null
 *                   if it was resumed part-way
 * @param replaces   the earlier item its producer said it replaces, a link
 *                   only; null if none
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LandedItem(UUID itemId, String intake, ItemKind kind,
        SourceRef source, long size, String digest, Instant landedAt,
        long snapshotId, String kekRef, String sha256, UUID replaces)
{

    /**
     * The {@code item.landed} message for this item.
     *
     * @param catalog   the name of the landing catalog
     * @param namespace the landing namespace
     * @return the message, ready to seal
     */
    public ConnectorMessage.ItemLanded message(String catalog,
            String namespace)
    {
        return new ConnectorMessage.ItemLanded(itemId, intake,
                kind.wireName(), size, digest, landedAt,
                new ConnectorMessage.StorageRef(catalog, namespace,
                        LandingTables.LANDED_ITEMS,
                        landedAt.atOffset(ZoneOffset.UTC).toLocalDate(),
                        snapshotId, kekRef),
                replaces);
    }
}
