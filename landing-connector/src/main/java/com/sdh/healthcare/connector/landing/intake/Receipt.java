/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * What a push's sender is told once its item is landed (design §5.3.8):
 * enough to find the item later and to check that it arrived intact,
 * without any name or content.
 *
 * @param itemId   the item's ID
 * @param intake   the intake it came in through
 * @param size     its size in bytes
 * @param sha256   the SHA-256 of the bytes as received, in lower-case hex
 * @param landedAt when it was landed, to the microsecond, as stored, so a
 *                 repeated push gets exactly the same receipt
 * @param sameAs   the earlier item of the intake with the same content, the
 *                 earliest still landed (STAGES.md §15.5a); null if none, or
 *                 if landing couldn't yet tell
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Receipt(UUID itemId, String intake, long size, String sha256,
        Instant landedAt, UUID sameAs)
{

    /**
     * The receipt of an item just landed.
     *
     * @param item   the item
     * @param sameAs the earlier item it is an exact copy of; null if none
     * @return its receipt
     */
    public static Receipt of(LandedItem item, UUID sameAs)
    {
        return new Receipt(item.itemId(), item.intake(), item.size(),
                item.sha256(), item.landedAt().truncatedTo(ChronoUnit.MICROS),
                sameAs);
    }

    /**
     * The receipt of an item landed earlier, from its ledger entry.
     *
     * @param entry  the entry
     * @param sameAs the earlier item it is an exact copy of; null if none
     * @return its receipt
     */
    public static Receipt of(LandingLedger.Entry entry, UUID sameAs)
    {
        return new Receipt(entry.itemId(), entry.intake(), entry.size(),
                entry.sha256(),
                entry.landedAt().truncatedTo(ChronoUnit.MICROS), sameAs);
    }

    /**
     * The receipt as JSON. Every field is an ID, a number, hex, a time or
     * the intake's name, which is checked to need no escaping. With no
     * {@code sameAs}, the field is left out.
     *
     * @return the JSON object
     */
    public String toJson()
    {
        return "{\"itemId\":\"" + itemId + "\",\"intake\":\"" + intake
                + "\",\"size\":" + size + ",\"sha256\":"
                + (sha256 == null ? "null" : "\"" + sha256 + "\"")
                + ",\"landedAt\":\"" + landedAt + "\""
                + (sameAs == null ? "" : ",\"sameAs\":\"" + sameAs + "\"")
                + "}";
    }
}
