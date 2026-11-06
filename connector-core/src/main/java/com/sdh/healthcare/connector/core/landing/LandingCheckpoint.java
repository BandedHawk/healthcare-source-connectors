/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * How far landing an item has got (design §5.3.2), in the table property
 * {@code sdh.landing.<intake>} of {@value LandingTables#LANDED_ITEMS},
 * committed with each batch of chunks and MAC'd like every checkpoint. A
 * restart needs only the next block and the digest chain so far.
 * <p>
 * It holds no PHI: the source is identified only by its keyed hash, which
 * an intake can match again by hashing what it lists.
 *
 * @param itemId     the item's ID
 * @param kind       what kind of item it is
 * @param source     where it came from; null if it has no source reference
 * @param startedAt  when landing it started
 * @param nextBlock  the first digest block not yet committed
 * @param chainHash  the digest chain after the last committed block, in
 *                   base64; 32 zero bytes before the first
 * @param done       whether the item is fully landed
 * @param size       the item's size; set once done
 * @param digest     the item's digest; set once done
 * @param landedAt   when the final commit was made; set once done
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LandingCheckpoint(UUID itemId, ItemKind kind, SourceRef source,
        Instant startedAt, long nextBlock, String chainHash, boolean done,
        long size, String digest, Instant landedAt)
{

    /** The start of the table property's name; the intake follows. */
    public static final String PROPERTY_PREFIX = "sdh.landing.";

    /** The label of the checkpoint's MAC. */
    private static final String LABEL = "sdh-landing";

    /**
     * The table property an intake's checkpoint is kept in.
     *
     * @param intake the intake's name
     * @return the property's name
     */
    public static String propertyKey(String intake)
    {
        return PROPERTY_PREFIX + intake;
    }

    /**
     * Writes and reads checkpoints with their MAC.
     *
     * @param ids makes the MACs, with the ID key
     * @return the codec
     */
    public static MacedJson<LandingCheckpoint> codec(RecordIds ids)
    {
        return new MacedJson<>(ids, LABEL, LandingCheckpoint::canonical,
                LandingCheckpoint::parse);
    }

    /**
     * A checkpoint's fields, in a fixed order.
     *
     * @param checkpoint the checkpoint
     * @return its fields; the source and finished values only when set
     */
    private static Map<String, Object> canonical(
            LandingCheckpoint checkpoint)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("itemId", checkpoint.itemId().toString());
        fields.put("kind", checkpoint.kind().wireName());
        if (checkpoint.source() != null)
        {
            fields.put("refHash", checkpoint.source().hash());
            fields.put("sourceSize", checkpoint.source().size());
            fields.put("sourceVersion", checkpoint.source().version());
        }
        fields.put("startedAt", checkpoint.startedAt().toString());
        fields.put("nextBlock", checkpoint.nextBlock());
        fields.put("chainHash", checkpoint.chainHash());
        fields.put("done", checkpoint.done());
        if (checkpoint.done())
        {
            fields.put("size", checkpoint.size());
            fields.put("digest", checkpoint.digest());
            fields.put("landedAt", checkpoint.landedAt().toString());
        }
        return fields;
    }

    /**
     * Rebuilds a checkpoint from its parsed fields.
     *
     * @param fields the fields
     * @return the checkpoint
     * @throws IllegalArgumentException if a field is missing or malformed
     */
    private static LandingCheckpoint parse(Map<String, Object> fields)
    {
        try
        {
            String refHash = JSONObjectUtils.getString(fields, "refHash");
            SourceRef source = refHash == null ? null : new SourceRef(refHash,
                    JSONObjectUtils.getLong(fields, "sourceSize"),
                    JSONObjectUtils.getString(fields, "sourceVersion"));
            boolean done = JSONObjectUtils.getBoolean(fields, "done");
            return new LandingCheckpoint(
                    UUID.fromString(JSONObjectUtils.getString(fields,
                            "itemId")),
                    ItemKind.fromWire(JSONObjectUtils.getString(fields,
                            "kind")),
                    source,
                    Instant.parse(JSONObjectUtils.getString(fields,
                            "startedAt")),
                    JSONObjectUtils.getLong(fields, "nextBlock"),
                    JSONObjectUtils.getString(fields, "chainHash"),
                    done,
                    done ? JSONObjectUtils.getLong(fields, "size") : -1,
                    done ? JSONObjectUtils.getString(fields, "digest")
                            : null,
                    done ? Instant.parse(JSONObjectUtils.getString(fields,
                            "landedAt")) : null);
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException(malformed);
        }
    }
}
