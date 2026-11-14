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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The items an intake's last group commit landed (docs/PERFORMANCE.md
 * §3.1), in the table property {@code sdh.landing-batch.<intake>} of
 * {@value LandingTables#LANDED_ITEMS}, committed with them and MAC'd like
 * every checkpoint. The landing checkpoint names only the last of them;
 * this names them all, so recovery can make sure each has its ledger row
 * and {@code item.landed} message. A commit that lands one item alone
 * removes it.
 * <p>
 * It holds no PHI: only item IDs and where their rows are.
 *
 * @param itemIds  the items, in the order they were landed; the last is
 *                 the one the landing checkpoint names
 * @param file     the data file that holds them, as
 *                 {@code DataFile.location()}
 * @param fileSize the data file's size, in bytes
 * @param rows     how many rows the data file holds
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LandingBatch(List<UUID> itemIds, String file, long fileSize,
        int rows)
{

    /** The start of the table property's name; the intake follows. */
    public static final String PROPERTY_PREFIX = "sdh.landing-batch.";

    /**
     * The start of the name of the table property that says which group
     * commit's messages were all acknowledged; the intake follows.
     */
    public static final String ANNOUNCED_PREFIX = "sdh.landing-announced.";

    /** The label of the property's MAC. */
    private static final String LABEL = "sdh-landing-batch";

    /**
     * Creates a batch, copying its items.
     *
     * @param itemIds  the items, at least one
     * @param file     the data file that holds them
     * @param fileSize its size, in bytes
     * @param rows     how many rows it holds
     * @throws IllegalArgumentException if there are no items, or fewer
     *                                  rows than two for each
     */
    public LandingBatch
    {
        if (itemIds.isEmpty() || rows < 2 * itemIds.size())
        {
            throw new IllegalArgumentException(
                    "A batch has a chunk and a summary row for each item");
        }
        itemIds = List.copyOf(itemIds);
    }

    /**
     * The table property an intake's batch is kept in.
     *
     * @param intake the intake's name
     * @return the property's name
     */
    public static String propertyKey(String intake)
    {
        return PROPERTY_PREFIX + intake;
    }

    /**
     * The table property naming the last item of the last group commit
     * whose messages were all acknowledged, so recovery needn't send them
     * again.
     *
     * @param intake the intake's name
     * @return the property's name
     */
    public static String announcedKey(String intake)
    {
        return ANNOUNCED_PREFIX + intake;
    }

    /**
     * Writes and reads the announced group's last item with its MAC.
     *
     * @param ids makes the MACs, with the ID key
     * @return the codec
     */
    public static MacedJson<UUID> announcedCodec(RecordIds ids)
    {
        return new MacedJson<>(ids, "sdh-landing-announced",
                itemId -> Map.of("itemId", itemId.toString()),
                fields ->
                {
                    try
                    {
                        return UUID.fromString(
                                JSONObjectUtils.getString(fields, "itemId"));
                    }
                    catch (ParseException | NullPointerException malformed)
                    {
                        throw new IllegalArgumentException(malformed);
                    }
                });
    }

    /**
     * Writes and reads batches with their MAC.
     *
     * @param ids makes the MACs, with the ID key
     * @return the codec
     */
    public static MacedJson<LandingBatch> codec(RecordIds ids)
    {
        return new MacedJson<>(ids, LABEL, LandingBatch::canonical,
                LandingBatch::parse);
    }

    /**
     * The last item: the one the landing checkpoint names.
     *
     * @return its ID
     */
    public UUID last()
    {
        return itemIds.get(itemIds.size() - 1);
    }

    /**
     * A batch's fields, in a fixed order.
     *
     * @param batch the batch
     * @return its fields
     */
    private static Map<String, Object> canonical(LandingBatch batch)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("itemIds",
                batch.itemIds().stream().map(UUID::toString).toList());
        fields.put("file", batch.file());
        fields.put("fileSize", batch.fileSize());
        fields.put("rows", batch.rows());
        return fields;
    }

    /**
     * Rebuilds a batch from its parsed fields.
     *
     * @param fields the fields
     * @return the batch
     * @throws IllegalArgumentException if a field is missing or malformed
     */
    private static LandingBatch parse(Map<String, Object> fields)
    {
        try
        {
            List<String> itemIds =
                    JSONObjectUtils.getStringList(fields, "itemIds");
            String file = JSONObjectUtils.getString(fields, "file");
            if (itemIds == null || file == null)
            {
                throw new IllegalArgumentException("A field is missing");
            }
            return new LandingBatch(
                    itemIds.stream().map(UUID::fromString).toList(), file,
                    JSONObjectUtils.getLong(fields, "fileSize"),
                    JSONObjectUtils.getInt(fields, "rows"));
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException(malformed);
        }
    }
}
