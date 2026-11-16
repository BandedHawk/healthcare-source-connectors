/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends again the end of an item that the stage after never received
 * (STAGES.md §13.2, changed 2026-11-16). The stage after's reconciler marks
 * an item whose records are all there but whose end from this stage never
 * came ({@link StageRegistry#wantTotal}); this reads those marks from the
 * stage after's registry, in the same database, and has this stage send
 * each item's end again, under the id it first sent it with, so the stage
 * after takes it once whether or not the first arrives too. An item
 * this stage hasn't ended yet is left: its end is sent when it ends. Runs
 * on the pod that holds the duties lease, after the reconciler.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemEndResender
{

    /** The most items sent again in a run. */
    static final int LIMIT = 1_000;

    /** A plain identifier, for a schema name. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Postgres's SQL state for a table that isn't there. */
    private static final String UNDEFINED_TABLE = "42P01";

    /** Postgres's SQL state for a schema that isn't there. */
    private static final String INVALID_SCHEMA = "3F000";

    /** Logs each run's counts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ItemEndResender.class);

    /** Sends an item's end again. */
    @FunctionalInterface
    public interface Resend
    {

        /**
         * Sends an item's end again, if this stage has ended it.
         *
         * @param itemId the item
         * @return true if it was sent; false if this stage hasn't ended
         *         it, or another pod holds it now
         * @throws InterruptedException if interrupted
         * @throws Exception            if the store or the broker fails
         */
        boolean resend(UUID itemId) throws Exception;
    }

    /** Connections to the database both registries are in. */
    private final DataSource dataSource;

    /** The stage after's item table. */
    private final String items;

    /** The stage after's name, as its rows have it. */
    private final String next;

    /** This stage's name, for telemetry. */
    private final String stage;

    /** Sends an item's end again. */
    private final Resend resend;

    /** How long a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /**
     * Creates a resender.
     *
     * @param dataSource   connections to the database
     * @param nextSchema   the schema of the stage after's registry
     * @param next         the stage after's name, such as {@code convert}
     * @param stage        this stage's name, such as {@code split}
     * @param resend       sends an item's end again
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public ItemEndResender(DataSource dataSource, String nextSchema,
            String next, String stage, Resend resend, Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(nextSchema).matches())
        {
            throw new IllegalArgumentException(
                    "Invalid next stage's registry schema name");
        }
        this.dataSource = dataSource;
        this.items = nextSchema + ".stage_item";
        this.next = next;
        this.stage = stage;
        this.resend = resend;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * A resender for a stage, if its settings name the next stage's
     * registry ({@code sdh.stage.next-registry-schema}).
     *
     * @param dataSource   connections to the database
     * @param nextSchema   the schema of the next stage's registry; null or
     *                     blank for none
     * @param stage        this stage's name, such as {@code split}
     * @param resend       sends an item's end again
     * @param queryTimeout how long a statement may run
     * @return the resender; null if no schema is named
     * @throws IllegalArgumentException if one is named but no record stage
     *                                  follows this one, or it isn't a
     *                                  plain identifier
     */
    public static ItemEndResender of(DataSource dataSource, String nextSchema,
            String stage, Resend resend, Duration queryTimeout)
    {
        if (nextSchema == null || nextSchema.isBlank())
        {
            return null;
        }
        String next = StageMessages.after(stage).orElseThrow(() ->
                new IllegalArgumentException(StageProperties.PREFIX
                        + ".next-registry-schema is set, but no record stage"
                        + " follows " + stage));
        return new ItemEndResender(dataSource, nextSchema, next, stage,
                resend, queryTimeout);
    }

    /**
     * Sends again the ends the stage after is missing, the longest-missing
     * first, up to {@link #LIMIT}. One that fails is logged and left to the
     * next run.
     *
     * @return how many were sent
     * @throws InterruptedException if interrupted
     * @throws IllegalStateException if the stage after's registry can't be
     *                               read
     */
    public int resend() throws InterruptedException
    {
        int sent = 0;
        int failed = 0;
        List<UUID> wanted = wanted();
        for (UUID itemId : wanted)
        {
            try
            {
                if (resend.resend(itemId))
                {
                    sent++;
                    Telemetry.get().auditWarn("stage.item_end_resent",
                            Fields.of(Attr.STAGE, stage)
                                    .with(Attr.ITEM_ID, itemId));
                }
            }
            catch (InterruptedException interrupted)
            {
                throw interrupted;
            }
            catch (Exception failure)
            {
                failed++;
                LOG.warn("stage.item_end_resend_failed stage={} item={}"
                        + " error={}", stage, itemId,
                        failure.getClass().getSimpleName());
            }
        }
        LOG.info("stage.item_ends_resent stage={} wanted={} sent={}"
                + " failed={}", stage, wanted.size(), sent, failed);
        return sent;
    }

    /**
     * The items the stage after has marked as missing their end, whose end
     * still hasn't come, the longest-missing first.
     *
     * @return their ids, up to {@link #LIMIT}; none if the stage after
     *         hasn't made its registry yet
     * @throws IllegalStateException if the registry can't be read
     */
    List<UUID> wanted()
    {
        String sql = """
                SELECT item_id FROM %s
                 WHERE stage = ? AND total_wanted_at IS NOT NULL
                   AND expected IS NULL
                 ORDER BY total_wanted_at
                 LIMIT ?""".formatted(items);
        List<UUID> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, next);
            select.setInt(2, LIMIT);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(rows.getObject(1, UUID.class));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState())
                    || INVALID_SCHEMA.equals(failure.getSQLState()))
            {
                // The stage after hasn't started yet: it wants nothing.
                return found;
            }
            throw new IllegalStateException(
                    "The next stage's registry is unavailable", failure);
        }
    }
}
