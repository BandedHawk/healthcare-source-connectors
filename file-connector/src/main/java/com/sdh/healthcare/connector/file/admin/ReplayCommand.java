/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.pipeline.SplitMessages;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.Replayer;
import java.io.PrintStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Sends all the split stage's output for a finished item again
 * (STAGES.md §13.2): every {@code record.split}, rebuilt from
 * {@code split_records} and placed in its landed message as when first
 * sent, then its {@code item.split}, rebuilt from its checkpoint. For an
 * item lost between the split and convert stages: the convert stage drops
 * the records it has, converts those it lacks, and completes the item.
 * <p>
 * The rules of §13.2: only an item whose splitting has finished
 * ({@code COMPLETED} or {@code FAILED}, so its {@code stage.item_completed}
 * was sent), since a replay of one in progress would announce it early;
 * each message is the one first sent, under its own {@code messageId}, but
 * the broker is given an ID of its own ({@link Replayer}). Nothing is
 * written to the tables or the lease database.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ReplayCommand implements AdminCommand<SplitAdminContext>
{

    /** The command's help. */
    static final String USAGE = """
            replay --item-id <id>
                Sends every record.split of an item the stage has finished
                splitting again, rebuilt from split_records, then its
                item.split, each under a broker ID of its own, and waits for
                the broker. The convert stage converts what it lacks and
                completes the item (STAGES.md §13.2). Nothing is written.""";

    /** Gives the time that makes each replay's broker IDs new. */
    private final Clock clock;

    /** Creates the command. */
    ReplayCommand()
    {
        this(Clock.systemUTC());
    }

    /**
     * Creates the command with a clock, for tests.
     *
     * @param clock gives the time
     */
    ReplayCommand(Clock clock)
    {
        this.clock = clock;
    }

    @Override
    public String name()
    {
        return "replay";
    }

    @Override
    public String usage()
    {
        return USAGE;
    }

    @Override
    public Set<String> flags()
    {
        return Set.of();
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id");
    }

    @Override
    public void run(AdminArgs arguments, SplitAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        UUID itemId = parseId(arguments.required("item-id"));
        audit.add(Attr.ITEM_ID, itemId);
        audit.add(Attr.STAGE, "split");
        ConnectorProperties properties = context.properties();

        ItemCheckpoints.Stored stored = context.checkpoints().load(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The split stage has no row for this item"));
        Checkpoint checkpoint = stored.checkpoint();
        audit.add(Attr.STATE, checkpoint.state().name());
        if (checkpoint.state() != Checkpoint.State.COMPLETED
                && checkpoint.state() != Checkpoint.State.FAILED)
        {
            throw new IllegalStateException("The item is "
                    + checkpoint.state() + ": only an item the stage has"
                    + " finished can be replayed");
        }

        Table landed = context.landedItems();
        landed.refresh();
        Record summary = LandedItems.summary(landed, itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The item is no longer in landing"));
        KeyMaterial keys = context.keys();
        LandedRefs refs = LandedRefs.of(summary, keys.wrap());

        Table splitRecords = context.splitRecords();
        splitRecords.refresh();
        audit.add(Attr.TABLE, IngestTables.SPLIT_RECORDS);
        List<CommittedRecords.Row> rows =
                CommittedRecords.all(splitRecords, itemId);
        long snapshotId = splitRecords.currentSnapshot() == null ? -1
                : splitRecords.currentSnapshot().snapshotId();
        String catalog = properties.storage().catalogName();
        String namespace = properties.storage().namespace();
        List<ConnectorMessage> messages = new ArrayList<>();
        for (CommittedRecords.Row row : rows)
        {
            messages.add(SplitMessages.recordSplit(itemId, row,
                    new StorageRef(catalog, namespace,
                            IngestTables.SPLIT_RECORDS, row.ingestedDate(),
                            snapshotId, row.kekRef()), refs));
        }
        messages.add(SplitMessages.itemSplit(checkpoint,
                properties.landing().intake(),
                CommittedRecords.countFailed(splitRecords, itemId),
                (String) summary.getField(LandingTables.ITEM_DIGEST), refs,
                stored.endedAt(), clock.instant()));
        audit.add(Attr.COUNT, rows.size());

        String suffix = new Replayer(new MessageSealer(keys.signer(),
                keys.signerKid(), keys.recipient(), keys.recipientKid()),
                context.publisher(), new RetryPolicy(
                        properties.retry().maxAttempts(),
                        properties.retry().initialBackoff(),
                        properties.retry().maxBackoff()), clock)
                .send(messages);
        audit.add(Attr.BROKER_MESSAGE_ID,
                messages.get(messages.size() - 1).messageId() + suffix);
        out.printf("sent %d record.split and the item.split for %s again,"
                + " with broker IDs ending %s; the broker acknowledged"
                + " them%n", rows.size(), itemId, suffix);
    }

    /**
     * Reads an item ID.
     *
     * @param text the ID as typed
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    private static UUID parseId(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException notAnId)
        {
            throw new AdminArgs.UsageException("--item-id must be a UUID");
        }
    }
}
