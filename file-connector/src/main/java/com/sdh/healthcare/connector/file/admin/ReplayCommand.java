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
import com.sdh.healthcare.connector.core.failure.FailureStore;
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
        send(context, itemId, false, clock, audit, out);
    }

    /**
     * Sends an item's output again: every {@code record.split} and its
     * end, or its end alone, as the fail store counts it now: for
     * {@code decline} and {@code reopen}, which change the counts.
     *
     * @param context  the stage's context
     * @param itemId   the item
     * @param endOnly  whether to send the end alone
     * @param clock    gives the time that makes the broker IDs new
     * @param audit    the command's audit record
     * @param out      where to say what was sent
     * @throws Exception if it can't be sent
     */
    static void send(SplitAdminContext context, UUID itemId, boolean endOnly,
            Clock clock, AuditRecord audit, PrintStream out) throws Exception
    {
        ConnectorProperties properties = context.properties();

        ItemCheckpoints.Stored stored = context.checkpoints().load(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The split stage has no row for this item"));
        Checkpoint checkpoint = stored.checkpoint();
        if (!endOnly)
        {
            // decline and reopen audit the failure's state under this key.
            audit.add(Attr.STATE, checkpoint.state().name());
        }
        if (endOnly && checkpoint.state() != Checkpoint.State.COMPLETED
                && checkpoint.state() != Checkpoint.State.FAILED)
        {
            out.println("The item is " + checkpoint.state() + ": its end"
                    + " will count the change when it is sent");
            return;
        }
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
        List<CommittedRecords.Row> failed = new ArrayList<>();
        for (CommittedRecords.Row row : rows)
        {
            if (!SplitMessages.announced(row))
            {
                // In the fail store, not sent on (STAGES.md §15.1).
                failed.add(row);
                continue;
            }
            if (endOnly)
            {
                continue;
            }
            messages.add(SplitMessages.recordSplit(itemId, row,
                    new StorageRef(catalog, namespace,
                            IngestTables.SPLIT_RECORDS, row.ingestedDate(),
                            snapshotId, row.kekRef()), refs));
        }
        // Operators' corrections that passed (STAGES.md §15.6d): sent with
        // the rest, and counted in the item's end.
        List<CommittedRecords.Row> corrections =
                CommittedRecords.corrections(splitRecords, itemId);
        if (!endOnly)
        {
            for (CommittedRecords.Row row : corrections)
            {
                messages.add(SplitMessages.recordSplit(itemId, row,
                        new StorageRef(catalog, namespace,
                                IngestTables.SPLIT_RECORDS,
                                row.ingestedDate(), snapshotId, row.kekRef()),
                        refs));
            }
        }
        boolean wholeItem = context.failures()
                .failure(itemId, FailureStore.WHOLE_ITEM)
                .map(each -> each.state() == FailureStore.State.CORRECTED)
                .orElse(false);
        // The message that ended it, as the fail store counts it now:
        // item.split, or item.progress while a failure is open (§15.6).
        // Nothing is written: an item's failures not in the store (one
        // split before it) count as open.
        List<FailureStore.Failure> itemFailures =
                SplitMessages.failures(checkpoint, failed);
        FailureStore.Counts counts = itemFailures.isEmpty()
                ? FailureStore.Counts.NONE
                : context.failures().counts(itemId);
        if (counts.total() < itemFailures.size())
        {
            counts = SplitMessages.unstored(checkpoint, failed.size());
        }
        messages.add(SplitMessages.itemEnd(checkpoint,
                properties.landing().intake(), failed.size(), counts,
                (String) summary.getField(LandingTables.ITEM_DIGEST), refs,
                stored.endedAt(), clock.instant(),
                SplitMessages.Corrected.of(checkpoint, corrections,
                        wholeItem)));
        audit.add(Attr.COUNT, messages.size() - 1);
        audit.add(Attr.FAILED_COUNT, failed.size());

        String suffix = new Replayer(new MessageSealer(keys.signer(),
                keys.signerKid(), keys.recipient(), keys.recipientKid()),
                context.publisher(), new RetryPolicy(
                        properties.retry().maxAttempts(),
                        properties.retry().initialBackoff(),
                        properties.retry().maxBackoff()), clock)
                .send(messages);
        audit.add(Attr.BROKER_MESSAGE_ID,
                messages.get(messages.size() - 1).messageId() + suffix);
        ConnectorMessage end = messages.get(messages.size() - 1);
        out.printf("sent %d record.split and the %s for %s again,"
                + " with broker IDs ending %s; the broker acknowledged"
                + " them%n", messages.size() - 1, end.type().wireName(),
                itemId, suffix);
        if (!failed.isEmpty())
        {
            out.printf("%d failed piece(s) are in the fail store, not sent"
                    + " on%n", failed.size());
        }
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
