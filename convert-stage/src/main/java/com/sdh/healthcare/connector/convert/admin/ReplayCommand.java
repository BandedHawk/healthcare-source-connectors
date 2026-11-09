/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.convert.ConvertProcessor;
import com.sdh.healthcare.connector.convert.ResultReporter;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.stage.RegistryReport;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.Replayer;
import java.io.PrintStream;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;

/**
 * Sends all the convert stage's output for a completed item again
 * (STAGES.md §13.2): every {@code record.stored} and {@code record.failed},
 * rebuilt from {@code records}, then its {@code item.converted}; and, with
 * {@code --with-result}, the producer's {@code item.result}. For an item
 * lost after the convert stage: the next stage drops what it has, and
 * processes what it lacks.
 * <p>
 * The rules of §13.2: only an item the stage has completed (its
 * {@code stage.item_completed} sent), since a replay of one in progress
 * would announce it early; each message is the one first sent, under its
 * own {@code messageId}, but the broker is given an ID of its own
 * ({@link Replayer}); and the producer's result only on request, since a
 * second result could set off the producer's own handling. The item's
 * {@code item.converted} names the time it was completed, from the
 * registry. Nothing is written to the tables or the lease database.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ReplayCommand implements AdminCommand<ConvertAdminContext>
{

    /** The command's help. */
    static final String USAGE = """
            replay --item-id <id> [--with-result]
                Sends every record.stored and record.failed of an item the
                stage has completed again, rebuilt from records, then its
                item.converted, each under a broker ID of its own, and waits
                for the broker (STAGES.md §13.2). With --with-result, also
                the producer's item.result, for a result that was lost.
                Nothing is written.""";

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
        return Set.of("with-result");
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id");
    }

    @Override
    public void run(AdminArgs arguments, ConvertAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        UUID itemId = parseId(arguments.required("item-id"));
        audit.add(Attr.ITEM_ID, itemId);
        audit.add(Attr.STAGE, ConvertProcessor.STAGE);
        RegistryReport registry = context.registry();
        RegistryReport.Item item = registry.item(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The convert stage has no row for this item"));
        if (item.completedAt() == null)
        {
            throw new IllegalStateException("The item isn't complete: only"
                    + " an item the stage has completed can be replayed");
        }
        KeyMaterial keys = context.keys();
        ItemSplit total = null;
        byte[] sealed = registry.totalMessage(itemId).orElse(null);
        if (sealed != null
                && keys.inbox().open(sealed) instanceof ItemSplit split)
        {
            total = split;
        }
        boolean withResult = arguments.flag("with-result");
        if (withResult && total == null)
        {
            throw new IllegalStateException("The item's item.split wasn't"
                    + " kept, so its result can't be rebuilt");
        }
        // Opened before anything is sent, so a replay that can't send the
        // result sends nothing.
        ResultsChannel results = withResult ? context.results() : null;

        Table records = context.records();
        records.refresh();
        audit.add(Attr.TABLE, RecordsTable.DEFAULT_NAME);
        List<ConnectorMessage> messages = ConvertProcessor.replayMessages(
                itemId, records, context.properties().storage().catalogName(),
                context.properties().storage().namespace(),
                total == null ? ConvertProcessor.UNKNOWN_INTAKE
                        : total.intake(),
                item.done(), item.failed(), item.completedAt(),
                item.completion());
        int recordCount = messages.size() - 1;
        audit.add(Attr.COUNT, recordCount);

        String suffix = new Replayer(new MessageSealer(keys.signer(),
                keys.signerKid(), keys.recipient(), keys.recipientKid()),
                context.publisher(), context.retry(), clock).send(messages);
        audit.add(Attr.BROKER_MESSAGE_ID,
                messages.get(messages.size() - 1).messageId() + suffix);
        out.printf("sent %d record messages and the item.converted for %s"
                + " again, with broker IDs ending %s; the broker"
                + " acknowledged them%n", recordCount, itemId, suffix);

        if (withResult)
        {
            List<ItemResult> parts = ResultReporter.parts(
                    ResultReporter.Facts.of(total), ItemResult.COMPLETED,
                    total.recordCount(),
                    CommittedRecords.failed(records, itemId, keys.wrap()),
                    item.completedAt(), ResultReporter.PART_SIZE,
                    ResultReporter.PART_BYTES, item.completion());
            for (ItemResult part : parts)
            {
                results.send(part, suffix);
            }
            audit.add(Attr.RESULT, "with_result");
            out.printf("sent the producer's item.result again, in %d"
                    + " part(s)%n", parts.size());
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
