/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Sends a landed item's {@code item.landed} again (STAGES.md §13.2), for an
 * item lost between landing and the split stage, or whose
 * {@code item.split} was lost after: the split stage splits an item it
 * hasn't, and sends the {@code item.split} of one it has finished again,
 * unchanged. Nothing else is needed downstream, since each stage drops
 * what it already has.
 * <p>
 * The rules of §13.2: only a fully landed item of this node's intake (with
 * a summary row) is sent; the message is the one first sent, under its own
 * {@code messageId}, but the broker is given an ID of its own,
 * {@code <messageId>~replay-<millis>}, since Artemis, Service Bus and SQS
 * FIFO drop a message whose ID they have seen without saying so; and no
 * receipt goes to the producer. The send waits for the broker's
 * acknowledgement, as the node's own does.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ReplayCommand implements AdminCommand<LandingAdminContext>
{

    /** The command's help. */
    static final String USAGE = """
            replay --item-id <id>
                Sends a fully landed item's item.landed again, under a
                broker ID of its own, and waits for the broker. The split
                stage splits it if it hasn't, or announces it again if it
                has (STAGES.md §13.2). No receipt goes to the producer, and
                nothing is written to the tables.""";

    /** What a replay adds to the message ID, for the broker's ID. */
    static final String REPLAY_SUFFIX = "~replay-";

    /** How long to wait for the broker's acknowledgement of each try. */
    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    /** Gives the time that makes each replay's broker ID new. */
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
    public void run(AdminArgs arguments, LandingAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        UUID itemId = parseId(arguments.required("item-id"));
        audit.add(Attr.ITEM_ID, itemId);
        LandingProperties properties = context.properties();
        String intake = properties.landing().intake();
        audit.add(Attr.INTAKE, intake);

        Table landedItems = context.landedItems();
        landedItems.refresh();
        audit.add(Attr.TABLE, LandingTables.LANDED_ITEMS);
        Record summary = LandedItems.summary(landedItems, itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "No fully landed item with this id: one still"
                                + " being landed can't be replayed"));
        if (!intake.equals(summary.getField(LandingTables.INTAKE)))
        {
            throw new IllegalStateException(
                    "The item was landed by another intake");
        }
        LandedItem item = LandedItems.landedItem(summary,
                landedItems.currentSnapshot().snapshotId());
        ConnectorMessage.ItemLanded message = item.message(
                properties.storage().catalogName(),
                properties.storage().namespace());

        KeyMaterial keys = context.keys();
        OutboundMessage first = OutboundMessage.of(message,
                new MessageSealer(keys.signer(), keys.signerKid(),
                        keys.recipient(), keys.recipientKid())
                        .seal(message));
        String brokerId = first.messageId() + REPLAY_SUFFIX
                + clock.millis();
        OutboundMessage replay = new OutboundMessage(brokerId, first.type(),
                first.itemId(), first.key(), first.body());
        audit.add(Attr.BROKER_MESSAGE_ID, brokerId);

        RetryPolicy retry = new RetryPolicy(properties.retry().maxAttempts(),
                properties.retry().initialBackoff(),
                properties.retry().maxBackoff());
        retry.call("landing.replay", failure -> true, attempt ->
                context.publisher().publish(replay).get(
                        ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        out.printf("sent item.landed for %s again, as %s; the broker"
                + " acknowledged it%n", itemId, brokerId);
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
