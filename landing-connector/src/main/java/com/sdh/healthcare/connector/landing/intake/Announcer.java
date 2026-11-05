/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.pipeline.PublishFailedException;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Sends the sealed {@code item.landed} message for a landed item and waits
 * for the broker to acknowledge it (design §5.3.6). The message ID is fixed
 * by the item, {@code il-<item id>}, so sending it again after a restart is
 * harmless: the splitter drops repeats.
 * <p>
 * With results on (design §5.3.8), the item's receipt,
 * {@code item.received}, then goes to the producer, built from the item's
 * summary row so it is the same after a restart. Both are sent before the
 * landing is finished, so a crash means both are sent again, never that
 * the receipt is lost.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Announcer
{

    /**
     * Where receipts go, and what they are read from.
     *
     * @param channel     the producer's results queue
     * @param landedItems the {@code landed_items} table
     * @param keys        unwraps the summary rows' data keys
     */
    public record Receipts(ResultsChannel channel, Table landedItems,
            KeyProvider keys)
    {
    }

    /** Where receipts go; null when results are off. */
    private final Receipts receipts;

    /** Seals messages for the recipient. */
    private final MessageSealer sealer;

    /** Sends sealed messages to the queue. */
    private final MessagePublisher publisher;

    /** The landing catalog's name, put in messages. */
    private final String catalogName;

    /** The landing namespace, put in messages. */
    private final String namespace;

    /** How sends are retried. */
    private final RetryPolicy retry;

    /** How long to wait for the broker's acknowledgement. */
    private final Duration timeout;

    /**
     * Creates an announcer.
     *
     * @param sealer      seals messages for the recipient
     * @param publisher   sends them to the queue
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param retry       how sends are retried
     * @param timeout     how long to wait for each acknowledgement
     */
    public Announcer(MessageSealer sealer, MessagePublisher publisher,
            String catalogName, String namespace, RetryPolicy retry,
            Duration timeout)
    {
        this(sealer, publisher, catalogName, namespace, retry, timeout,
                null);
    }

    /**
     * Creates an announcer that also sends receipts.
     *
     * @param sealer      seals messages for the recipient
     * @param publisher   sends them to the queue
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param retry       how sends are retried
     * @param timeout     how long to wait for each acknowledgement
     * @param receipts    where receipts go; null for none
     */
    public Announcer(MessageSealer sealer, MessagePublisher publisher,
            String catalogName, String namespace, RetryPolicy retry,
            Duration timeout, Receipts receipts)
    {
        this.receipts = receipts;
        this.sealer = sealer;
        this.publisher = publisher;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.retry = retry;
        this.timeout = timeout;
    }

    /**
     * Sends an item's {@code item.landed} message and waits for its
     * acknowledgement; then, with results on, its receipt.
     *
     * @param item the landed item
     * @throws PublishFailedException if the broker didn't acknowledge one
     *                                after every retry, or the summary
     *                                row couldn't be read
     * @throws InterruptedException   if the wait is interrupted
     */
    public void announce(LandedItem item)
            throws PublishFailedException, InterruptedException
    {
        ConnectorMessage.ItemLanded message =
                item.message(catalogName, namespace);
        OutboundMessage outbound = new OutboundMessage(message.messageId(),
                MessageType.ITEM_LANDED, item.itemId(), sealer.seal(message));
        try
        {
            retry.call("landing.announce", failure -> true, attempt ->
                    publisher.publish(outbound).get(timeout.toMillis(),
                            TimeUnit.MILLISECONDS));
        }
        catch (InterruptedException interrupted)
        {
            throw interrupted;
        }
        catch (Exception failure)
        {
            throw new PublishFailedException(failure);
        }
        if (receipts != null)
        {
            receipts.channel().send(receipt(item));
        }
        Telemetry.get().itemLanded(item.intake(), item.kind().wireName(),
                item.size());
        Telemetry.get().audit("item.announced",
                Fields.of(Attr.ITEM_ID, item.itemId())
                        .with(Attr.INTAKE, item.intake()));
    }

    /**
     * The receipt for an item, from its summary row: the producer's
     * reference and, for a batch, the messages' broker IDs and the
     * earlier messages they replace.
     *
     * @param item the landed item
     * @return the receipt
     * @throws PublishFailedException if the summary row can't be read
     */
    private ConnectorMessage.ItemReceived receipt(LandedItem item)
            throws PublishFailedException
    {
        try
        {
            Record summary = LandedItems.summary(
                    receipts.landedItems(), item.itemId())
                    .orElseThrow(() -> new IOException("No summary row"));
            return new ConnectorMessage.ItemReceived(item.itemId(),
                    item.intake(), item.kind().wireName(), item.size(),
                    item.sha256(), item.landedAt(),
                    LandedItems.originalName(summary, receipts.keys())
                            .orElse(null),
                    item.replaces(),
                    LandedItems.messageRefs(summary, receipts.keys())
                            .orElse(null),
                    LandedItems.messageReplaces(summary, receipts.keys())
                            .orElse(null));
        }
        catch (IOException | RuntimeException unreadable)
        {
            throw new PublishFailedException(unreadable);
        }
    }
}
