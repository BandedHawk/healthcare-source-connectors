/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.pipeline.PublishFailedException;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Sends the sealed {@code item.landed} message for a landed item and waits
 * for the broker to acknowledge it (design §5.3.6). The message ID is fixed
 * by the item, {@code il-<item id>}, so sending it again after a restart is
 * harmless: the splitter drops repeats.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Announcer
{

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
        this.sealer = sealer;
        this.publisher = publisher;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.retry = retry;
        this.timeout = timeout;
    }

    /**
     * Sends an item's {@code item.landed} message and waits for its
     * acknowledgement.
     *
     * @param item the landed item
     * @throws PublishFailedException if the broker didn't acknowledge it
     *                                after every retry
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
        Telemetry.get().itemLanded(item.intake());
        Telemetry.get().audit("item.announced",
                Fields.of(Attr.ITEM_ID, item.itemId())
                        .with(Attr.INTAKE, item.intake()));
    }
}
