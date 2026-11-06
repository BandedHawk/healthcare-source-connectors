/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.publish;

import com.nimbusds.jose.JWSSigner;
import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.pipeline.PublishFailedException;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.camel.CamelContext;

/**
 * Sends messages to an intake's producer, on its results queue (design
 * §5.3.8): receipts from the landing node, final results from the
 * splitter. Each is sealed to the producer's own key, since it carries the
 * producer's references, and signed with this node's key.
 * <p>
 * A send waits for the broker's acknowledgement and is retried; the caller
 * sends before it commits the step that follows, so a crash means the
 * message is sent again, never lost. Message IDs are fixed by the item, so
 * the producer can drop repeats.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ResultsChannel implements AutoCloseable
{

    /** The settings' name, for errors. */
    private static final String PREFIX = "sdh.results";

    /** Seals messages for the producer. */
    private final MessageSealer sealer;

    /** Sends sealed messages to the results queue. */
    private final MessagePublisher publisher;

    /** Closes the publisher; null if the caller owns it. */
    private final AutoCloseable closer;

    /** How sends are retried. */
    private final RetryPolicy retry;

    /** How long to wait for the broker's acknowledgement. */
    private final Duration timeout;

    /**
     * A channel on a publisher.
     *
     * @param sealer    seals messages for the producer
     * @param publisher sends them
     * @param closer    closes the publisher; null if the caller does
     * @param retry     how sends are retried
     * @param timeout   how long to wait for each acknowledgement
     */
    public ResultsChannel(MessageSealer sealer, MessagePublisher publisher,
            AutoCloseable closer, RetryPolicy retry, Duration timeout)
    {
        this.sealer = sealer;
        this.publisher = publisher;
        this.closer = closer;
        this.retry = retry;
        this.timeout = timeout;
    }

    /**
     * Opens the channel the settings describe.
     *
     * @param results   the results settings, enabled
     * @param main      the node's own queue, whose broker connection an
     *                  Artemis results queue shares
     * @param camel     the Camel context to publish through
     * @param signer    signs messages
     * @param signerKid the signing key's ID
     * @param retry     how sends are retried
     * @param timeout   how long to wait for each acknowledgement
     * @return the channel
     * @throws IOException              if the producer's key can't be read
     * @throws GeneralSecurityException if it isn't the key pinned
     * @throws IllegalArgumentException if a setting is missing or wrong
     */
    public static ResultsChannel open(CommonSettings.Results results,
            CommonSettings.Queue main, CamelContext camel, JWSSigner signer,
            String signerKid, RetryPolicy retry, Duration timeout)
            throws IOException, GeneralSecurityException
    {
        CommonSettings.Queue queue = results.queue();
        if (queue.type() == null || queue.uri() == null)
        {
            throw new IllegalArgumentException(PREFIX
                    + ".queue.type and uri are required when results are"
                    + " enabled");
        }
        QueueType type = QueueType.of(queue.type());
        if (type == QueueType.ARTEMIS)
        {
            boolean shared = QueueType.of(main.type()) == QueueType.ARTEMIS;
            if (shared && queue.artemis().brokerUrl() != null
                    && !queue.artemis().brokerUrl()
                            .equals(main.artemis().brokerUrl()))
            {
                throw new IllegalArgumentException(PREFIX
                        + ".queue.artemis must be the broker of sdh.queue:"
                        + " both share one JMS connection");
            }
            if (!shared)
            {
                Publishers.configureBroker(camel, type,
                        queue.artemis().brokerUrl(),
                        queue.artemis().username(),
                        queue.artemis().password());
            }
        }
        String recipientKid = results.recipient().kid();
        if (recipientKid == null || recipientKid.isBlank())
        {
            throw new IllegalArgumentException(PREFIX
                    + ".recipient.kid is required when results are enabled");
        }
        MessageSealer sealer = new MessageSealer(signer, signerKid,
                KeyMaterial.pinnedKey(results.recipient(),
                        PREFIX + ".recipient"),
                recipientKid);
        ClosablePublisher publisher = Publishers.create(camel, type,
                type.effectiveUri(queue.uri()));
        return new ResultsChannel(sealer, publisher, publisher, retry,
                timeout);
    }

    /**
     * Sends a message and waits for its acknowledgement.
     *
     * @param message the message, for the producer
     * @throws PublishFailedException if the broker didn't acknowledge it
     *                                after every retry
     * @throws InterruptedException   if the wait is interrupted
     */
    public void send(ConnectorMessage message)
            throws PublishFailedException, InterruptedException
    {
        OutboundMessage outbound = OutboundMessage.of(message,
                sealer.seal(message));
        try
        {
            retry.call("results.send", failure -> true, attempt ->
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
        Telemetry.get().audit("results.sent",
                Fields.of(Attr.ITEM_ID, message.itemId())
                        .with(Attr.MESSAGE_TYPE, message.type().wireName()));
    }

    /**
     * Closes the publisher, if this channel owns it.
     *
     * @throws Exception if it can't be closed
     */
    @Override
    public void close() throws Exception
    {
        if (closer != null)
        {
            closer.close();
        }
    }
}
