/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.publish;

import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.camel.AsyncProducer;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.service.ServiceHelper;

/**
 * A {@link MessagePublisher} over any Camel producer endpoint.
 * <p>
 * It uses the endpoint's async producer directly from the calling thread,
 * so messages reach the client library in the order they are published (a
 * {@code ProducerTemplate.asyncSend} would hand them to a thread pool and
 * lose the order). The future completes when the producer reports the send
 * done, that is on the broker's acknowledgement, with the acknowledgement
 * settings {@link QueueType} adds. Create it through {@link Publishers},
 * which adds one-at-a-time sending for brokers whose client reorders
 * pipelined sends.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CamelMessagePublisher implements ClosablePublisher
{

    /** Resolves the endpoint. */
    private final CamelContext camel;

    /** Sets the broker's headers on each exchange. */
    private final QueueType type;

    /** The endpoint URI, with the type's options added. */
    private final String uri;

    /** The started producer; null until the first send. */
    private AsyncProducer producer;

    /**
     * Creates a publisher. The producer starts on the first send.
     *
     * @param camel        resolves the endpoint
     * @param type         sets the broker's headers
     * @param effectiveUri the endpoint URI, with the type's options added
     */
    public CamelMessagePublisher(CamelContext camel, QueueType type,
            String effectiveUri)
    {
        this.camel = camel;
        this.type = type;
        this.uri = effectiveUri;
    }

    /**
     * The producer, started on first use.
     *
     * @return the producer
     * @throws Exception if the endpoint can't be resolved or started
     */
    private synchronized AsyncProducer producer() throws Exception
    {
        if (producer == null)
        {
            Endpoint endpoint = camel.getEndpoint(uri);
            AsyncProducer started = endpoint.createAsyncProducer();
            ServiceHelper.startService(started);
            producer = started;
        }
        return producer;
    }

    /**
     * Sends a message, its body as ASCII text.
     *
     * @param message the message
     * @return completes when the producer reports the send done, or fails
     *         with its error
     */
    @Override
    public CompletableFuture<Void> publish(OutboundMessage message)
    {
        CompletableFuture<Void> done = new CompletableFuture<>();
        try
        {
            Exchange exchange = new DefaultExchange(camel);
            exchange.getIn().setBody(
                    new String(message.body(), StandardCharsets.US_ASCII));
            type.apply(exchange, message);
            producer().process(exchange, doneSync ->
            {
                if (exchange.getException() != null)
                {
                    done.completeExceptionally(exchange.getException());
                }
                else
                {
                    done.complete(null);
                }
            });
        }
        catch (Exception failure)
        {
            done.completeExceptionally(failure);
        }
        return done;
    }

    /** Stops the producer, if it was started. */
    @Override
    public synchronized void close()
    {
        if (producer != null)
        {
            ServiceHelper.stopAndShutdownService(producer);
            producer = null;
        }
    }
}
