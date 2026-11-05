/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import java.time.Duration;
import org.apache.camel.component.azure.eventhubs.EventHubsEndpoint;
import org.apache.camel.component.azure.eventhubs.client.EventHubsClientFactory;

/**
 * Checks an Event Hubs intake's hub, by asking for its properties with a
 * consumer client made as Camel makes the consumer's, and its checkpoint
 * container (design §5.3.16). The processor itself retries a lost
 * namespace without telling Camel. Asking for the hub's properties reads
 * no event and moves no checkpoint.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class EventhubsCheck implements BrokerCheck
{

    /** The hub's client. */
    private final EventHubConsumerAsyncClient hub;

    /** The checkpoint container's client. */
    private final BlobContainerAsyncClient container;

    /** The longest to wait for an answer. */
    private final Duration timeout;

    /**
     * Creates the check.
     *
     * @param endpoint    the consumer's endpoint
     * @param checkpoints where the checkpoints are kept
     * @param timeout     the longest to wait for an answer
     */
    EventhubsCheck(EventHubsEndpoint endpoint,
            LandingProperties.Eventhubs checkpoints, Duration timeout)
    {
        this.hub = EventHubsClientFactory.createEventHubConsumerAsyncClient(
                endpoint.getConfiguration());
        this.container = EventHubsSupport.container(checkpoints);
        this.timeout = timeout;
    }

    @Override
    public IntakeProblem check()
    {
        try
        {
            hub.getEventHubProperties().block(timeout);
        }
        catch (RuntimeException failure)
        {
            IntakeProblem amqp = null;
            for (Throwable cause = failure; cause != null && amqp == null;
                    cause = cause.getCause())
            {
                amqp = classifyAmqp(cause);
            }
            return amqp == null ? IntakeProblem.NETWORK : amqp;
        }
        try
        {
            return Boolean.TRUE.equals(container.exists().block(timeout))
                    ? null : IntakeProblem.NOT_FOUND;
        }
        catch (RuntimeException failure)
        {
            return classifyStorage(failure);
        }
    }

    @Override
    public void close()
    {
        hub.close();
    }

    /**
     * Why an AMQP call failed, if the broker said.
     *
     * @param failure one exception in the chain
     * @return the reason; null if it isn't an AMQP refusal
     */
    static IntakeProblem classifyAmqp(Throwable failure)
    {
        if (failure instanceof AmqpException amqp)
        {
            AmqpErrorCondition condition = amqp.getErrorCondition();
            if (condition == AmqpErrorCondition.UNAUTHORIZED_ACCESS)
            {
                return IntakeProblem.AUTH;
            }
            if (condition == AmqpErrorCondition.NOT_FOUND)
            {
                return IntakeProblem.NOT_FOUND;
            }
        }
        return null;
    }

    /**
     * Why the checkpoint container couldn't be checked.
     *
     * @param failure the failure
     * @return the reason
     */
    static IntakeProblem classifyStorage(Throwable failure)
    {
        for (Throwable cause = failure; cause != null;
                cause = cause.getCause())
        {
            if (cause instanceof BlobStorageException storage)
            {
                return switch (storage.getStatusCode())
                {
                    case 401 -> IntakeProblem.AUTH;
                    case 403 -> IntakeProblem.PERMISSION;
                    case 404 -> IntakeProblem.NOT_FOUND;
                    default -> IntakeProblem.NETWORK;
                };
            }
        }
        return IntakeProblem.NETWORK;
    }
}
