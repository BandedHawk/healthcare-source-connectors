/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import java.time.Duration;
import org.apache.camel.component.azure.servicebus.ServiceBusEndpoint;
import org.apache.camel.component.azure.servicebus.client.ServiceBusClientFactory;

/**
 * Checks a Service Bus intake's queue or subscription, by a receiver made
 * as Camel makes the consumer's, from its settings and credentials (design
 * §5.3.16). The processor itself retries a lost namespace without telling
 * Camel. The check peeks from the last possible sequence number, so it
 * needs only the consumer's Listen right, and takes, locks and reads no
 * message.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ServicebusCheck implements BrokerCheck
{

    /** The receiver. */
    private final ServiceBusReceiverAsyncClient receiver;

    /** The longest to wait for an answer. */
    private final Duration timeout;

    /**
     * Creates the check.
     *
     * @param endpoint the consumer's endpoint
     * @param timeout  the longest to wait for an answer
     */
    ServicebusCheck(ServiceBusEndpoint endpoint, Duration timeout)
    {
        this.receiver = new ServiceBusClientFactory()
                .createServiceBusReceiverAsyncClient(
                        endpoint.getConfiguration());
        this.timeout = timeout;
    }

    @Override
    public IntakeProblem check()
    {
        try
        {
            receiver.peekMessage(Long.MAX_VALUE).block(timeout);
            return null;
        }
        catch (RuntimeException failure)
        {
            return classify(failure);
        }
    }

    @Override
    public void close()
    {
        receiver.close();
    }

    /**
     * Why Service Bus didn't answer.
     *
     * @param failure the failure
     * @return the reason
     */
    static IntakeProblem classify(Throwable failure)
    {
        for (Throwable cause = failure; cause != null;
                cause = cause.getCause())
        {
            if (cause instanceof ServiceBusException service)
            {
                ServiceBusFailureReason reason = service.getReason();
                if (ServiceBusFailureReason.UNAUTHORIZED.equals(reason))
                {
                    return IntakeProblem.AUTH;
                }
                if (ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
                        .equals(reason))
                {
                    return IntakeProblem.NOT_FOUND;
                }
            }
            IntakeProblem amqp = EventhubsCheck.classifyAmqp(cause);
            if (amqp != null)
            {
                return amqp;
            }
        }
        return IntakeProblem.NETWORK;
    }
}
