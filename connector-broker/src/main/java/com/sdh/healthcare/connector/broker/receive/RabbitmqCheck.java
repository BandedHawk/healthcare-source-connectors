/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import java.util.Arrays;
import java.util.List;
import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

/**
 * Checks that a RabbitMQ consumer's queues are there, by a passive declare on
 * the node's own connection factory (design §5.3.16). The consumer only
 * logs a deleted queue, and takes it up again once it is back. The
 * connection itself is watched by {@link #listener}, which hears of each
 * failed or new connection at once.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RabbitmqCheck implements BrokerCheck
{

    /** Declares passively, on the node's factory. */
    private final RabbitAdmin admin;

    /** The queues consumed. */
    private final List<String> queues;

    /**
     * Creates the check.
     *
     * @param factory the node's connection factory
     * @param queues  the queues, comma-separated, as the URI names them
     */
    RabbitmqCheck(CachingConnectionFactory factory, String queues)
    {
        this.admin = new RabbitAdmin(factory);
        this.queues = Arrays.stream(queues.split(",")).map(String::trim)
                .filter(queue -> !queue.isEmpty()).toList();
    }

    @Override
    public BrokerProblem check()
    {
        try
        {
            for (String queue : queues)
            {
                if (admin.getQueueInfo(queue) == null)
                {
                    return BrokerProblem.NOT_FOUND;
                }
            }
            return null;
        }
        catch (AmqpException failure)
        {
            return classify(failure);
        }
    }

    @Override
    public void close()
    {
    }

    /**
     * Tells the broker's health of each failed or new connection.
     *
     * @param health told whether the broker answers
     * @return the listener
     */
    static ConnectionListener listener(BrokerHealth health)
    {
        return new ConnectionListener()
        {
            @Override
            public void onCreate(Connection connection)
            {
                health.answered();
            }

            @Override
            public void onShutDown(ShutdownSignalException signal)
            {
                if (!signal.isInitiatedByApplication())
                {
                    health.unreachable(BrokerProblem.NETWORK);
                }
            }

            @Override
            public void onFailed(Exception failure)
            {
                health.unreachable(classify(failure));
            }
        };
    }

    /**
     * Why RabbitMQ couldn't be reached.
     *
     * @param failure the failure
     * @return {@link BrokerProblem#AUTH} for refused credentials, else
     *         {@link BrokerProblem#NETWORK}
     */
    static BrokerProblem classify(Throwable failure)
    {
        for (Throwable cause = failure; cause != null;
                cause = cause.getCause())
        {
            if (cause instanceof AuthenticationFailureException
                    || cause instanceof PossibleAuthenticationFailureException
                    || cause instanceof AmqpAuthenticationException)
            {
                return BrokerProblem.AUTH;
            }
        }
        return BrokerProblem.NETWORK;
    }
}
