/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

/**
 * Makes the RabbitMQ connection factory a receiver consumes through, bound
 * in the Camel registry as {@link ReceiveSettings#RABBITMQ_BEAN}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RabbitmqConnections
{

    /** Not instantiated: this class only has static methods. */
    private RabbitmqConnections()
    {
    }

    /**
     * Makes a connection factory.
     *
     * @param settings the connection settings
     * @return the factory; the caller destroys it when done
     * @throws NoSuchAlgorithmException if TLS can't be set up
     * @throws KeyManagementException   if TLS can't be set up
     */
    public static CachingConnectionFactory factory(
            ReceiveProperties.Rabbitmq settings)
            throws NoSuchAlgorithmException, KeyManagementException
    {
        com.rabbitmq.client.ConnectionFactory client =
                new com.rabbitmq.client.ConnectionFactory();
        if (settings.ssl())
        {
            client.useSslProtocol(SSLContext.getDefault());
            client.enableHostnameVerification();
        }
        CachingConnectionFactory factory =
                new CachingConnectionFactory(client);
        factory.setAddresses(settings.addresses());
        factory.setVirtualHost(settings.virtualHost());
        if (settings.username() != null)
        {
            factory.setUsername(settings.username());
        }
        if (settings.password() != null)
        {
            factory.setPassword(settings.password());
        }
        return factory;
    }
}
