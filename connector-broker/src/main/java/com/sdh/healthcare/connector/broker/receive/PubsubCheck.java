/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.camel.component.google.pubsub.GooglePubsubEndpoint;

/**
 * Checks a Pub/Sub consumer's subscription, by looking it up with the
 * component's own admin client, so with the consumer's endpoint and
 * credentials (design §5.3.16). The subscriber itself retries a lost
 * service without a word. The account needs
 * {@code pubsub.subscriptions.get}, which {@code roles/pubsub.subscriber}
 * doesn't include; without it every check reports
 * {@link BrokerProblem#PERMISSION}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class PubsubCheck implements BrokerCheck
{

    /** The admin client. */
    private final SubscriptionAdminClient admin;

    /** The subscription's full name. */
    private final String subscription;

    /** The longest to wait for an answer. */
    private final Duration timeout;

    /**
     * Creates the check.
     *
     * @param endpoint the consumer's endpoint
     * @param timeout  the longest to wait for an answer
     * @throws IOException if the admin client can't be made
     */
    PubsubCheck(GooglePubsubEndpoint endpoint, Duration timeout)
            throws IOException
    {
        this.admin = endpoint.getComponent()
                .getSubscriptionAdminClient(endpoint);
        this.subscription = ProjectSubscriptionName.format(
                endpoint.getProjectId(), endpoint.getDestinationName());
        this.timeout = timeout;
    }

    @Override
    public BrokerProblem check() throws InterruptedException
    {
        ApiFuture<Subscription> answer = admin.getSubscriptionCallable()
                .futureCall(GetSubscriptionRequest.newBuilder()
                        .setSubscription(subscription).build());
        try
        {
            answer.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        }
        catch (ExecutionException failure)
        {
            return failure.getCause() instanceof ApiException api
                    ? classify(api.getStatusCode().getCode())
                    : BrokerProblem.NETWORK;
        }
        catch (TimeoutException slow)
        {
            answer.cancel(true);
            return BrokerProblem.NETWORK;
        }
    }

    @Override
    public void close()
    {
        admin.close();
    }

    /**
     * Why Pub/Sub didn't answer.
     *
     * @param code the gRPC status
     * @return the reason
     */
    static BrokerProblem classify(StatusCode.Code code)
    {
        return switch (code)
        {
            case NOT_FOUND -> BrokerProblem.NOT_FOUND;
            case PERMISSION_DENIED -> BrokerProblem.PERMISSION;
            case UNAUTHENTICATED -> BrokerProblem.AUTH;
            default -> BrokerProblem.NETWORK;
        };
    }
}
