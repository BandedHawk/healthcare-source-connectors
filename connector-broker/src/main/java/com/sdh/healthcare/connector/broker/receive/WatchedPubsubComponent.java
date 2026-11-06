/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.google.api.core.ApiService;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.component.google.pubsub.GooglePubsubEndpoint;

/**
 * Camel's Pub/Sub component, listening to each subscriber it makes (design
 * §5.3.16). On an error it can't retry, such as a deleted subscription or
 * a revoked permission, Camel's consumer stops for good while its route
 * still says it has started; this hears of it from the subscriber itself.
 * Camel's Spring Boot settings ({@code camel.component.google-pubsub.*})
 * apply to it as to the component it replaces.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class WatchedPubsubComponent extends GooglePubsubComponent
{

    /** Told when a subscriber gives up, with why. */
    private final Consumer<BrokerProblem> gaveUp;

    /**
     * Creates the component.
     *
     * @param gaveUp told when a subscriber gives up, with why
     */
    WatchedPubsubComponent(Consumer<BrokerProblem> gaveUp)
    {
        this.gaveUp = gaveUp;
    }

    @Override
    public Subscriber getSubscriber(String subscription,
            MessageReceiver receiver, GooglePubsubEndpoint endpoint)
            throws IOException
    {
        Subscriber subscriber = super.getSubscriber(subscription, receiver,
                endpoint);
        subscriber.addListener(new ApiService.Listener()
        {
            @Override
            public void failed(ApiService.State from, Throwable failure)
            {
                BrokerProblem reason = gaveUpOn(failure);
                if (reason != null)
                {
                    gaveUp.accept(reason);
                }
            }
        }, Runnable::run);
        return subscriber;
    }

    /**
     * Whether Camel's consumer gives up on a subscriber's failure: on any
     * error but a retryable {@link ApiException}, as Camel decides.
     *
     * @param failure the failure
     * @return why it gave up; null if it will retry
     */
    static BrokerProblem gaveUpOn(Throwable failure)
    {
        ApiException api = failure instanceof ApiException direct ? direct
                : failure != null
                        && failure.getCause() instanceof ApiException cause
                                ? cause : null;
        if (api == null)
        {
            return BrokerProblem.NETWORK;
        }
        return api.isRetryable() ? null
                : PubsubCheck.classify(api.getStatusCode().getCode());
    }
}
