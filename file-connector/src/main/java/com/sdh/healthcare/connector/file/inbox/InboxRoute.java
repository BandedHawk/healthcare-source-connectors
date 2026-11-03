/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.util.Optional;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

/**
 * Consumes {@code item.landed} messages from the landing queue into the
 * inbox (design §5.3.6). The message is acknowledged when the route
 * finishes, after the inbox commit. If the commit fails, the route tries
 * again in place, with a growing pause, and doesn't take the next message
 * until it succeeds; a fatal error stops the process instead.
 * <p>
 * The consumer URI comes from {@code sdh.landing.queue-uri} as it is. It
 * must acknowledge only when the route finishes, not on receipt: for
 * Kafka, for example, with automatic offset commits turned off.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboxRoute extends RouteBuilder
{

    /** The route's ID. */
    public static final String ROUTE_ID = "inbox";

    /** The consumer endpoint. */
    private final String uri;

    /** Opens, checks and records messages. */
    private final InboxReceiver receiver;

    /** The pauses between attempts. */
    private final RetryPolicy retry;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /**
     * Creates the route.
     *
     * @param uri      the consumer endpoint
     * @param receiver opens, checks and records messages
     * @param retry    the pauses between attempts
     * @param fatal    stops the process on a fatal error
     */
    public InboxRoute(String uri, InboxReceiver receiver, RetryPolicy retry,
            FatalExit fatal)
    {
        this.uri = uri;
        this.receiver = receiver;
        this.retry = retry;
        this.fatal = fatal;
    }

    @Override
    public void configure()
    {
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(-1)
                .redeliveryDelay(retry.initialBackoff().toMillis())
                .useExponentialBackOff()
                .backOffMultiplier(2)
                .maximumRedeliveryDelay(retry.maxBackoff().toMillis())
                .allowRedeliveryWhileStopping(false)
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .logExhaustedMessageBody(false)
                .logExhaustedMessageHistory(false)
                .logStackTrace(false)
                .logRetryStackTrace(false));
        from(uri)
                .routeId(ROUTE_ID)
                .autoStartup(false)
                .process(this::receive);
    }

    /**
     * Passes a message to the receiver, stopping the process on a fatal
     * error.
     *
     * @param exchange the message
     * @throws Exception if the inbox can't be written yet
     */
    private void receive(Exchange exchange) throws Exception
    {
        try
        {
            receiver.receive(exchange.getIn().getMandatoryBody(byte[].class));
        }
        catch (Exception | Error failure)
        {
            Optional<String> reason = InboxWorker.fatalReason(failure);
            if (reason.isPresent())
            {
                fatal.exit(reason.get());
            }
            throw failure;
        }
    }
}
