/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.util.Set;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

/**
 * Tells an SQS consumer's health of every poll (design §5.3.16): a poll that
 * completes, even with no messages, means SQS answered; one that fails
 * says why. Camel's own strategy would log the failure instead, at length.
 * A poll that hangs reports nothing, so a stall shows as a growing
 * time since the broker last answered (the landing node's
 * {@code sdh.intake.last_poll_age}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SqsPollStrategy implements PollingConsumerPollStrategy
{

    /** Error codes SQS gives for credentials it won't accept. */
    private static final Set<String> AUTH_CODES = Set.of(
            "InvalidClientTokenId", "SignatureDoesNotMatch",
            "UnrecognizedClientException", "ExpiredToken",
            "ExpiredTokenException", "InvalidSignatureException",
            "IncompleteSignature", "MissingAuthenticationToken");

    /** Error codes SQS gives for an account that may not read the queue. */
    private static final Set<String> PERMISSION_CODES = Set.of(
            "AccessDenied", "AccessDeniedException", "AuthorizationError",
            "KMS.AccessDeniedException");

    /** Told whether SQS answers. */
    private final BrokerHealth health;

    /**
     * Creates the strategy.
     *
     * @param health told whether SQS answers
     */
    SqsPollStrategy(BrokerHealth health)
    {
        this.health = health;
    }

    @Override
    public boolean begin(Consumer consumer, Endpoint endpoint)
    {
        return true;
    }

    @Override
    public void commit(Consumer consumer, Endpoint endpoint,
            int polledMessages)
    {
        health.answered();
    }

    @Override
    public boolean rollback(Consumer consumer, Endpoint endpoint,
            int retryCounter, Exception cause)
    {
        health.unreachable(classify(cause));
        return false;
    }

    /**
     * Why a poll failed.
     *
     * @param failure the failure
     * @return the reason
     */
    static BrokerProblem classify(Throwable failure)
    {
        for (Throwable cause = failure; cause != null;
                cause = cause.getCause())
        {
            if (cause instanceof QueueDoesNotExistException)
            {
                return BrokerProblem.NOT_FOUND;
            }
            if (cause instanceof AwsServiceException service
                    && service.awsErrorDetails() != null)
            {
                String code = service.awsErrorDetails().errorCode();
                if (AUTH_CODES.contains(code))
                {
                    return BrokerProblem.AUTH;
                }
                if (PERMISSION_CODES.contains(code))
                {
                    return BrokerProblem.PERMISSION;
                }
            }
        }
        return BrokerProblem.NETWORK;
    }
}
