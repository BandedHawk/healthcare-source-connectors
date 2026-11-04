/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exponential backoff with full jitter for transient errors (design §11).
 *
 * @param maxAttempts    how many times an operation is tried
 * @param initialBackoff the longest wait after the first failure
 * @param maxBackoff     the longest wait after any failure
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff,
        Duration maxBackoff)
{

    /** Logs retries, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RetryPolicy.class);

    /** Eight attempts, from 200 ms up to 30 s apart. */
    public static final RetryPolicy DEFAULT = new RetryPolicy(8,
            Duration.ofMillis(200), Duration.ofSeconds(30));

    /**
     * Storage and catalog errors that are safe to retry. An allow-list:
     * anything else, notably {@code CommitStateUnknownException} and lease
     * loss, stops the pipeline.
     */
    public static final Predicate<Throwable> TRANSIENT_STORAGE =
            error -> error instanceof IOException
                    || error instanceof UncheckedIOException
                    || error instanceof CommitFailedException
                    || error instanceof ServiceFailureException
                    || error instanceof ServiceUnavailableException
                    || error instanceof UncheckedSQLException;

    /**
     * Checks the attempt count.
     *
     * @param maxAttempts    how many times an operation is tried
     * @param initialBackoff the longest wait after the first failure
     * @param maxBackoff     the longest wait after any failure
     * @throws IllegalArgumentException if {@code maxAttempts} is below 1
     */
    public RetryPolicy
    {
        if (maxAttempts < 1)
        {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    /**
     * One try of an operation.
     *
     * @param <T> what the operation returns
     */
    @FunctionalInterface
    public interface Attempt<T>
    {

        /**
         * Tries the operation.
         *
         * @param attempt which try this is, from 1
         * @return the result
         * @throws Exception if it fails
         */
        T run(int attempt) throws Exception;
    }

    /**
     * Runs {@code body} until it succeeds, a non-retryable error occurs, or
     * attempts run out.
     *
     * @param <T>       what the operation returns
     * @param operation the operation's name, for the log
     * @param retryable which errors are retried
     * @param body      the operation
     * @return its result
     * @throws Exception the last error, if it never succeeds
     */
    public <T> T call(String operation, Predicate<Throwable> retryable,
            Attempt<T> body) throws Exception
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return body.run(attempt);
            }
            catch (Exception failure)
            {
                if (attempt >= maxAttempts || !retryable.test(failure))
                {
                    throw failure;
                }
                long cap = Math.min(maxBackoff.toMillis(),
                        initialBackoff.toMillis()
                                << Math.min(attempt - 1, 20));
                long sleep = ThreadLocalRandom.current().nextLong(cap + 1);
                LOG.warn("retry op={} attempt={} error={} backoff_ms={}",
                        operation, attempt,
                        failure.getClass().getSimpleName(), sleep);
                Thread.sleep(sleep);
            }
        }
    }
}
