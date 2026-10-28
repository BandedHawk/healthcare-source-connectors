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

/** Exponential backoff with full jitter for transient errors (design §11). */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

    public static final RetryPolicy DEFAULT = new RetryPolicy(8, Duration.ofMillis(200), Duration.ofSeconds(30));

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    /**
     * Storage and catalog errors that are safe to retry. An allow-list: anything else, notably
     * {@code CommitStateUnknownException} and lease loss, stops the pipeline.
     */
    public static final Predicate<Throwable> TRANSIENT_STORAGE = e -> e instanceof IOException
            || e instanceof UncheckedIOException
            || e instanceof CommitFailedException
            || e instanceof ServiceFailureException
            || e instanceof ServiceUnavailableException
            || e instanceof UncheckedSQLException;

    @FunctionalInterface
    public interface Attempt<T> {
        T run(int attempt) throws Exception;
    }

    /** Runs {@code body} until it succeeds, a non-retryable error occurs, or attempts run out. */
    public <T> T call(String operation, Predicate<Throwable> retryable, Attempt<T> body) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return body.run(attempt);
            } catch (Exception e) {
                if (attempt >= maxAttempts || !retryable.test(e)) {
                    throw e;
                }
                long cap = Math.min(maxBackoff.toMillis(), initialBackoff.toMillis() << Math.min(attempt - 1, 20));
                long sleep = ThreadLocalRandom.current().nextLong(cap + 1);
                LOG.warn("retry op={} attempt={} error={} backoff_ms={}", operation, attempt,
                        e.getClass().getSimpleName(), sleep);
                Thread.sleep(sleep);
            }
        }
    }
}
